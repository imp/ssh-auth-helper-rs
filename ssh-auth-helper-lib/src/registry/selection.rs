//! Snapshot/commit boundary for setup. No filesystem I/O while the registry is locked.

use super::*;

pub(crate) struct ProbeTicket {
    pub(crate) id: api::RegistrationId,
    pub(crate) socket: Utf8PathBuf,
    pub(crate) gate: Arc<Semaphore>,
    identity: SocketIdentity,
    generation: u64,
}

impl ProbeTicket {
    pub(crate) fn validate_identity(&self) -> Result<(), api::ProbeFailure> {
        let identity = SocketIdentity::read(&self.socket).map_err(|error| match error {
            RegistryError::Io(error) if error.kind() == io::ErrorKind::NotFound => {
                api::ProbeFailure::MissingSocket
            }
            RegistryError::UnsafeSocket => api::ProbeFailure::UnsafeSocket,
            _ => api::ProbeFailure::Transport,
        })?;
        if identity != self.identity {
            return Err(api::ProbeFailure::ReplacedSocket);
        }
        Ok(())
    }

    fn current(&self, registry: &Registry) -> bool {
        self.generation == registry.generation
            && registry.candidates.iter().any(|candidate| {
                candidate.registration.id == self.id
                    && candidate.identity == self.identity
                    && candidate.retry_at.is_none_or(|at| at <= Instant::now())
            })
    }
}

impl State {
    pub(crate) fn next_probe(&self) -> Result<ProbeTicket, api::SetupFailure> {
        let registry = self.registry.lock();
        let ready =
            |candidate: &&Candidate| candidate.retry_at.is_none_or(|at| at <= Instant::now());
        let candidate = registry
            .active
            .as_ref()
            .and_then(|id| {
                registry
                    .candidates
                    .iter()
                    .find(|candidate| candidate.registration.id == *id)
                    .filter(ready)
            })
            .or_else(|| registry.candidates.iter().find(ready))
            .ok_or(if registry.candidates.is_empty() {
                api::SetupFailure::NoCandidates
            } else {
                api::SetupFailure::Backoff
            })?;
        Ok(ProbeTicket {
            id: candidate.registration.id.clone(),
            socket: candidate.registration.socket.clone(),
            identity: candidate.identity,
            gate: Arc::clone(&candidate.gate),
            generation: registry.generation,
        })
    }

    pub(crate) fn ticket_current(&self, ticket: &ProbeTicket) -> bool {
        ticket.current(&self.registry.lock())
    }

    pub(crate) fn setup_failed(&self, reason: api::SetupFailure) {
        self.registry.lock().last_setup_failure = Some(reason);
    }

    pub(crate) fn probe_failed(&self, ticket: &ProbeTicket, failure: api::ProbeFailure) {
        let mut registry = self.registry.lock();
        if !ticket.current(&registry) {
            return;
        }
        let Some(index) = registry
            .candidates
            .iter()
            .position(|candidate| candidate.registration.id == ticket.id)
        else {
            return;
        };
        let candidate = &mut registry.candidates[index];
        candidate.registration.health = api::Health::TemporarilyUnavailable;
        candidate.registration.observed_at = Some(Timestamp::now());
        candidate.registration.last_failure = Some(failure);
        match failure {
            api::ProbeFailure::MissingSocket => {
                registry.retire(index, api::RetirementReason::Missing);
            }
            api::ProbeFailure::ReplacedSocket => {
                registry.retire(index, api::RetirementReason::Replaced);
            }
            api::ProbeFailure::UnsafeSocket => {
                registry.retire(index, api::RetirementReason::Unsafe);
            }
            _ => {
                candidate.failures = candidate.failures.saturating_add(1);
                let mut delay = self.config.retry_initial;
                for _ in 1..candidate.failures.min(64) {
                    delay = delay.saturating_mul(2).min(self.config.retry_max);
                    if delay == self.config.retry_max {
                        break;
                    }
                }
                candidate.retry_at = Some(Instant::now() + delay);
                if registry.active.as_ref() == Some(&ticket.id) {
                    registry.active = None;
                    registry.generation = registry.generation.wrapping_add(1);
                }
            }
        }
        // Retirement fields are bounded; trimming does not remove live registrations.
        let _ = registry.trim_history(&self.hello, &self.config);
    }

    pub(crate) fn pin(self: &Arc<Self>, ticket: &ProbeTicket) -> Option<RelayLease> {
        let mut registry = self.registry.lock();
        if !ticket.current(&registry) {
            return None;
        }
        let candidate = registry
            .candidates
            .iter_mut()
            .find(|candidate| candidate.registration.id == ticket.id)?;
        candidate.registration.health = api::Health::Available;
        candidate.registration.observed_at = Some(Timestamp::now());
        candidate.registration.last_failure = None;
        candidate.retry_at = None;
        candidate.failures = 0;
        candidate.registration.active_relays += 1;
        registry.active_relays += 1;
        registry.last_setup_failure = None;
        if registry.active.as_ref() != Some(&ticket.id) {
            registry.active = Some(ticket.id.clone());
            registry.generation = registry.generation.wrapping_add(1);
        }
        Some(RelayLease {
            state: Arc::clone(self),
            id: ticket.id.clone(),
        })
    }

    pub(crate) fn relay_failed(&self, id: &api::RegistrationId) {
        self.registry.lock().last_relay_error = Some(api::RelayFailure {
            registration: id.clone(),
            observed_at: Timestamp::now(),
        });
    }
}

pub(crate) struct RelayLease {
    state: Arc<State>,
    id: api::RegistrationId,
}

impl Drop for RelayLease {
    fn drop(&mut self) {
        let mut registry = self.state.registry.lock();
        registry.active_relays -= 1;
        if let Some(candidate) = registry
            .candidates
            .iter_mut()
            .find(|candidate| candidate.registration.id == self.id)
        {
            candidate.registration.active_relays -= 1;
        } else if let Some(retirement) = registry
            .retirements
            .iter_mut()
            .find(|retirement| retirement.registration.id == self.id)
        {
            retirement.registration.active_relays -= 1;
        }
    }
}
