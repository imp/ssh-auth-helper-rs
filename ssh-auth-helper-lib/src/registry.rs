//! Bounded socket-identity registry. Registration does not probe connectivity.

use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::MetadataExt;

use parking_lot::Mutex;
use tokio::sync::Semaphore;

use super::*;

// Leave space for the RPC envelope and the largest escaped correlation ID.
const STATUS_BYTES: usize = control::MAX_FRAME_BYTES - 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SocketIdentity {
    device: u64,
    inode: u64,
}

impl SocketIdentity {
    fn read(path: &Utf8Path) -> Result<Self, RegistryError> {
        let metadata = fs::metadata(path)?;
        Self::from_metadata(&metadata, rustix::process::getuid().as_raw())
    }

    fn from_metadata(metadata: &fs::Metadata, uid: u32) -> Result<Self, RegistryError> {
        if !metadata.file_type().is_socket() || metadata.uid() != uid {
            return Err(RegistryError::UnsafeSocket);
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[derive(Debug)]
struct ValidatedSocket {
    request: api::RegisterRequest,
    resolved: Utf8PathBuf,
    identity: SocketIdentity,
}

impl ValidatedSocket {
    fn new(
        request: api::RegisterRequest,
        endpoints: &[SocketIdentity; 2],
    ) -> Result<Self, RegistryError> {
        if request
            .label
            .as_ref()
            .is_some_and(|label| label.len() > api::MAX_LABEL_BYTES)
        {
            return Err(RegistryError::LabelTooLong);
        }
        // Reuse the lexical/path-byte policy without preparing or changing a directory.
        runtime::validate_directory_syntax(&request.socket)
            .map_err(|_| RegistryError::UnsafeSocket)?;
        if request.socket.as_str().len() > runtime::MAX_SOCKET_PATH_BYTES {
            return Err(RegistryError::UnsafeSocket);
        }
        let resolved = request.socket.canonicalize_utf8()?;
        let identity = SocketIdentity::read(&resolved)?;
        if endpoints.contains(&identity) {
            return Err(RegistryError::RecursiveSocket);
        }
        Ok(Self {
            request,
            resolved,
            identity,
        })
    }
}

#[derive(Clone, Debug)]
struct Candidate {
    registration: api::Registration,
    resolved: Utf8PathBuf,
    identity: SocketIdentity,
    gate: Arc<Semaphore>,
    retry_at: Option<Instant>,
    failures: u32,
}

impl Candidate {
    fn view(&self, now: Instant) -> api::Registration {
        let mut registration = self.registration.clone();
        registration.retry_after_ms = self
            .retry_at
            .map(|at| forwarding::millis(at.saturating_duration_since(now)));
        registration
    }

    fn retire(self, reason: api::RetirementReason) -> api::Retirement {
        api::Retirement {
            registration: self.registration,
            retired_at: Timestamp::now(),
            reason,
        }
    }
}
#[derive(Clone, Debug, Default)]
struct Registry {
    candidates: Vec<Candidate>,
    retirements: VecDeque<api::Retirement>,
    active: Option<api::RegistrationId>,
    generation: u64,
    active_relays: usize,
    last_setup_failure: Option<api::SetupFailure>,
    last_relay_error: Option<api::RelayFailure>,
}

impl Registry {
    fn retire(&mut self, index: usize, reason: api::RetirementReason) {
        let candidate = self.candidates.remove(index);
        if self.active.as_ref() == Some(&candidate.registration.id) {
            self.active = None;
        }
        self.generation = self.generation.wrapping_add(1);
        let retirement = candidate.retire(reason);
        self.retirements.push_back(retirement);
        while self.retirements.len() > api::MAX_RETIREMENTS {
            self.retirements.pop_front();
        }
    }

    fn register(
        &mut self,
        socket: ValidatedSocket,
        hello: &api::HelloResponse,
        config: &ForwardingConfig,
        deadline: Instant,
    ) -> Result<api::RegisterResponse, RegistryError> {
        if Instant::now() >= deadline {
            return Err(RegistryError::Deadline);
        }
        // Work on bounded candidate state so every rejection leaves the registry unchanged.
        let mut next = self.clone();
        for index in (0..next.candidates.len()).rev() {
            let candidate = &next.candidates[index];
            if candidate.identity != socket.identity
                && (candidate.registration.socket == socket.request.socket
                    || candidate.resolved == socket.resolved)
            {
                next.retire(index, api::RetirementReason::Replaced);
            }
        }
        let registration = if let Some(candidate) = next
            .candidates
            .iter()
            .find(|candidate| candidate.identity == socket.identity)
        {
            candidate.view(Instant::now())
        } else {
            if next.candidates.len() >= api::MAX_REGISTRATIONS {
                return Err(RegistryError::Capacity);
            }
            let registration = api::Registration {
                id: api::RegistrationId::new(hello.instance, Uuid::new_v4())
                    .map_err(|_| RegistryError::InvalidInstance)?,
                socket: socket.request.socket,
                label: socket.request.label,
                registered_at: Timestamp::now(),
                health: api::Health::Unobserved,
                observed_at: None,
                last_failure: None,
                retry_after_ms: None,
                active_relays: 0,
            };
            next.candidates.push(Candidate {
                registration: registration.clone(),
                resolved: socket.resolved,
                identity: socket.identity,
                gate: Arc::new(Semaphore::new(1)),
                retry_at: None,
                failures: 0,
            });
            next.generation = next.generation.wrapping_add(1);
            registration
        };
        next.trim_history(hello, config)?;
        if Instant::now() >= deadline {
            return Err(RegistryError::Deadline);
        }
        *self = next;
        Ok(api::RegisterResponse { registration })
    }

    fn unregister(
        &mut self,
        id: &api::RegistrationId,
        hello: &api::HelloResponse,
        config: &ForwardingConfig,
        deadline: Instant,
    ) -> Result<api::UnregisterResponse, RegistryError> {
        if Instant::now() >= deadline {
            return Err(RegistryError::Deadline);
        }
        if id.instance() == hello.instance
            && let Some(index) = self
                .candidates
                .iter()
                .position(|candidate| candidate.registration.id == *id)
        {
            let mut next = self.clone();
            next.retire(index, api::RetirementReason::Unregistered);
            next.trim_history(hello, config)?;
            if Instant::now() >= deadline {
                return Err(RegistryError::Deadline);
            }
            *self = next;
        }
        Ok(api::UnregisterResponse {})
    }

    fn status(&self, hello: &api::HelloResponse, config: &ForwardingConfig) -> api::StatusResponse {
        api::StatusResponse {
            instance: hello.instance,
            endpoints: hello.endpoints.clone(),
            registrations: self
                .candidates
                .iter()
                .map(|candidate| candidate.view(Instant::now()))
                .collect(),
            recent_retirements: self.retirements.iter().cloned().collect(),
            capacity: api::MAX_REGISTRATIONS,
            retirement_capacity: api::MAX_RETIREMENTS,
            active: self.active.clone(),
            active_relays: self.active_relays,
            bounds: config.bounds(),
            last_setup_failure: self.last_setup_failure,
            last_relay_error: self.last_relay_error.clone(),
        }
    }

    fn fits(&self, hello: &api::HelloResponse, config: &ForwardingConfig) -> bool {
        // Normalize mutable fields, then reserve their worst-case growth. Health/retry
        // fields need less than 256 bytes per registration (fixed enums, timestamps,
        // bounded integer counters). Global active/diagnostic growth needs <512 bytes.
        let mut baseline = self.status(hello, config);
        baseline.active = None;
        baseline.active_relays = 0;
        baseline.last_setup_failure = None;
        baseline.last_relay_error = None;
        for registration in &mut baseline.registrations {
            registration.health = api::Health::Unobserved;
            registration.observed_at = None;
            registration.last_failure = None;
            registration.retry_after_ms = None;
            registration.active_relays = 0;
        }
        let reserve = self.candidates.len() * 256 + 512;
        json::to_vec(&baseline)
            .is_ok_and(|bytes| bytes.len().saturating_add(reserve) <= STATUS_BYTES)
    }

    fn trim_history(
        &mut self,
        hello: &api::HelloResponse,
        config: &ForwardingConfig,
    ) -> Result<(), RegistryError> {
        while !self.fits(hello, config) {
            if self.retirements.pop_front().is_none() {
                return Err(RegistryError::Capacity);
            }
        }
        Ok(())
    }
}

/// Daemon-owned state; locks cover only bounded in-memory work, never socket/filesystem I/O.
#[derive(Debug)]
pub(crate) struct State {
    pub(crate) hello: api::HelloResponse,
    endpoints: [SocketIdentity; 2],
    registry: Mutex<Registry>,
    pub(crate) config: ForwardingConfig,
}

impl State {
    #[cfg(test)]
    pub(crate) fn fixture(hello: api::HelloResponse) -> Self {
        Self {
            hello,
            endpoints: [SocketIdentity {
                device: 0,
                inode: 0,
            }; 2],
            registry: Mutex::new(Registry::default()),
            config: ForwardingConfig::default(),
        }
    }

    pub(crate) fn new(
        hello: api::HelloResponse,
        config: ForwardingConfig,
    ) -> Result<Self, RegistryError> {
        config.validate()?;
        let endpoints = [
            SocketIdentity::read(&hello.endpoints.agent)?,
            SocketIdentity::read(&hello.endpoints.control)?,
        ];
        Ok(Self {
            hello,
            endpoints,
            registry: Mutex::new(Registry::default()),
            config,
        })
    }

    pub(crate) fn register(
        &self,
        request: api::RegisterRequest,
        deadline: Instant,
    ) -> Result<api::RegisterResponse, RegistryError> {
        let socket = ValidatedSocket::new(request, &self.endpoints)?;
        self.registry
            .lock()
            .register(socket, &self.hello, &self.config, deadline)
    }

    pub(crate) fn unregister(
        &self,
        id: &api::RegistrationId,
        deadline: Instant,
    ) -> Result<api::UnregisterResponse, RegistryError> {
        self.registry
            .lock()
            .unregister(id, &self.hello, &self.config, deadline)
    }

    pub(crate) fn status(&self) -> api::StatusResponse {
        self.registry.lock().status(&self.hello, &self.config)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error(transparent)]
    Config(#[from] forwarding::InvalidConfig),
    #[error("registry operation deadline exceeded")]
    Deadline,
    #[error("socket inspection failed: {0}")]
    Io(#[from] io::Error),
    #[error("upstream must be an absolute current-user-owned Unix socket")]
    UnsafeSocket,
    #[error("cannot register a helper endpoint or its alias")]
    RecursiveSocket,
    #[error("registry capacity or status size limit exceeded")]
    Capacity,
    #[error("label exceeds 128 UTF-8 bytes")]
    LabelTooLong,
    #[error("invalid daemon instance")]
    InvalidInstance,
}

impl From<RegistryError> for jsonrpc_uds::RpcError {
    fn from(error: RegistryError) -> Self {
        let (code, message, kind) = match error {
            RegistryError::Config(_) => (-32603, "Internal error", "invalid_config"),
            RegistryError::Deadline => (-32004, "Operation deadline exceeded", "deadline_exceeded"),
            RegistryError::Io(_) | RegistryError::UnsafeSocket => {
                (-32001, "Unsafe upstream socket", "unsafe_path")
            }
            RegistryError::RecursiveSocket => {
                (-32002, "Recursive upstream socket", "recursive_upstream")
            }
            RegistryError::Capacity => (-32003, "Registry capacity exceeded", "capacity_exceeded"),
            RegistryError::LabelTooLong => (-32602, "Invalid params", "label_too_long"),
            RegistryError::InvalidInstance => (-32603, "Internal error", "invalid_instance"),
        };
        Self {
            code,
            message: message.into(),
            data: Some(json::json!({ "kind": kind })),
        }
    }
}

mod selection;

pub(crate) use selection::ProbeTicket;

#[cfg(test)]
mod tests;
