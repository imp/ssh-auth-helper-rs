//! Bounded socket-identity registry. Registration does not probe connectivity.

use std::collections::VecDeque;
use std::fs;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::MetadataExt;

use jiff::Timestamp;
use parking_lot::Mutex;
use ssh_auth_helper_api::Health;
use ssh_auth_helper_api::MAX_LABEL_BYTES;
use ssh_auth_helper_api::MAX_REGISTRATIONS;
use ssh_auth_helper_api::MAX_RETIREMENTS;
use ssh_auth_helper_api::RegisterRequest;
use ssh_auth_helper_api::RegisterResponse;
use ssh_auth_helper_api::Registration;
use ssh_auth_helper_api::RegistrationId;
use ssh_auth_helper_api::Retirement;
use ssh_auth_helper_api::RetirementReason;
use ssh_auth_helper_api::StatusResponse;
use ssh_auth_helper_api::UnregisterResponse;
use tokio::time::Instant;
use uuid::Uuid;

use super::*;

// Leave space for the RPC envelope and the largest escaped correlation ID.
const STATUS_BYTES: usize = control::MAX_FRAME_BYTES - 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SocketIdentity {
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
    request: RegisterRequest,
    resolved: Utf8PathBuf,
    identity: SocketIdentity,
}

impl ValidatedSocket {
    fn new(
        request: RegisterRequest,
        endpoints: &[SocketIdentity; 2],
    ) -> Result<Self, RegistryError> {
        if request
            .label
            .as_ref()
            .is_some_and(|label| label.len() > MAX_LABEL_BYTES)
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
    registration: Registration,
    resolved: Utf8PathBuf,
    identity: SocketIdentity,
}

impl Candidate {
    fn retire(self, reason: RetirementReason) -> Retirement {
        Retirement {
            registration: self.registration,
            retired_at: jiff::Timestamp::now(),
            reason,
        }
    }
}
#[derive(Clone, Debug, Default)]
struct Registry {
    candidates: Vec<Candidate>,
    retirements: VecDeque<Retirement>,
}

impl Registry {
    fn retire(&mut self, index: usize, reason: RetirementReason) {
        let candidate = self.candidates.remove(index);
        let retirement = candidate.retire(reason);
        self.retirements.push_back(retirement);
        while self.retirements.len() > MAX_RETIREMENTS {
            self.retirements.pop_front();
        }
    }

    fn register(
        &mut self,
        socket: ValidatedSocket,
        hello: &control::HelloResponse,
        deadline: Instant,
    ) -> Result<RegisterResponse, RegistryError> {
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
                next.retire(index, RetirementReason::Replaced);
            }
        }
        let registration = if let Some(candidate) = next
            .candidates
            .iter()
            .find(|candidate| candidate.identity == socket.identity)
        {
            candidate.registration.clone()
        } else {
            if next.candidates.len() >= MAX_REGISTRATIONS {
                return Err(RegistryError::Capacity);
            }
            let registration = Registration {
                id: RegistrationId::new(hello.instance, Uuid::new_v4())
                    .map_err(|_| RegistryError::InvalidInstance)?,
                socket: socket.request.socket,
                label: socket.request.label,
                registered_at: Timestamp::now(),
                health: Health::Unobserved,
                observed_at: None,
            };
            next.candidates.push(Candidate {
                registration: registration.clone(),
                resolved: socket.resolved,
                identity: socket.identity,
            });
            registration
        };
        next.trim_history(hello)?;
        if Instant::now() >= deadline {
            return Err(RegistryError::Deadline);
        }
        *self = next;
        Ok(RegisterResponse { registration })
    }

    fn unregister(
        &mut self,
        id: &RegistrationId,
        hello: &control::HelloResponse,
        deadline: Instant,
    ) -> Result<UnregisterResponse, RegistryError> {
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
            next.retire(index, RetirementReason::Unregistered);
            next.trim_history(hello)?;
            if Instant::now() >= deadline {
                return Err(RegistryError::Deadline);
            }
            *self = next;
        }
        Ok(UnregisterResponse {})
    }

    fn status(&self, hello: &control::HelloResponse) -> StatusResponse {
        StatusResponse {
            instance: hello.instance,
            endpoints: hello.endpoints.clone(),
            registrations: self
                .candidates
                .iter()
                .map(|candidate| candidate.registration.clone())
                .collect(),
            recent_retirements: self.retirements.iter().cloned().collect(),
            capacity: MAX_REGISTRATIONS,
            retirement_capacity: MAX_RETIREMENTS,
            active: None,
            active_relays: 0,
        }
    }

    fn fits(&self, hello: &control::HelloResponse) -> bool {
        json::to_vec(&self.status(hello)).is_ok_and(|bytes| bytes.len() <= STATUS_BYTES)
    }

    fn trim_history(&mut self, hello: &control::HelloResponse) -> Result<(), RegistryError> {
        while !self.fits(hello) {
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
    pub(crate) hello: control::HelloResponse,
    endpoints: [SocketIdentity; 2],
    registry: Mutex<Registry>,
}

impl State {
    #[cfg(test)]
    pub(crate) fn fixture(hello: control::HelloResponse) -> Self {
        Self {
            hello,
            endpoints: [SocketIdentity {
                device: 0,
                inode: 0,
            }; 2],
            registry: Mutex::new(Registry::default()),
        }
    }

    pub(crate) fn new(hello: control::HelloResponse) -> Result<Self, RegistryError> {
        let endpoints = [
            SocketIdentity::read(&hello.endpoints.agent)?,
            SocketIdentity::read(&hello.endpoints.control)?,
        ];
        Ok(Self {
            hello,
            endpoints,
            registry: Mutex::new(Registry::default()),
        })
    }

    pub(crate) fn register(
        &self,
        request: RegisterRequest,
        deadline: Instant,
    ) -> Result<RegisterResponse, RegistryError> {
        let socket = ValidatedSocket::new(request, &self.endpoints)?;
        self.registry.lock().register(socket, &self.hello, deadline)
    }

    pub(crate) fn unregister(
        &self,
        id: &RegistrationId,
        deadline: Instant,
    ) -> Result<UnregisterResponse, RegistryError> {
        self.registry.lock().unregister(id, &self.hello, deadline)
    }

    pub(crate) fn status(&self) -> StatusResponse {
        self.registry.lock().status(&self.hello)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
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

#[cfg(test)]
mod tests;
