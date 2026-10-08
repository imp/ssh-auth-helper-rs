//! Runtime path discovery. Filesystem ownership and permission validation is
//! deliberately separate; these paths alone do not authorize binding sockets.

use camino::{Utf8Path, Utf8PathBuf};
use std::ffi::OsStr;

#[cfg(target_os = "macos")]
const MAX_SOCKET_PATH_BYTES: usize = 103;
#[cfg(not(target_os = "macos"))]
const MAX_SOCKET_PATH_BYTES: usize = 107;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimePaths {
    directory: Utf8PathBuf,
    agent: Utf8PathBuf,
    control: Utf8PathBuf,
    lock: Utf8PathBuf,
}

impl RuntimePaths {
    /// Discover paths for the current user, without filesystem changes.
    ///
    /// # Errors
    /// Rejects non-UTF-8, relative, malformed, or overly long paths.
    pub fn discover(explicit: Option<Utf8PathBuf>) -> Result<Self, RuntimeError> {
        let xdg = std::env::var_os("XDG_RUNTIME_DIR");
        Self::resolve(explicit, xdg.as_deref(), rustix::process::getuid().as_raw())
    }

    fn resolve(
        explicit: Option<Utf8PathBuf>,
        xdg: Option<&OsStr>,
        uid: u32,
    ) -> Result<Self, RuntimeError> {
        let directory = match explicit {
            Some(path) => path,
            None => match xdg {
                Some(base) => {
                    let base = Utf8Path::from_path(std::path::Path::new(base))
                        .ok_or(RuntimeError::NonUtf8Environment)?;
                    validate_directory_path(base)?;
                    base.join("ssh-auth-helper")
                }
                None => Utf8PathBuf::from(format!("/tmp/ssh-auth-helper-{uid}")),
            },
        };
        validate_directory_path(&directory)?;
        let paths = Self {
            agent: directory.join("agent.sock"),
            control: directory.join("control.sock"),
            lock: directory.join("daemon.lock"),
            directory,
        };
        for path in [&paths.agent, &paths.control] {
            if path.as_str().len() > MAX_SOCKET_PATH_BYTES {
                return Err(RuntimeError::SocketPathTooLong {
                    path: path.clone(),
                    maximum: MAX_SOCKET_PATH_BYTES,
                });
            }
        }
        Ok(paths)
    }

    pub fn directory(&self) -> &Utf8Path {
        &self.directory
    }

    pub fn agent(&self) -> &Utf8Path {
        &self.agent
    }

    pub fn control(&self) -> &Utf8Path {
        &self.control
    }

    pub fn lock(&self) -> &Utf8Path {
        &self.lock
    }
}

fn validate_directory_path(path: &Utf8Path) -> Result<(), RuntimeError> {
    if !path.is_absolute() {
        return Err(RuntimeError::InvalidPath(
            "runtime directory must be absolute",
        ));
    }
    // Reject lexical traversal rather than silently changing the supplied path.
    if path
        .as_str()
        .split('/')
        .any(|part| part == "." || part == "..")
    {
        return Err(RuntimeError::InvalidPath(
            "runtime directory must not contain . or .. components",
        ));
    }
    if path
        .as_str()
        .chars()
        .any(|c| c == '\0' || c == '\n' || c == '\r')
    {
        return Err(RuntimeError::InvalidPath(
            "runtime directory must not contain NUL or line breaks",
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("XDG_RUNTIME_DIR must be a UTF-8 path")]
    NonUtf8Environment,
    #[error("{0}")]
    InvalidPath(&'static str),
    #[error("socket path {path} exceeds the platform limit of {maximum} bytes")]
    SocketPathTooLong { path: Utf8PathBuf, maximum: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::ffi::OsStrExt;

    #[test]
    fn explicit_path_overrides_environment_without_lossy_conversion() {
        let paths = RuntimePaths::resolve(
            Some("/tmp/helper".into()),
            Some(OsStr::from_bytes(b"\xff")),
            42,
        )
        .unwrap();
        assert_eq!(paths.agent(), "/tmp/helper/agent.sock");
        assert_eq!(paths.control(), "/tmp/helper/control.sock");
        assert_eq!(paths.lock(), "/tmp/helper/daemon.lock");
    }

    #[test]
    fn xdg_and_uid_fallback_have_stable_endpoint_names() {
        let xdg = RuntimePaths::resolve(None, Some(OsStr::new("/run/user/42")), 42).unwrap();
        assert_eq!(xdg.agent(), "/run/user/42/ssh-auth-helper/agent.sock");
        let fallback = RuntimePaths::resolve(None, None, 42).unwrap();
        assert_eq!(fallback.agent(), "/tmp/ssh-auth-helper-42/agent.sock");
    }

    #[test]
    fn invalid_environment_and_runtime_paths_are_rejected() {
        assert!(matches!(
            RuntimePaths::resolve(None, Some(OsStr::from_bytes(b"/tmp/\xff")), 42),
            Err(RuntimeError::NonUtf8Environment)
        ));
        for path in [
            "relative",
            "/tmp/../helper",
            "/tmp/./helper",
            "/tmp/line\nbreak",
            "/tmp/nul\0byte",
        ] {
            assert!(matches!(
                RuntimePaths::resolve(Some(path.into()), None, 42),
                Err(RuntimeError::InvalidPath(_))
            ));
        }
    }

    #[test]
    fn socket_path_limits_count_utf8_bytes() {
        let suffix_bytes = "/control.sock".len();
        let directory = format!("/{}", "a".repeat(MAX_SOCKET_PATH_BYTES - suffix_bytes - 1));
        assert!(RuntimePaths::resolve(Some(directory.clone().into()), None, 42).is_ok());
        assert!(matches!(
            RuntimePaths::resolve(Some(format!("{directory}a").into()), None, 42),
            Err(RuntimeError::SocketPathTooLong { .. })
        ));
        assert!(matches!(
            RuntimePaths::resolve(Some(format!("/{}", "界".repeat(40)).into()), None, 42),
            Err(RuntimeError::SocketPathTooLong { .. })
        ));
    }
}
