//! Runtime path discovery. Filesystem ownership and permission validation is
//! deliberately separate; these paths alone do not authorize binding sockets.

use std::ffi::OsStr;

use super::*;

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
    /// Discover paths for the current user from supplied configuration.
    /// Does not read environment variables or change the filesystem.
    ///
    /// # Errors
    /// Rejects non-UTF-8, relative, malformed, or overly long paths.
    pub fn discover(
        explicit: Option<&Utf8Path>,
        xdg: Option<&OsStr>,
    ) -> Result<Self, RuntimeError> {
        let directory = if let Some(directory) = explicit {
            directory.to_owned()
        } else {
            xdg.map(|base| Utf8Path::from_os_str(base).ok_or(RuntimeError::NonUtf8Environment))
                .transpose()?
                .map_or_else(default_directory, |base| base.join("ssh-auth-helper"))
        };
        Self::with_dir(directory)
    }

    fn with_dir(directory: Utf8PathBuf) -> Result<Self, RuntimeError> {
        validate_directory_syntax(&directory)?;
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

fn default_directory() -> Utf8PathBuf {
    let uid = rustix::process::getuid();
    Utf8PathBuf::from(format!("/tmp/ssh-auth-helper-{uid}"))
}

/// Check path syntax without accessing the filesystem. Directory creation,
/// ownership, permissions, and safe symlink handling belong to startup.
fn validate_directory_syntax(path: &Utf8Path) -> Result<(), RuntimeError> {
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

    #[test]
    fn runtime_directory_has_stable_endpoint_names() {
        let paths = RuntimePaths::with_dir("/tmp/helper".into()).unwrap();
        assert_eq!(paths.agent(), "/tmp/helper/agent.sock");
        assert_eq!(paths.control(), "/tmp/helper/control.sock");
        assert_eq!(paths.lock(), "/tmp/helper/daemon.lock");
    }

    #[test]
    fn invalid_runtime_paths_are_rejected() {
        for path in [
            "",
            "relative",
            "/tmp/../helper",
            "/tmp/./helper",
            "/tmp/line\nbreak",
            "/tmp/line\rbreak",
            "/tmp/nul\0byte",
        ] {
            assert!(matches!(
                RuntimePaths::with_dir(path.into()),
                Err(RuntimeError::InvalidPath(_))
            ));
        }
    }

    #[test]
    fn socket_path_limits_count_utf8_bytes() {
        let suffix_bytes = "/control.sock".len();
        let directory = format!("/{}", "a".repeat(MAX_SOCKET_PATH_BYTES - suffix_bytes - 1));
        assert!(RuntimePaths::with_dir(directory.clone().into()).is_ok());
        assert!(matches!(
            RuntimePaths::with_dir(format!("{directory}a").into()),
            Err(RuntimeError::SocketPathTooLong { .. })
        ));
        assert!(matches!(
            RuntimePaths::with_dir(format!("/{}", "界".repeat(40)).into()),
            Err(RuntimeError::SocketPathTooLong { .. })
        ));
    }
}
