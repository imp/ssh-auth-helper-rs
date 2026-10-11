//! Runtime paths, private directory preparation and daemon lifetime ownership.

use std::collections::VecDeque;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::OwnedFd;

use camino::Utf8Component;
use rustix::fs;
use rustix::io;
use tokio::net;

use super::*;

pub use startup::RuntimeLock;
pub use startup::RuntimeSockets;

mod startup;

#[cfg(target_os = "macos")]
const MAX_SOCKET_PATH_BYTES: usize = 103;
#[cfg(not(target_os = "macos"))]
const MAX_SOCKET_PATH_BYTES: usize = 107;
const MAX_ANCESTOR_SYMLINKS: usize = 40;

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
        xdg: Option<&ffi::OsStr>,
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

/// An opened, validated runtime directory. Preparing it neither binds sockets
/// nor proves daemon readiness. The retained descriptor anchors later startup work.
#[derive(Debug)]
pub struct RuntimeDirectory {
    paths: RuntimePaths,
    descriptor: OwnedFd,
    uid: u32,
}

impl RuntimeDirectory {
    /// Open a safe directory, creating only its final component if missing.
    /// Existing objects are checked without changing their modes or contents.
    ///
    /// # Errors
    /// Rejects unsafe ancestors, directory aliases, ownership, modes, and endpoint
    /// types. Propagates filesystem errors, including missing parents.
    pub fn prepare(paths: RuntimePaths) -> Result<Self, RuntimeError> {
        let uid = rustix::process::getuid().as_raw();
        let parent_path = paths.directory.parent().ok_or(RuntimeError::InvalidPath(
            "runtime directory must have a final component",
        ))?;
        let name = paths
            .directory
            .file_name()
            .ok_or(RuntimeError::InvalidPath(
                "runtime directory must have a final component",
            ))?;
        let parent = open_trusted_parent(parent_path, uid)?;
        fs::mkdirat(&parent, name, fs::Mode::RWXU).or_else(|error| match error {
            io::Errno::EXIST => Ok(()),
            source => Err(filesystem_error(
                &paths.directory,
                "create directory",
                source,
            )),
        })?;
        let descriptor = open_directory(&parent, name, &paths.directory)?;
        let metadata = fs::fstat(&descriptor)
            .map_err(|source| filesystem_error(&paths.directory, "inspect directory", source))?;
        validate_owned_object(
            &metadata,
            fs::FileType::Directory,
            fs::Mode::RWXU,
            uid,
            &paths.directory,
        )?;
        let directory = Self {
            paths,
            descriptor,
            uid,
        };
        directory.validate_endpoints()
    }

    pub fn paths(&self) -> &RuntimePaths {
        &self.paths
    }

    /// Check existing endpoint names without following links or deleting objects.
    /// This does not determine whether a socket has a live listener.
    ///
    /// # Errors
    /// Rejects unsafe endpoint ownership, modes, and types, or metadata errors.
    pub fn validate_endpoints(self) -> Result<Self, RuntimeError> {
        let mode = fs::Mode::RUSR | fs::Mode::WUSR;
        for (name, kind) in [
            ("agent.sock", fs::FileType::Socket),
            ("control.sock", fs::FileType::Socket),
            ("daemon.lock", fs::FileType::RegularFile),
        ] {
            let path = self.paths.directory.join(name);
            match fs::statat(&self.descriptor, name, fs::AtFlags::SYMLINK_NOFOLLOW) {
                Ok(metadata) => validate_owned_object(&metadata, kind, mode, self.uid, &path)?,
                Err(io::Errno::NOENT) => {}
                Err(source) => return Err(filesystem_error(&path, "inspect endpoint", source)),
            }
        }
        Ok(self)
    }

    /// Authorize a connected local peer by UID, independently of PID or GID.
    ///
    /// # Errors
    /// Rejects a foreign UID or failure to retrieve kernel peer credentials.
    pub fn authorize_peer(&self, stream: &net::UnixStream) -> Result<(), RuntimeError> {
        authorize_peer_uid(stream, self.uid)
    }
}

impl AsFd for RuntimeDirectory {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.descriptor.as_fd()
    }
}

pub(crate) fn authorize_peer_uid(
    stream: &net::UnixStream,
    expected: u32,
) -> Result<(), RuntimeError> {
    let actual = stream
        .peer_cred()
        .map_err(RuntimeError::PeerCredentials)?
        .uid();
    if actual != expected {
        return Err(RuntimeError::PeerUid { expected, actual });
    }
    Ok(())
}

#[derive(Debug)]
enum AncestorComponent {
    Root,
    Parent,
    Name(String),
}

fn ancestor_components(path: &Utf8Path) -> VecDeque<AncestorComponent> {
    path.components()
        .filter_map(|component| match component {
            Utf8Component::RootDir => Some(AncestorComponent::Root),
            Utf8Component::ParentDir => Some(AncestorComponent::Parent),
            Utf8Component::Normal(name) => Some(AncestorComponent::Name(name.to_owned())),
            Utf8Component::CurDir => None,
            Utf8Component::Prefix(_) => unreachable!("only Unix paths are supported"),
        })
        .collect()
}

fn open_trusted_parent(path: &Utf8Path, uid: u32) -> Result<OwnedFd, RuntimeError> {
    let root_path = Utf8Path::new("/");
    let root = open_directory(fs::CWD, "/", root_path)?;
    let metadata = fs::fstat(&root)
        .map_err(|source| filesystem_error(root_path, "inspect ancestor", source))?;
    validate_ancestor(&metadata, uid, root_path)?;
    let mut stack = vec![(root, root_path.to_owned())];
    let mut pending = ancestor_components(path);
    let mut followed = 0;
    while let Some(component) = pending.pop_front() {
        match component {
            AncestorComponent::Root => stack.truncate(1),
            AncestorComponent::Parent => {
                if stack.len() > 1 {
                    stack.pop();
                }
            }
            AncestorComponent::Name(name) => {
                let (parent, parent_path) = stack.last().expect("root descriptor is retained");
                let candidate = parent_path.join(&name);
                let metadata = fs::statat(parent, name.as_str(), fs::AtFlags::SYMLINK_NOFOLLOW)
                    .map_err(|source| filesystem_error(&candidate, "inspect ancestor", source))?;
                if fs::FileType::from_raw_mode(metadata.st_mode) == fs::FileType::Symlink {
                    if metadata.st_uid != 0 {
                        return Err(unsafe_path(
                            &candidate,
                            "ancestor symlink must be root-owned",
                        ));
                    }
                    followed += 1;
                    if followed > MAX_ANCESTOR_SYMLINKS {
                        return Err(unsafe_path(&candidate, "too many ancestor symlinks"));
                    }
                    let target =
                        fs::readlinkat(parent, name.as_str(), Vec::new()).map_err(|source| {
                            filesystem_error(&candidate, "read ancestor symlink", source)
                        })?;
                    let target = target.to_str().map_err(|_| {
                        unsafe_path(&candidate, "ancestor symlink target must be UTF-8")
                    })?;
                    let after = fs::statat(parent, name.as_str(), fs::AtFlags::SYMLINK_NOFOLLOW)
                        .map_err(|source| {
                            filesystem_error(&candidate, "reinspect ancestor symlink", source)
                        })?;
                    if metadata.st_dev != after.st_dev
                        || metadata.st_ino != after.st_ino
                        || after.st_uid != 0
                        || fs::FileType::from_raw_mode(after.st_mode) != fs::FileType::Symlink
                    {
                        return Err(unsafe_path(
                            &candidate,
                            "ancestor symlink changed during inspection",
                        ));
                    }
                    let mut target_components = ancestor_components(Utf8Path::new(target));
                    target_components.append(&mut pending);
                    pending = target_components;
                } else {
                    let descriptor = open_directory(parent, &name, &candidate)?;
                    let metadata = fs::fstat(&descriptor).map_err(|source| {
                        filesystem_error(&candidate, "inspect opened ancestor", source)
                    })?;
                    validate_ancestor(&metadata, uid, &candidate)?;
                    stack.push((descriptor, candidate));
                }
            }
        }
    }
    Ok(stack.pop().expect("root descriptor is retained").0)
}

fn open_directory<Fd: AsFd>(
    parent: Fd,
    name: &str,
    path: &Utf8Path,
) -> Result<OwnedFd, RuntimeError> {
    fs::openat(
        parent,
        name,
        fs::OFlags::RDONLY | fs::OFlags::DIRECTORY | fs::OFlags::NOFOLLOW | fs::OFlags::CLOEXEC,
        fs::Mode::empty(),
    )
    .map_err(|source| filesystem_error(path, "open directory without following links", source))
}

fn validate_ancestor(metadata: &fs::Stat, uid: u32, path: &Utf8Path) -> Result<(), RuntimeError> {
    if fs::FileType::from_raw_mode(metadata.st_mode) != fs::FileType::Directory {
        return Err(unsafe_path(path, "ancestor must be a directory"));
    }
    if metadata.st_uid != 0 && metadata.st_uid != uid {
        return Err(unsafe_path(
            path,
            "ancestor must belong to root or the current user",
        ));
    }
    let mode = fs::Mode::from_raw_mode(metadata.st_mode).bits();
    let shared_temporary_directory = metadata.st_uid == 0 && mode & 0o1000 != 0;
    if mode & 0o022 != 0 && !shared_temporary_directory {
        return Err(unsafe_path(
            path,
            "writable ancestor must be root-owned and sticky",
        ));
    }
    Ok(())
}

fn validate_owned_object(
    metadata: &fs::Stat,
    kind: fs::FileType,
    mode: fs::Mode,
    uid: u32,
    path: &Utf8Path,
) -> Result<(), RuntimeError> {
    if fs::FileType::from_raw_mode(metadata.st_mode) != kind {
        return Err(unsafe_path(path, "unexpected filesystem object type"));
    }
    if metadata.st_uid != uid {
        return Err(unsafe_path(
            path,
            "filesystem object must belong to the current user",
        ));
    }
    if fs::Mode::from_raw_mode(metadata.st_mode).bits() & 0o7777 != mode.bits() {
        return Err(unsafe_path(
            path,
            "incorrect permissions or special mode bits",
        ));
    }
    Ok(())
}

fn unsafe_path(path: &Utf8Path, reason: &'static str) -> RuntimeError {
    RuntimeError::UnsafePath {
        path: path.to_owned(),
        reason,
    }
}

fn filesystem_error(path: &Utf8Path, operation: &'static str, source: io::Errno) -> RuntimeError {
    RuntimeError::Filesystem {
        path: path.to_owned(),
        operation,
        source,
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
    #[error("daemon lock is busy at {0}")]
    LockBusy(Utf8PathBuf),
    #[error("live listener occupies {0}; preserving endpoint")]
    LiveEndpoint(Utf8PathBuf),
    #[error("deadline exceeded while {0}")]
    Deadline(&'static str),
    #[error("cannot {operation} at {path}: {source}")]
    Socket {
        path: Utf8PathBuf,
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("XDG_RUNTIME_DIR must be a UTF-8 path")]
    NonUtf8Environment,
    #[error("{0}")]
    InvalidPath(&'static str),
    #[error("socket path {path} exceeds the platform limit of {maximum} bytes")]
    SocketPathTooLong { path: Utf8PathBuf, maximum: usize },
    #[error("unsafe runtime path {path}: {reason}")]
    UnsafePath {
        path: Utf8PathBuf,
        reason: &'static str,
    },
    #[error("cannot {operation} at {path}: {source}")]
    Filesystem {
        path: Utf8PathBuf,
        operation: &'static str,
        #[source]
        source: io::Errno,
    },
    #[error("cannot retrieve local peer credentials: {0}")]
    PeerCredentials(#[source] std::io::Error),
    #[error("local peer UID {actual} does not match helper UID {expected}")]
    PeerUid { expected: u32, actual: u32 },
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

    #[tokio::test(flavor = "current_thread")]
    async fn kernel_peer_uid_is_checked_against_expected_user() {
        let (stream, _peer) = net::UnixStream::pair().unwrap();
        let uid = rustix::process::getuid().as_raw();
        authorize_peer_uid(&stream, uid).unwrap();
        let foreign = if uid == 0 { 1 } else { 0 };
        assert!(
            matches!(authorize_peer_uid(&stream, foreign), Err(RuntimeError::PeerUid { expected, actual }) if expected == foreign && actual == uid)
        );
    }
}
