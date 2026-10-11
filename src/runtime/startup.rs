//! Lifetime ownership, conservative recovery and identity-scoped socket cleanup.
use std::io::ErrorKind;

use tokio::time::Instant;
use tokio::time::timeout_at;

use super::*;

const PRIVATE_FILE_MODE: fs::Mode = fs::Mode::RUSR.union(fs::Mode::WUSR);
const ENDPOINTS: [&str; 2] = ["agent.sock", "control.sock"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Identity {
    device: fs::Dev,
    inode: u64,
}

impl Identity {
    fn of(metadata: &fs::Stat) -> Self {
        Self {
            device: metadata.st_dev,
            inode: metadata.st_ino,
        }
    }
}

/// Holds the exclusive daemon lock until dropped. The lock file is never unlinked.
#[derive(Debug)]
pub struct RuntimeLock {
    descriptor: OwnedFd,
    identity: Identity,
    directory: RuntimeDirectory,
}

impl RuntimeLock {
    /// Try to own this runtime directory without waiting for another daemon.
    ///
    /// # Errors
    /// Returns `LockBusy` for contention and rejects unsafe or replaced objects.
    pub fn acquire(directory: RuntimeDirectory) -> Result<Self, RuntimeError> {
        let directory = directory.validate_endpoints()?;
        let flags =
            fs::OFlags::RDWR | fs::OFlags::NOFOLLOW | fs::OFlags::CLOEXEC | fs::OFlags::NONBLOCK;
        let descriptor = match fs::openat(
            &directory,
            "daemon.lock",
            flags | fs::OFlags::CREATE | fs::OFlags::EXCL,
            PRIVATE_FILE_MODE,
        ) {
            Ok(descriptor) => descriptor,
            Err(io::Errno::EXIST) => {
                fs::openat(&directory, "daemon.lock", flags, fs::Mode::empty()).map_err(
                    |source| filesystem_error(directory.paths.lock(), "open lock", source),
                )?
            }
            Err(source) => {
                return Err(filesystem_error(
                    directory.paths.lock(),
                    "create lock",
                    source,
                ));
            }
        };
        let metadata = fs::fstat(&descriptor)
            .map_err(|source| filesystem_error(directory.paths.lock(), "inspect lock", source))?;
        validate_owned_object(
            &metadata,
            fs::FileType::RegularFile,
            PRIVATE_FILE_MODE,
            directory.uid,
            directory.paths.lock(),
        )?;
        fs::flock(&descriptor, fs::FlockOperation::NonBlockingLockExclusive).map_err(|source| {
            if source == io::Errno::WOULDBLOCK {
                RuntimeError::LockBusy(directory.paths.lock().to_owned())
            } else {
                filesystem_error(directory.paths.lock(), "lock", source)
            }
        })?;
        let guard = Self {
            descriptor,
            identity: Identity::of(&metadata),
            directory,
        };
        guard.validate_identity()?;
        Ok(guard)
    }

    pub fn directory(&self) -> &RuntimeDirectory {
        &self.directory
    }

    fn validate_identity(&self) -> Result<(), RuntimeError> {
        let paths = &self.directory.paths;
        let parent = open_trusted_parent(
            paths.directory.parent().expect("validated final component"),
            self.directory.uid,
        )?;
        let reopened = open_directory(
            &parent,
            paths
                .directory
                .file_name()
                .expect("validated final component"),
            paths.directory(),
        )?;
        let named_directory = fs::fstat(&reopened).map_err(|source| {
            filesystem_error(paths.directory(), "inspect named directory", source)
        })?;
        validate_owned_object(
            &named_directory,
            fs::FileType::Directory,
            fs::Mode::RWXU,
            self.directory.uid,
            paths.directory(),
        )?;
        let retained = fs::fstat(&self.directory).map_err(|source| {
            filesystem_error(paths.directory(), "inspect retained directory", source)
        })?;
        if Identity::of(&named_directory) != Identity::of(&retained) {
            return Err(unsafe_path(
                paths.directory(),
                "runtime directory identity changed",
            ));
        }
        let named_lock = stat_entry(&self.directory, "daemon.lock")?
            .ok_or_else(|| unsafe_path(paths.lock(), "lock entry disappeared"))?;
        validate_owned_object(
            &named_lock,
            fs::FileType::RegularFile,
            PRIVATE_FILE_MODE,
            self.directory.uid,
            paths.lock(),
        )?;
        let opened_lock = fs::fstat(&self.descriptor)
            .map_err(|source| filesystem_error(paths.lock(), "inspect held lock", source))?;
        if Identity::of(&named_lock) != self.identity || Identity::of(&opened_lock) != self.identity
        {
            return Err(unsafe_path(paths.lock(), "lock identity changed"));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct BoundSocket {
    listener: net::UnixListener,
    name: &'static str,
    identity: Identity,
    private: bool,
}

/// Owns both listeners and their cleanup while retaining the lifetime lock.
#[derive(Debug)]
pub struct RuntimeSockets {
    // Drop listeners before releasing the lock, including abandoned startup paths.
    sockets: Vec<BoundSocket>,
    lock: RuntimeLock,
    cleaned: bool,
}

impl RuntimeSockets {
    /// Recover refused stale sockets and bind both private endpoints.
    ///
    /// # Errors
    /// Preserves live/uncertain listeners, unsafe objects and changed identities.
    pub async fn bind(lock: RuntimeLock, deadline: Instant) -> Result<Self, RuntimeError> {
        let mut guard = Self {
            sockets: Vec::with_capacity(2),
            lock,
            cleaned: false,
        };
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline("preparing listeners"));
        }
        guard.recover(deadline).await?;
        for name in ENDPOINTS {
            if Instant::now() >= deadline {
                return Err(RuntimeError::Deadline("binding listeners"));
            }
            guard.bind_one(name)?;
        }
        Ok(guard)
    }

    fn bind_one(&mut self, name: &'static str) -> Result<(), RuntimeError> {
        self.lock.validate_identity()?;
        let directory = &self.lock.directory;
        let path = directory.paths.directory.join(name);
        let listener = net::UnixListener::bind(&path).map_err(|source| RuntimeError::Socket {
            path: path.clone(),
            operation: "bind",
            source,
        })?;
        let metadata = stat_entry(directory, name)?
            .ok_or_else(|| unsafe_path(&path, "new socket disappeared"))?;
        if fs::FileType::from_raw_mode(metadata.st_mode) != fs::FileType::Socket
            || metadata.st_uid != directory.uid
        {
            return Err(unsafe_path(
                &path,
                "new socket has unexpected type or owner",
            ));
        }
        let identity = Identity::of(&metadata);
        self.sockets.push(BoundSocket {
            listener,
            name,
            identity,
            private: false,
        });
        self.lock.validate_identity()?;
        require_identity(directory, name, identity)?;
        // Only our just-bound socket in a private, same-user directory is changed.
        // Linux rustix does not support SYMLINK_NOFOLLOW for chmodat. Identity
        // checks are not atomic against an actively hostile same-user process.
        fs::chmodat(directory, name, PRIVATE_FILE_MODE, fs::AtFlags::empty())
            .map_err(|source| filesystem_error(&path, "set new socket permissions", source))?;
        let metadata = require_identity(directory, name, identity)?;
        validate_owned_object(
            &metadata,
            fs::FileType::Socket,
            PRIVATE_FILE_MODE,
            directory.uid,
            &path,
        )?;
        self.sockets
            .last_mut()
            .expect("new socket recorded")
            .private = true;
        self.lock.validate_identity()?;
        Ok(())
    }

    pub fn directory(&self) -> &RuntimeDirectory {
        &self.lock.directory
    }

    pub fn agent_listener(&self) -> &net::UnixListener {
        &self.sockets[0].listener
    }

    pub fn control_listener(&self) -> &net::UnixListener {
        &self.sockets[1].listener
    }

    async fn recover(&self, deadline: Instant) -> Result<(), RuntimeError> {
        self.lock.validate_identity()?;
        let directory = &self.lock.directory;
        let mut stale = Vec::new();
        for name in ENDPOINTS {
            let Some(metadata) = stat_entry(directory, name)? else {
                continue;
            };
            let path = directory.paths.directory.join(name);
            validate_owned_object(
                &metadata,
                fs::FileType::Socket,
                PRIVATE_FILE_MODE,
                directory.uid,
                &path,
            )?;
            let identity = Identity::of(&metadata);
            match timeout_at(deadline, net::UnixStream::connect(&path)).await {
                Ok(Ok(_stream)) => return Err(RuntimeError::LiveEndpoint(path)),
                Ok(Err(error)) if error.kind() == ErrorKind::ConnectionRefused => {
                    stale.push((name, identity))
                }
                Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
                    if stat_entry(directory, name)?.is_some() {
                        return Err(unsafe_path(&path, "endpoint changed during recovery"));
                    }
                }
                Ok(Err(source)) => {
                    return Err(RuntimeError::Socket {
                        path,
                        operation: "check existing listener",
                        source,
                    });
                }
                Err(_) => return Err(RuntimeError::Deadline("checking existing listeners")),
            }
        }
        // Finish assessing both names before deleting either.
        for (name, identity) in stale {
            self.lock.validate_identity()?;
            let metadata = require_identity(directory, name, identity)?;
            let path = directory.paths.directory.join(name);
            validate_owned_object(
                &metadata,
                fs::FileType::Socket,
                PRIVATE_FILE_MODE,
                directory.uid,
                &path,
            )?;
            fs::unlinkat(directory, name, fs::AtFlags::empty())
                .map_err(|source| filesystem_error(&path, "remove stale socket", source))?;
        }
        Ok(())
    }

    /// Remove only matching owned sockets, then release listeners and the lock.
    ///
    /// # Errors
    /// Reports replacements or filesystem failures without removing those objects.
    pub fn cleanup(mut self) -> Result<(), RuntimeError> {
        self.cleaned = true;
        self.cleanup_inner()
    }

    fn cleanup_inner(&self) -> Result<(), RuntimeError> {
        let directory = &self.lock.directory;
        let metadata = fs::fstat(directory).map_err(|source| {
            filesystem_error(
                directory.paths.directory(),
                "inspect cleanup directory",
                source,
            )
        })?;
        validate_owned_object(
            &metadata,
            fs::FileType::Directory,
            fs::Mode::RWXU,
            directory.uid,
            directory.paths.directory(),
        )?;
        let mut first_error = None;
        for socket in &self.sockets {
            let result = (|| {
                let Some(metadata) = stat_entry(directory, socket.name)? else {
                    return Ok(());
                };
                let path = directory.paths.directory.join(socket.name);
                if Identity::of(&metadata) != socket.identity
                    || fs::FileType::from_raw_mode(metadata.st_mode) != fs::FileType::Socket
                    || metadata.st_uid != directory.uid
                {
                    return Err(unsafe_path(
                        &path,
                        "cleanup preserved a replacement endpoint",
                    ));
                }
                if socket.private {
                    validate_owned_object(
                        &metadata,
                        fs::FileType::Socket,
                        PRIVATE_FILE_MODE,
                        directory.uid,
                        &path,
                    )?;
                }
                fs::unlinkat(directory, socket.name, fs::AtFlags::empty())
                    .map_err(|source| filesystem_error(&path, "remove instance socket", source))
            })();
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for RuntimeSockets {
    fn drop(&mut self) {
        if !self.cleaned
            && let Err(error) = self.cleanup_inner()
        {
            eprintln!("ssh-auth-helper: {error}");
        }
    }
}

fn stat_entry(directory: &RuntimeDirectory, name: &str) -> Result<Option<fs::Stat>, RuntimeError> {
    match fs::statat(directory, name, fs::AtFlags::SYMLINK_NOFOLLOW) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(io::Errno::NOENT) => Ok(None),
        Err(source) => Err(filesystem_error(
            &directory.paths.directory.join(name),
            "inspect endpoint",
            source,
        )),
    }
}

fn require_identity(
    directory: &RuntimeDirectory,
    name: &str,
    identity: Identity,
) -> Result<fs::Stat, RuntimeError> {
    let path = directory.paths.directory.join(name);
    let metadata =
        stat_entry(directory, name)?.ok_or_else(|| unsafe_path(&path, "endpoint disappeared"))?;
    if Identity::of(&metadata) != identity
        || fs::FileType::from_raw_mode(metadata.st_mode) != fs::FileType::Socket
        || metadata.st_uid != directory.uid
    {
        return Err(unsafe_path(&path, "endpoint identity changed"));
    }
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn partial_startup_rolls_back_only_the_created_socket() {
        let parent = tempfile::tempdir_in("/tmp").unwrap();
        let path = Utf8PathBuf::from_path_buf(parent.path().join("helper")).unwrap();
        let paths = RuntimePaths::discover(Some(&path), None).unwrap();
        let lock = RuntimeLock::acquire(RuntimeDirectory::prepare(paths.clone()).unwrap()).unwrap();
        let mut guard = RuntimeSockets {
            sockets: Vec::new(),
            lock,
            cleaned: false,
        };
        guard.bind_one("agent.sock").unwrap();
        std::fs::write(paths.control(), b"preserve this conflicting file").unwrap();
        assert!(guard.bind_one("control.sock").is_err());
        drop(guard);
        assert!(!paths.agent().exists());
        assert_eq!(
            std::fs::read(paths.control()).unwrap(),
            b"preserve this conflicting file"
        );
        assert!(paths.lock().exists());
    }
}
