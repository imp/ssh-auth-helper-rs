use std::fs;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use camino::Utf8PathBuf;
use ssh_auth_helper::runtime::RuntimeDirectory;
use ssh_auth_helper::runtime::RuntimeError;
use ssh_auth_helper::runtime::RuntimePaths;

fn fixture() -> (tempfile::TempDir, RuntimePaths) {
    let parent = tempfile::tempdir_in("/tmp").unwrap();
    let path = Utf8PathBuf::from_path_buf(parent.path().join("helper")).unwrap();
    let paths = RuntimePaths::discover(Some(&path), None).unwrap();
    (parent, paths)
}

fn set_mode(path: &camino::Utf8Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn create_directory(paths: &RuntimePaths) {
    fs::create_dir(paths.directory()).unwrap();
    set_mode(paths.directory(), 0o700);
}

#[test]
fn missing_leaf_is_created_privately_and_valid_directory_is_reused() {
    let (_parent, paths) = fixture();
    let opened = RuntimeDirectory::prepare(paths.clone()).unwrap();
    let metadata = fs::symlink_metadata(paths.directory()).unwrap();
    assert!(metadata.is_dir());
    assert_eq!(metadata.uid(), rustix::process::getuid().as_raw());
    assert_eq!(metadata.mode() & 0o7777, 0o700);
    assert_eq!(opened.paths(), &paths);
    assert!(!paths.agent().exists());
    assert!(!paths.control().exists());
    assert!(!paths.lock().exists());
    let again = RuntimeDirectory::prepare(paths).unwrap();
    let original = rustix::fs::fstat(&opened).unwrap();
    let reopened = rustix::fs::fstat(&again).unwrap();
    assert_eq!(
        (original.st_dev, original.st_ino),
        (reopened.st_dev, reopened.st_ino)
    );
}

#[test]
fn missing_parent_is_not_created_recursively() {
    let (parent, _) = fixture();
    let path = Utf8PathBuf::from_path_buf(parent.path().join("missing/helper")).unwrap();
    let paths = RuntimePaths::discover(Some(&path), None).unwrap();
    assert!(matches!(
        RuntimeDirectory::prepare(paths),
        Err(RuntimeError::Filesystem {
            source: rustix::io::Errno::NOENT,
            ..
        })
    ));
    assert!(!parent.path().join("missing").exists());
}

#[test]
fn invalid_directory_modes_are_rejected_without_repair() {
    for mode in [0o750, 0o777, 0o1700, 0o2700, 0o4700] {
        let (_parent, paths) = fixture();
        create_directory(&paths);
        // Darwin inherits /tmp's group; use our own group so SGID is not stripped.
        rustix::fs::chownat(
            rustix::fs::CWD,
            paths.directory().as_str(),
            None,
            Some(rustix::process::getgid()),
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )
        .unwrap();
        let marker = paths.directory().join("marker");
        fs::write(&marker, b"preserve me").unwrap();
        set_mode(paths.directory(), mode);
        let before = fs::symlink_metadata(paths.directory()).unwrap();
        assert_eq!(
            before.mode() & 0o7777,
            mode,
            "fixture must retain requested mode bits"
        );
        let result = RuntimeDirectory::prepare(paths.clone());
        assert!(
            matches!(result, Err(RuntimeError::UnsafePath { .. })),
            "requested mode {mode:o}, actual mode {:o}, result {result:?}",
            before.mode() & 0o7777
        );
        let after = fs::symlink_metadata(paths.directory()).unwrap();
        assert_eq!(
            (before.ino(), before.mode(), before.uid()),
            (after.ino(), after.mode(), after.uid())
        );
        assert_eq!(fs::read(marker).unwrap(), b"preserve me");
    }
}

#[test]
fn runtime_leaf_file_and_symlink_are_preserved_and_rejected() {
    let (_parent, paths) = fixture();
    fs::write(paths.directory(), b"keep").unwrap();
    assert!(RuntimeDirectory::prepare(paths.clone()).is_err());
    assert_eq!(fs::read(paths.directory()).unwrap(), b"keep");
    fs::remove_file(paths.directory()).unwrap();
    let target = paths.directory().with_file_name("target");
    fs::create_dir(&target).unwrap();
    set_mode(&target, 0o700);
    symlink(&target, paths.directory()).unwrap();
    assert!(RuntimeDirectory::prepare(paths.clone()).is_err());
    assert!(
        fs::symlink_metadata(paths.directory())
            .unwrap()
            .is_symlink()
    );
    assert!(target.is_dir());
}

#[test]
fn writable_non_sticky_ancestor_is_rejected_before_creation() {
    let (parent, paths) = fixture();
    fs::set_permissions(parent.path(), fs::Permissions::from_mode(0o777)).unwrap();
    assert!(matches!(
        RuntimeDirectory::prepare(paths.clone()),
        Err(RuntimeError::UnsafePath { .. })
    ));
    assert!(!paths.directory().exists());
    assert_eq!(fs::metadata(parent.path()).unwrap().mode() & 0o7777, 0o777);
}

#[test]
fn ancestor_alias_policy_follows_only_root_owned_links() {
    let (parent, _) = fixture();
    let target = Utf8PathBuf::from_path_buf(parent.path().join("target")).unwrap();
    fs::create_dir(&target).unwrap();
    set_mode(&target, 0o700);
    let alias = target.with_file_name("alias");
    symlink("target", &alias).unwrap();
    let leaf = alias.join("helper");
    let paths = RuntimePaths::discover(Some(&leaf), None).unwrap();
    let result = RuntimeDirectory::prepare(paths);
    if rustix::process::getuid().is_root() {
        result.unwrap();
        assert!(target.join("helper").is_dir());
    } else {
        assert!(matches!(result, Err(RuntimeError::UnsafePath { .. })));
        assert!(!target.join("helper").exists());
    }
    assert!(fs::symlink_metadata(alias).unwrap().is_symlink());
}

#[test]
fn concurrent_starters_retain_the_same_directory_object() {
    let (_parent, paths) = fixture();
    let barrier = Arc::new(Barrier::new(12));
    #[allow(
        clippy::needless_collect,
        reason = "spawn every barrier participant before joining any thread"
    )]
    let starters: Vec<_> = (0..12)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let paths = paths.clone();
            thread::spawn(move || {
                barrier.wait();
                RuntimeDirectory::prepare(paths).unwrap()
            })
        })
        .collect();
    let directories: Vec<_> = starters
        .into_iter()
        .map(|starter| starter.join().unwrap())
        .collect();
    let first = rustix::fs::fstat(&directories[0]).unwrap();
    for directory in &directories {
        let metadata = rustix::fs::fstat(directory).unwrap();
        assert_eq!(
            (first.st_dev, first.st_ino),
            (metadata.st_dev, metadata.st_ino)
        );
    }
}

#[test]
fn valid_existing_endpoints_are_inspected_and_preserved() {
    let (_parent, paths) = fixture();
    create_directory(&paths);
    let _agent = UnixListener::bind(paths.agent()).unwrap();
    let _control = UnixListener::bind(paths.control()).unwrap();
    fs::write(paths.lock(), b"lock contents").unwrap();
    for path in [paths.agent(), paths.control(), paths.lock()] {
        set_mode(path, 0o600);
    }
    let before: Vec<_> = [paths.agent(), paths.control(), paths.lock()]
        .into_iter()
        .map(|path| fs::symlink_metadata(path).unwrap().ino())
        .collect();
    RuntimeDirectory::prepare(paths.clone())
        .unwrap()
        .validate_endpoints()
        .unwrap();
    let after: Vec<_> = [paths.agent(), paths.control(), paths.lock()]
        .into_iter()
        .map(|path| fs::symlink_metadata(path).unwrap().ino())
        .collect();
    assert_eq!(before, after);
    assert_eq!(fs::read(paths.lock()).unwrap(), b"lock contents");
}

#[test]
fn endpoint_files_directories_and_symlinks_are_rejected_without_removal() {
    for name in ["agent.sock", "control.sock", "daemon.lock"] {
        for kind in ["file", "directory", "symlink"] {
            if name == "daemon.lock" && kind == "file" {
                continue;
            }
            let (_parent, paths) = fixture();
            create_directory(&paths);
            let endpoint = paths.directory().join(name);
            match kind {
                "file" => {
                    fs::write(&endpoint, b"keep").unwrap();
                    set_mode(&endpoint, 0o600);
                }
                "directory" => fs::create_dir(&endpoint).unwrap(),
                "symlink" => symlink("missing-target", &endpoint).unwrap(),
                _ => unreachable!(),
            }
            let before = fs::symlink_metadata(&endpoint).unwrap();
            assert!(matches!(
                RuntimeDirectory::prepare(paths),
                Err(RuntimeError::UnsafePath { .. })
            ));
            let after = fs::symlink_metadata(&endpoint).unwrap();
            assert_eq!(
                (before.ino(), before.mode(), before.uid()),
                (after.ino(), after.mode(), after.uid())
            );
            if kind == "file" {
                assert_eq!(fs::read(endpoint).unwrap(), b"keep");
            }
        }
    }
}

#[test]
fn unsafe_socket_and_lock_modes_are_rejected() {
    for name in ["agent.sock", "control.sock", "daemon.lock"] {
        let (_parent, paths) = fixture();
        create_directory(&paths);
        let endpoint = paths.directory().join(name);
        let _listener = if name == "daemon.lock" {
            fs::write(&endpoint, b"keep").unwrap();
            None
        } else {
            Some(UnixListener::bind(&endpoint).unwrap())
        };
        set_mode(&endpoint, 0o666);
        assert!(matches!(
            RuntimeDirectory::prepare(paths),
            Err(RuntimeError::UnsafePath { .. })
        ));
        assert_eq!(
            fs::symlink_metadata(endpoint).unwrap().mode() & 0o7777,
            0o666
        );
    }
}

#[test]
fn retained_descriptor_refers_to_original_directory_after_rename() {
    let (_parent, paths) = fixture();
    let directory = RuntimeDirectory::prepare(paths.clone()).unwrap();
    let moved = paths.directory().with_file_name("moved");
    fs::rename(paths.directory(), &moved).unwrap();
    create_directory(&paths);
    fs::write(
        moved.join("agent.sock"),
        b"invalid endpoint in original directory",
    )
    .unwrap();
    let pinned = rustix::fs::fstat(&directory).unwrap();
    assert!(matches!(
        directory.validate_endpoints(),
        Err(RuntimeError::UnsafePath { .. })
    ));
    assert_eq!(pinned.st_ino, fs::metadata(&moved).unwrap().ino());
    assert_ne!(
        pinned.st_ino,
        fs::metadata(paths.directory()).unwrap().ino()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn same_user_peer_is_authorized_using_real_credentials() {
    let (_parent, paths) = fixture();
    let directory = RuntimeDirectory::prepare(paths).unwrap();
    let (stream, _peer) = tokio::net::UnixStream::pair().unwrap();
    directory.authorize_peer(&stream).unwrap();
}

#[cfg(target_os = "linux")]
mod privileged {
    use super::*;
    use std::os::unix::process::CommandExt;
    use std::process::Command;
    use std::time::Duration;

    use rustix::fs::AtFlags;
    use rustix::process::Uid;

    fn require_root() {
        assert!(
            rustix::process::getuid().is_root(),
            "run only in the isolated root test container"
        );
    }

    fn change_owner(path: &camino::Utf8Path) {
        rustix::fs::chownat(
            rustix::fs::CWD,
            path.as_str(),
            Some(Uid::from_raw(1001)),
            None,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .unwrap();
    }

    #[test]
    #[ignore = "requires an isolated Linux container running as root"]
    fn real_foreign_directory_ancestor_and_endpoint_owners_are_rejected() {
        require_root();
        let (_parent, paths) = fixture();
        create_directory(&paths);
        change_owner(paths.directory());
        assert!(matches!(
            RuntimeDirectory::prepare(paths.clone()),
            Err(RuntimeError::UnsafePath { .. })
        ));
        assert_eq!(fs::metadata(paths.directory()).unwrap().uid(), 1001);

        for name in ["agent.sock", "control.sock", "daemon.lock"] {
            let (_parent, paths) = fixture();
            create_directory(&paths);
            let endpoint = paths.directory().join(name);
            let _listener = if name == "daemon.lock" {
                fs::write(&endpoint, b"keep").unwrap();
                None
            } else {
                Some(UnixListener::bind(&endpoint).unwrap())
            };
            set_mode(&endpoint, 0o600);
            change_owner(&endpoint);
            assert!(matches!(
                RuntimeDirectory::prepare(paths),
                Err(RuntimeError::UnsafePath { .. })
            ));
            assert_eq!(fs::symlink_metadata(endpoint).unwrap().uid(), 1001);
        }

        let (parent, paths) = fixture();
        change_owner(camino::Utf8Path::from_path(parent.path()).unwrap());
        assert!(matches!(
            RuntimeDirectory::prepare(paths.clone()),
            Err(RuntimeError::UnsafePath { .. })
        ));
        assert!(!paths.directory().exists());
    }

    #[test]
    #[ignore = "requires an isolated Linux container running as root"]
    fn foreign_owned_ancestor_alias_is_rejected() {
        require_root();
        let (parent, _) = fixture();
        let alias = Utf8PathBuf::from_path_buf(parent.path().join("alias")).unwrap();
        symlink("/tmp", &alias).unwrap();
        change_owner(&alias);
        let leaf = alias.join("must-not-be-created");
        let paths = RuntimePaths::discover(Some(&leaf), None).unwrap();
        assert!(matches!(
            RuntimeDirectory::prepare(paths),
            Err(RuntimeError::UnsafePath { .. })
        ));
        assert_eq!(fs::symlink_metadata(alias).unwrap().uid(), 1001);
    }

    #[test]
    #[ignore = "requires an isolated Linux container running as root"]
    fn root_owned_symlink_loop_hits_finite_traversal_limit() {
        require_root();
        let (parent, _) = fixture();
        let alias = Utf8PathBuf::from_path_buf(parent.path().join("loop")).unwrap();
        symlink("loop", &alias).unwrap();
        let leaf = alias.join("helper");
        let paths = RuntimePaths::discover(Some(&leaf), None).unwrap();
        assert!(matches!(
            RuntimeDirectory::prepare(paths),
            Err(RuntimeError::UnsafePath {
                reason: "too many ancestor symlinks",
                ..
            })
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires isolated Linux root container and python3"]
    async fn real_foreign_peer_is_rejected_even_if_fixture_permissions_are_weakened() {
        require_root();
        let (parent, paths) = fixture();
        let directory = RuntimeDirectory::prepare(paths.clone()).unwrap();
        let listener = tokio::net::UnixListener::bind(paths.agent()).unwrap();
        // Deliberately weaken only this fixture to exercise the independent UID gate.
        fs::set_permissions(parent.path(), fs::Permissions::from_mode(0o711)).unwrap();
        set_mode(paths.directory(), 0o711);
        set_mode(paths.agent(), 0o666);
        let mut child = Command::new("python3")
            .arg("-c")
            .arg("import socket,sys; s=socket.socket(socket.AF_UNIX); s.connect(sys.argv[1]); s.sendall(b'x'); s.close()")
            .arg(paths.agent().as_str())
            .uid(1001)
            .gid(1001)
            .spawn().unwrap();
        let accepted = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await;
        if accepted.is_err() {
            child.kill().unwrap();
        }
        let status = child.wait().unwrap();
        assert!(status.success());
        let (stream, _) = accepted.unwrap().unwrap();
        assert!(matches!(
            directory.authorize_peer(&stream),
            Err(RuntimeError::PeerUid {
                expected: 0,
                actual: 1001
            })
        ));
    }
}
