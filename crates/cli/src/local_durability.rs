use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use capabilities::Capability;
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStatus,
    CheckpointStore, Lease,
};
use dag_core::FlowId;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

pub struct FsCheckpointStore {
    root: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct LeaseRecord {
    checkpoint_id: String,
    flow_id: FlowId,
    run_id: String,
    expires_at_ms: u64,
}

#[cfg(test)]
type LeaseHook = Box<dyn Fn(&Path) + Send + Sync + 'static>;

#[cfg(test)]
static LEASE_CREATE_HOOK: OnceLock<Mutex<Option<LeaseHook>>> = OnceLock::new();

#[cfg(test)]
static LEASE_REMOVE_HOOK: OnceLock<Mutex<Option<LeaseHook>>> = OnceLock::new();

#[cfg(test)]
static LEASE_OPEN_HOOK: OnceLock<Mutex<Option<LeaseHook>>> = OnceLock::new();

impl FsCheckpointStore {
    pub fn new() -> Self {
        Self::with_root(Self::default_root())
    }

    pub fn with_root(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn default_root() -> PathBuf {
        PathBuf::from(".flow").join("checkpoints")
    }

    fn flow_dir(&self, flow_id: &FlowId) -> PathBuf {
        self.root.join(flow_id.as_str())
    }

    fn run_dir(&self, flow_id: &FlowId, run_id: &str) -> PathBuf {
        self.flow_dir(flow_id).join(run_id)
    }

    fn lease_dir(&self, handle: &CheckpointHandle) -> PathBuf {
        self.run_dir(&handle.flow_id, &handle.run_id).join(".leases")
    }

    fn lease_path(&self, handle: &CheckpointHandle) -> PathBuf {
        self.lease_dir(handle)
            .join(format!("{}.json", handle.checkpoint_id))
    }

    fn checkpoint_path(&self, handle: &CheckpointHandle) -> PathBuf {
        self.run_dir(&handle.flow_id, &handle.run_id)
            .join(format!("{}.json", handle.checkpoint_id))
    }

    fn checkpoint_path_from_record(&self, record: &CheckpointRecord) -> PathBuf {
        self.run_dir(&record.flow_id, &record.run_id)
            .join(format!("{}.json", record.checkpoint_id))
    }

    fn map_io_error(err: std::io::Error) -> CheckpointError {
        if err.kind() == std::io::ErrorKind::NotFound {
            CheckpointError::NotFound
        } else {
            CheckpointError::Storage(err.to_string())
        }
    }

    fn now_ms() -> u64 {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(now_ms).unwrap_or(u64::MAX)
    }

    fn is_expired(record: &CheckpointRecord, now_ms: u64) -> bool {
        let Some(ttl_ms) = record.ttl_ms else {
            return false;
        };
        record.created_at_ms.saturating_add(ttl_ms) <= now_ms
    }

    #[cfg(test)]
    fn set_lease_create_hook(hook: Option<LeaseHook>) {
        let lock = LEASE_CREATE_HOOK.get_or_init(|| Mutex::new(None));
        *lock.lock().expect("lease create hook lock") = hook;
    }

    #[cfg(test)]
    fn run_lease_create_hook(path: &Path) {
        let Some(lock) = LEASE_CREATE_HOOK.get() else {
            return;
        };
        if let Some(hook) = lock.lock().expect("lease create hook lock").as_ref() {
            hook(path);
        }
    }

    #[cfg(not(test))]
    fn run_lease_create_hook(_path: &Path) {}

    #[cfg(test)]
    fn set_lease_remove_hook(hook: Option<LeaseHook>) {
        let lock = LEASE_REMOVE_HOOK.get_or_init(|| Mutex::new(None));
        *lock.lock().expect("lease remove hook lock") = hook;
    }

    #[cfg(test)]
    fn run_lease_remove_hook(path: &Path) {
        let Some(lock) = LEASE_REMOVE_HOOK.get() else {
            return;
        };
        if let Some(hook) = lock.lock().expect("lease remove hook lock").as_ref() {
            hook(path);
        }
    }

    #[cfg(not(test))]
    fn run_lease_remove_hook(_path: &Path) {}

    #[cfg(test)]
    fn set_lease_open_hook(hook: Option<LeaseHook>) {
        let lock = LEASE_OPEN_HOOK.get_or_init(|| Mutex::new(None));
        *lock.lock().expect("lease open hook lock") = hook;
    }

    #[cfg(test)]
    fn run_lease_open_hook(path: &Path) {
        let Some(lock) = LEASE_OPEN_HOOK.get() else {
            return;
        };
        if let Some(hook) = lock.lock().expect("lease open hook lock").as_ref() {
            hook(path);
        }
    }

    #[cfg(not(test))]
    fn run_lease_open_hook(_path: &Path) {}
}

impl Capability for FsCheckpointStore {
    fn name(&self) -> &'static str {
        "checkpoint_store.fs"
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CheckpointStore for FsCheckpointStore {
    async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
        let handle = CheckpointHandle {
            checkpoint_id: record.checkpoint_id.clone(),
            flow_id: record.flow_id.clone(),
            run_id: record.run_id.clone(),
        };
        let run_dir = self.run_dir(&record.flow_id, &record.run_id);
        fs::create_dir_all(&run_dir).map_err(Self::map_io_error)?;
        let path = self.checkpoint_path_from_record(&record);
        let file = File::create(&path).map_err(Self::map_io_error)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &record)
            .map_err(|err| CheckpointError::Storage(err.to_string()))?;
        Ok(handle)
    }

    async fn get(&self, handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError> {
        let path = self.checkpoint_path(handle);
        let file = File::open(&path).map_err(Self::map_io_error)?;
        let reader = BufReader::new(file);
        serde_json::from_reader(reader).map_err(|err| CheckpointError::Storage(err.to_string()))
    }

    async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
        let path = self.checkpoint_path(handle);
        fs::remove_file(&path).map_err(Self::map_io_error)
    }

    async fn lease(&self, handle: &CheckpointHandle, ttl: Duration) -> Result<Lease, CheckpointError> {
        let path = self.checkpoint_path(handle);
        let metadata = fs::metadata(&path).map_err(Self::map_io_error)?;
        if !metadata.is_file() {
            return Err(CheckpointError::NotFound);
        }
        let lease_path = self.lease_path(handle);
        let existing: Option<LeaseRecord> = {
            Self::run_lease_open_hook(&lease_path);
            match File::open(&lease_path) {
                Ok(file) => {
                    let reader = BufReader::new(file);
                    Some(
                        serde_json::from_reader(reader)
                            .map_err(|err| CheckpointError::Storage(err.to_string()))?,
                    )
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => return Err(Self::map_io_error(err)),
            }
        };
        if let Some(existing) = existing {
            let now_ms = Self::now_ms();
            if existing.expires_at_ms > now_ms {
                return Err(CheckpointError::LeaseConflict);
            }
            Self::run_lease_remove_hook(&lease_path);
            match fs::remove_file(&lease_path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(Self::map_io_error(err)),
            }
        }
        let now_ms = Self::now_ms();
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        let lease_record = LeaseRecord {
            checkpoint_id: handle.checkpoint_id.clone(),
            flow_id: handle.flow_id.clone(),
            run_id: handle.run_id.clone(),
            expires_at_ms,
        };
        let lease_dir = self.lease_dir(handle);
        fs::create_dir_all(&lease_dir).map_err(Self::map_io_error)?;
        Self::run_lease_create_hook(&lease_path);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease_path)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    CheckpointError::LeaseConflict
                } else {
                    Self::map_io_error(err)
                }
            })?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &lease_record)
            .map_err(|err| CheckpointError::Storage(err.to_string()))?;
        Ok(Lease {
            lease_id: lease_path.to_string_lossy().to_string(),
            expires_at_ms,
        })
    }

    async fn release_lease(&self, lease: Lease) -> Result<(), CheckpointError> {
        let path = PathBuf::from(lease.lease_id);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Self::map_io_error(err)),
        }
    }

    async fn list(&self, filter: CheckpointFilter) -> Result<Vec<CheckpointHandle>, CheckpointError> {
        if let Some(status) = filter.status {
            if status == CheckpointStatus::Completed {
                return Ok(Vec::new());
            }
        }

        if !self.root.exists() {
            return Ok(Vec::new());
        }

        let mut handles = Vec::new();
        let flow_dirs: Vec<(FlowId, PathBuf)> = if let Some(flow_id) = filter.flow_id.clone() {
            vec![(flow_id.clone(), self.flow_dir(&flow_id))]
        } else {
            let mut dirs = Vec::new();
            for entry in fs::read_dir(&self.root).map_err(Self::map_io_error)? {
                let entry = entry.map_err(Self::map_io_error)?;
                if !entry.file_type().map_err(Self::map_io_error)?.is_dir() {
                    continue;
                }
                let Some(flow_id) = entry.file_name().to_str().map(|name| FlowId(name.to_string()))
                else {
                    continue;
                };
                dirs.push((flow_id, entry.path()));
            }
            dirs
        };

        let now_ms = Self::now_ms();

        for (flow_id, flow_dir) in flow_dirs {
            if !flow_dir.is_dir() {
                continue;
            }

            let run_dirs: Vec<(String, PathBuf)> = if let Some(run_id) = filter.run_id.clone() {
                vec![(run_id.clone(), flow_dir.join(&run_id))]
            } else {
                let mut runs = Vec::new();
                for entry in fs::read_dir(&flow_dir).map_err(Self::map_io_error)? {
                    let entry = entry.map_err(Self::map_io_error)?;
                    if !entry.file_type().map_err(Self::map_io_error)?.is_dir() {
                        continue;
                    }
                    let Some(run_id) = entry.file_name().to_str().map(|name| name.to_string()) else {
                        continue;
                    };
                    runs.push((run_id, entry.path()));
                }
                runs
            };

            for (run_id, run_dir) in run_dirs {
                if !run_dir.is_dir() {
                    continue;
                }

                for entry in fs::read_dir(&run_dir).map_err(Self::map_io_error)? {
                    let entry = entry.map_err(Self::map_io_error)?;
                    if !entry.file_type().map_err(Self::map_io_error)?.is_file() {
                        continue;
                    }
                    let path = entry.path();
                    if path.extension() != Some(OsStr::new("json")) {
                        continue;
                    }
                    let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                        continue;
                    };
                    let handle = CheckpointHandle {
                        checkpoint_id: stem.to_string(),
                        flow_id: flow_id.clone(),
                        run_id: run_id.clone(),
                    };
                    if let Some(status) = filter.status {
                        if status == CheckpointStatus::Active || status == CheckpointStatus::Expired {
                            let file = File::open(&path).map_err(Self::map_io_error)?;
                            let reader = BufReader::new(file);
                            let record: CheckpointRecord = serde_json::from_reader(reader)
                                .map_err(|err| CheckpointError::Storage(err.to_string()))?;
                            let expired = Self::is_expired(&record, now_ms);
                            if status == CheckpointStatus::Active && expired {
                                continue;
                            }
                            if status == CheckpointStatus::Expired && !expired {
                                continue;
                            }
                        }
                    }
                    handles.push(handle);
                }
            }
        }

        handles.sort_by(|left, right| {
            (
                left.flow_id.as_str(),
                left.run_id.as_str(),
                left.checkpoint_id.as_str(),
            )
                .cmp(&(
                    right.flow_id.as_str(),
                    right.run_id.as_str(),
                    right.checkpoint_id.as_str(),
                ))
        });

        Ok(handles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use capabilities::durability::{
        CheckpointError, CheckpointRecord, FlowFrontier, FrontierEntry, IdempotencyState,
        SerializedState,
    };
    use dag_core::FlowId;
    use serde_json::json;
    use std::sync::{Mutex, OnceLock};

    static LEASE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[allow(dead_code)]
    struct LeaseTestLock(std::sync::MutexGuard<'static, ()>);

    impl LeaseTestLock {
        fn acquire() -> Self {
            let lock = LEASE_TEST_LOCK.get_or_init(|| Mutex::new(()));
            Self(lock.lock().expect("lease test lock"))
        }
    }

    struct LeaseHookGuard;

    impl LeaseHookGuard {
        fn set_create(hook: LeaseHook) -> Self {
            FsCheckpointStore::set_lease_create_hook(Some(hook));
            Self
        }

        fn set_remove(hook: LeaseHook) -> Self {
            FsCheckpointStore::set_lease_remove_hook(Some(hook));
            Self
        }

        fn set_open(hook: LeaseHook) -> Self {
            FsCheckpointStore::set_lease_open_hook(Some(hook));
            Self
        }
    }

    impl Drop for LeaseHookGuard {
        fn drop(&mut self) {
            FsCheckpointStore::set_lease_create_hook(None);
            FsCheckpointStore::set_lease_remove_hook(None);
            FsCheckpointStore::set_lease_open_hook(None);
        }
    }

    #[test]
    fn test_fs_checkpoint_store_put_get_ack_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let expected_path = dir
            .path()
            .join(".flow")
            .join("checkpoints")
            .join("flow_a")
            .join("run_1")
            .join("ckpt_1.json");
        let handle = futures::executor::block_on(store.put(record.clone())).unwrap();
        let path = store.checkpoint_path_from_record(&record);
        assert_eq!(path, expected_path);
        assert!(path.exists());
        let blobs_dir = store.run_dir(&record.flow_id, &record.run_id).join("blobs");
        assert!(!blobs_dir.exists());
        let loaded = futures::executor::block_on(store.get(&handle)).unwrap();
        assert_eq!(loaded.checkpoint_id, record.checkpoint_id);
        futures::executor::block_on(store.ack(&handle)).unwrap();
        let err = futures::executor::block_on(store.get(&handle)).unwrap_err();
        assert!(matches!(err, CheckpointError::NotFound));
    }

    #[test]
    fn test_fs_checkpoint_store_lease_not_found() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let handle = CheckpointHandle {
            checkpoint_id: "missing".to_string(),
            flow_id: FlowId("flow_a".to_string()),
            run_id: "run_1".to_string(),
        };
        let err = futures::executor::block_on(store.lease(&handle, Duration::from_secs(5))).unwrap_err();
        assert!(matches!(err, CheckpointError::NotFound));
    }

    #[test]
    fn test_fs_checkpoint_store_lease_clamps_ttl() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let handle = futures::executor::block_on(store.put(record)).unwrap();

        let ttl = Duration::new(18_446_744_073_709_551, 621_000_000);
        let lease = futures::executor::block_on(store.lease(&handle, ttl)).unwrap();
        assert_eq!(lease.expires_at_ms, u64::MAX);
    }

    #[test]
    fn test_fs_checkpoint_store_lease_conflict_when_unexpired() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let handle = futures::executor::block_on(store.put(record)).unwrap();

        let lease = futures::executor::block_on(store.lease(&handle, Duration::from_secs(30))).unwrap();
        let lease_path = PathBuf::from(&lease.lease_id);
        assert!(lease_path.exists());

        let err = futures::executor::block_on(store.lease(&handle, Duration::from_secs(30))).unwrap_err();
        assert!(matches!(err, CheckpointError::LeaseConflict));
    }

    #[test]
    fn test_fs_checkpoint_store_lease_allows_overwrite_when_expired() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let handle = futures::executor::block_on(store.put(record)).unwrap();

        let lease_path = store.lease_path(&handle);
        let expired_at_ms = FsCheckpointStore::now_ms().saturating_sub(1);
        write_lease_record(&lease_path, "ckpt_1", "flow_a", "run_1", expired_at_ms);

        let lease = futures::executor::block_on(store.lease(&handle, Duration::from_secs(30))).unwrap();
        assert!(lease.expires_at_ms > expired_at_ms);
        let lease_path = PathBuf::from(&lease.lease_id);
        assert!(lease_path.exists());
    }

    #[test]
    fn test_fs_checkpoint_store_lease_atomic_create_blocks_double_grant() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let handle = futures::executor::block_on(store.put(record)).unwrap();
        let _guard = LeaseHookGuard::set_create(Box::new(|path| {
            let expires_at_ms = FsCheckpointStore::now_ms().saturating_add(30_000);
            let lease_record = LeaseRecord {
                checkpoint_id: "ckpt_1".to_string(),
                flow_id: FlowId("flow_a".to_string()),
                run_id: "run_1".to_string(),
                expires_at_ms,
            };
            fs::create_dir_all(path.parent().expect("lease parent")).unwrap();
            let file = File::create(path).unwrap();
            let writer = BufWriter::new(file);
            serde_json::to_writer(writer, &lease_record).unwrap();
        }));

        let err = futures::executor::block_on(store.lease(&handle, Duration::from_secs(30))).unwrap_err();
        assert!(matches!(err, CheckpointError::LeaseConflict));
    }

    #[test]
    fn test_fs_checkpoint_store_lease_ignores_remove_not_found_race() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let handle = futures::executor::block_on(store.put(record)).unwrap();

        let lease_path = store.lease_path(&handle);
        let expired_at_ms = FsCheckpointStore::now_ms().saturating_sub(1);
        write_lease_record(&lease_path, "ckpt_1", "flow_a", "run_1", expired_at_ms);

        let lease_path = PathBuf::from(lease_path);
        let _guard = LeaseHookGuard::set_remove(Box::new(move |path| {
            if path == lease_path {
                let _ = fs::remove_file(path);
            }
        }));

        let lease = futures::executor::block_on(store.lease(&handle, Duration::from_secs(30))).unwrap();
        assert!(PathBuf::from(&lease.lease_id).exists());
    }

    #[test]
    fn test_fs_checkpoint_store_lease_open_not_found_race_allows_create() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let handle = futures::executor::block_on(store.put(record)).unwrap();

        let lease_path = store.lease_path(&handle);
        let expires_at_ms = FsCheckpointStore::now_ms().saturating_add(30_000);
        write_lease_record(&lease_path, "ckpt_1", "flow_a", "run_1", expires_at_ms);

        let lease_path_for_hook = lease_path.clone();
        let _guard = LeaseHookGuard::set_open(Box::new(move |path| {
            if path == lease_path_for_hook {
                let _ = fs::remove_file(path);
            }
        }));

        let lease = futures::executor::block_on(store.lease(&handle, Duration::from_secs(30))).unwrap();
        assert!(PathBuf::from(&lease.lease_id).exists());
    }

    #[test]
    fn test_fs_checkpoint_store_release_lease_removes_file() {
        let _lock = LeaseTestLock::acquire();
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
        let handle = futures::executor::block_on(store.put(record)).unwrap();

        let lease = futures::executor::block_on(store.lease(&handle, Duration::from_secs(30))).unwrap();
        let lease_path = PathBuf::from(&lease.lease_id);
        assert!(lease_path.exists());

        futures::executor::block_on(store.release_lease(lease)).unwrap();
        assert!(!lease_path.exists());
    }

    #[test]
    fn test_fs_checkpoint_store_list_filters_and_ignores_non_json() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let record_a1 = sample_checkpoint_record("flow_a", "run_1", "ckpt_2");
        let record_a2 = sample_checkpoint_record("flow_a", "run_2", "ckpt_1");
        let record_b1 = sample_checkpoint_record("flow_b", "run_1", "ckpt_1");
        futures::executor::block_on(store.put(record_a1)).unwrap();
        futures::executor::block_on(store.put(record_a2)).unwrap();
        futures::executor::block_on(store.put(record_b1)).unwrap();

        let run_dir = store.run_dir(&FlowId("flow_a".to_string()), "run_1");
        fs::create_dir_all(&run_dir).unwrap();
        File::create(run_dir.join("notes.txt")).unwrap();
        fs::create_dir_all(run_dir.join("blobs")).unwrap();

        let handles = futures::executor::block_on(store.list(CheckpointFilter::default())).unwrap();
        let ordered = handles
            .iter()
            .map(|handle| (handle.flow_id.as_str().to_string(), handle.run_id.clone(), handle.checkpoint_id.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec![
                ("flow_a".to_string(), "run_1".to_string(), "ckpt_2".to_string()),
                ("flow_a".to_string(), "run_2".to_string(), "ckpt_1".to_string()),
                ("flow_b".to_string(), "run_1".to_string(), "ckpt_1".to_string()),
            ]
        );

        let filtered = futures::executor::block_on(store.list(CheckpointFilter {
            flow_id: Some(FlowId("flow_a".to_string())),
            run_id: Some("run_1".to_string()),
            status: Some(CheckpointStatus::Active),
        }))
        .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].checkpoint_id, "ckpt_2");

        let completed = futures::executor::block_on(store.list(CheckpointFilter {
            flow_id: None,
            run_id: None,
            status: Some(CheckpointStatus::Completed),
        }))
        .unwrap();
        assert!(
            completed.is_empty(),
            "completed list returns empty because ack deletes records"
        );
    }

    #[test]
    fn test_fs_checkpoint_store_list_filters_active_vs_expired() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsCheckpointStore::with_root(dir.path().join(".flow").join("checkpoints"));
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let now_ms = u64::try_from(now_ms).unwrap_or(u64::MAX);

        let active = sample_checkpoint_record_with_ttl("flow_a", "run_1", "ckpt_active", now_ms - 1_000, 10_000);
        let expired = sample_checkpoint_record_with_ttl("flow_a", "run_1", "ckpt_expired", now_ms - 10_000, 1_000);
        futures::executor::block_on(store.put(active)).unwrap();
        futures::executor::block_on(store.put(expired)).unwrap();

        let active_handles = futures::executor::block_on(store.list(CheckpointFilter {
            flow_id: Some(FlowId("flow_a".to_string())),
            run_id: Some("run_1".to_string()),
            status: Some(CheckpointStatus::Active),
        }))
        .unwrap();
        assert_eq!(active_handles.len(), 1);
        assert_eq!(active_handles[0].checkpoint_id, "ckpt_active");

        let expired_handles = futures::executor::block_on(store.list(CheckpointFilter {
            flow_id: Some(FlowId("flow_a".to_string())),
            run_id: Some("run_1".to_string()),
            status: Some(CheckpointStatus::Expired),
        }))
        .unwrap();
        assert_eq!(expired_handles.len(), 1);
        assert_eq!(expired_handles[0].checkpoint_id, "ckpt_expired");
    }

    fn sample_checkpoint_record(flow_id: &str, run_id: &str, checkpoint_id: &str) -> CheckpointRecord {
        CheckpointRecord {
            checkpoint_id: checkpoint_id.to_string(),
            flow_id: FlowId(flow_id.to_string()),
            flow_version: "0.1.0".to_string(),
            run_id: run_id.to_string(),
            parent_run_id: None,
            frontier: FlowFrontier {
                completed: vec![FrontierEntry {
                    node_alias: "node_a".to_string(),
                    output_port: "out".to_string(),
                    cursor: None,
                }],
                pending: vec!["node_b".to_string()],
            },
            state: SerializedState {
                data: json!({"value": 42}),
                blobs: Vec::new(),
            },
            idempotency: IdempotencyState::default(),
            created_at_ms: 0,
            resume_after_ms: None,
            ttl_ms: None,
            version: 1,
        }
    }

    fn sample_checkpoint_record_with_ttl(
        flow_id: &str,
        run_id: &str,
        checkpoint_id: &str,
        created_at_ms: u64,
        ttl_ms: u64,
    ) -> CheckpointRecord {
        let mut record = sample_checkpoint_record(flow_id, run_id, checkpoint_id);
        record.created_at_ms = created_at_ms;
        record.ttl_ms = Some(ttl_ms);
        record
    }

    fn write_lease_record(
        path: &Path,
        checkpoint_id: &str,
        flow_id: &str,
        run_id: &str,
        expires_at_ms: u64,
    ) {
        fs::create_dir_all(path.parent().expect("lease parent")).unwrap();
        let lease_record = LeaseRecord {
            checkpoint_id: checkpoint_id.to_string(),
            flow_id: FlowId(flow_id.to_string()),
            run_id: run_id.to_string(),
            expires_at_ms,
        };
        let file = File::create(path).unwrap();
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &lease_record).unwrap();
    }
}
