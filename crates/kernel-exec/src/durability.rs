use std::time::Duration;

use serde_json::Value as JsonValue;
use thiserror::Error;

use capabilities::blob::BlobError;
use capabilities::durability::{
    CheckpointBlobStore, CheckpointError, CheckpointHandle, CheckpointRecord, CheckpointStore,
    SerializedState,
};

use crate::ExecutionError;

#[derive(Debug, Error)]
pub enum CheckpointCodecError {
    #[error("checkpoint codec error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub fn serialize_checkpoint(record: &CheckpointRecord) -> Result<Vec<u8>, CheckpointCodecError> {
    serde_json::to_vec(record).map_err(CheckpointCodecError::Serde)
}

pub fn deserialize_checkpoint(bytes: &[u8]) -> Result<CheckpointRecord, CheckpointCodecError> {
    serde_json::from_slice(bytes).map_err(CheckpointCodecError::Serde)
}

pub async fn serialize_state(
    checkpoint_id: &str,
    field: &str,
    state: &JsonValue,
    threshold_bytes: Option<u64>,
    blob_store: Option<&dyn CheckpointBlobStore>,
) -> Result<SerializedState, CheckpointError> {
    let serialized = serde_json::to_vec(state).map_err(|err| {
        CheckpointError::Storage(format!("failed to serialize state: {err}"))
    })?;

    let should_spill = threshold_bytes
        .map(|limit| serialized.len() as u64 >= limit)
        .unwrap_or(false);

    if !should_spill {
        return Ok(SerializedState {
            data: state.clone(),
            blobs: Vec::new(),
        });
    }

    let store = blob_store.ok_or_else(|| {
        CheckpointError::Storage("checkpoint blob store unavailable".to_string())
    })?;

    let blob = store
        .put_checkpoint_blob(checkpoint_id, field, &serialized)
        .await
        .map_err(|err| map_blob_error(err))?;

    Ok(SerializedState {
        data: JsonValue::Null,
        blobs: vec![blob],
    })
}

pub async fn rehydrate_state(
    serialized: &SerializedState,
    blob_store: Option<&dyn CheckpointBlobStore>,
) -> Result<JsonValue, CheckpointError> {
    if serialized.blobs.is_empty() {
        return Ok(serialized.data.clone());
    }

    let store = blob_store.ok_or_else(|| {
        CheckpointError::Storage("checkpoint blob store unavailable".to_string())
    })?;

    let blob = serialized
        .blobs
        .first()
        .ok_or_else(|| CheckpointError::Storage("missing blob reference".to_string()))?;

    let bytes = store
        .get(&blob.ref_id)
        .await
        .map_err(|err| map_blob_error(err))?
        .ok_or_else(|| CheckpointError::NotFound)?;

    serde_json::from_slice(&bytes)
        .map_err(|err| CheckpointError::Storage(format!("invalid checkpoint blob: {err}")))
}

pub async fn acquire_lease(
    store: &dyn CheckpointStore,
    handle: &CheckpointHandle,
    ttl: Duration,
) -> Result<(), ExecutionError> {
    store
        .lease(handle, ttl)
        .await
        .map(|_| ())
        .map_err(|err| map_checkpoint_error(handle, err))
}

pub async fn renew_lease(
    store: &dyn CheckpointStore,
    handle: &CheckpointHandle,
    ttl: Duration,
) -> Result<(), ExecutionError> {
    store
        .lease(handle, ttl)
        .await
        .map(|_| ())
        .map_err(|err| map_checkpoint_error(handle, err))
}

fn map_checkpoint_error(handle: &CheckpointHandle, err: CheckpointError) -> ExecutionError {
    match err {
        CheckpointError::NotFound => ExecutionError::CheckpointNotFound {
            checkpoint_id: handle.checkpoint_id.clone(),
        },
        CheckpointError::LeaseConflict | CheckpointError::LeaseExpired => {
            ExecutionError::CheckpointLeaseConflict {
                checkpoint_id: handle.checkpoint_id.clone(),
            }
        }
        CheckpointError::Storage(message) => ExecutionError::CheckpointStateCorrupted {
            checkpoint_id: handle.checkpoint_id.clone(),
            message,
        },
    }
}

fn map_blob_error(err: BlobError) -> CheckpointError {
    match err {
        BlobError::NotFound => CheckpointError::NotFound,
        BlobError::Other(message) => CheckpointError::Storage(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use capabilities::durability::{
        CheckpointHandle, CheckpointRecord, CheckpointStore, FlowFrontier, FrontierEntry,
        IdempotencyState, SerializedState,
    };
    use dag_core::FlowId;

    struct ConflictStore;

    impl capabilities::Capability for ConflictStore {
        fn name(&self) -> &'static str {
            "checkpoint.conflict"
        }
    }

    #[async_trait::async_trait]
    impl CheckpointStore for ConflictStore {
        async fn put(&self, _record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
            Err(CheckpointError::Storage("not implemented".to_string()))
        }

        async fn get(&self, _handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError> {
            Err(CheckpointError::Storage("not implemented".to_string()))
        }

        async fn ack(&self, _handle: &CheckpointHandle) -> Result<(), CheckpointError> {
            Err(CheckpointError::Storage("not implemented".to_string()))
        }

        async fn lease(&self, _handle: &CheckpointHandle, _ttl: Duration) -> Result<capabilities::durability::Lease, CheckpointError> {
            Err(CheckpointError::LeaseConflict)
        }

        async fn release_lease(&self, _lease: capabilities::durability::Lease) -> Result<(), CheckpointError> {
            Err(CheckpointError::Storage("not implemented".to_string()))
        }

        async fn list(&self, _filter: capabilities::durability::CheckpointFilter) -> Result<Vec<CheckpointHandle>, CheckpointError> {
            Err(CheckpointError::Storage("not implemented".to_string()))
        }
    }

    #[test]
    fn checkpoint_round_trip_restores_frontier_and_state() {
        let flow_id = FlowId("flow".to_string());
        let frontier = FlowFrontier {
            completed: vec![FrontierEntry {
                node_alias: "alpha".to_string(),
                output_port: "out".to_string(),
                cursor: None,
            }],
            pending: vec!["beta".to_string()],
        };
        let mut consumed = HashMap::new();
        consumed.insert("alpha".to_string(), vec!["token-a".to_string()]);
        let mut pending = HashMap::new();
        pending.insert("beta".to_string(), "token-b".to_string());
        let idempotency = IdempotencyState { consumed, pending };
        let record = CheckpointRecord {
            checkpoint_id: "ckpt-1".to_string(),
            flow_id,
            flow_version: "1.0.0".to_string(),
            run_id: "run-1".to_string(),
            parent_run_id: None,
            frontier: frontier.clone(),
            state: SerializedState {
                data: serde_json::json!({"value": 42}),
                blobs: Vec::new(),
            },
            idempotency,
            created_at_ms: 1,
            resume_after_ms: None,
            ttl_ms: None,
            version: 1,
        };

        let bytes = serialize_checkpoint(&record).expect("serialize");
        let decoded = deserialize_checkpoint(&bytes).expect("deserialize");

        assert_eq!(decoded.frontier, frontier);
        assert_eq!(decoded.state.data, serde_json::json!({"value": 42}));
        assert_eq!(decoded.state.blobs.len(), 0);
        assert_eq!(decoded.idempotency, record.idempotency);
    }

    #[tokio::test]
    async fn lease_conflict_yields_checkpoint_code() {
        let flow_id = FlowId("flow".to_string());
        let handle = CheckpointHandle {
            checkpoint_id: "ckpt-lease".to_string(),
            flow_id,
            run_id: "run-1".to_string(),
        };
        let store = ConflictStore;

        let err = acquire_lease(&store, &handle, Duration::from_secs(30))
            .await
            .expect_err("expected lease conflict");

        assert!(matches!(
            err,
            ExecutionError::CheckpointLeaseConflict { ref checkpoint_id } if checkpoint_id == "ckpt-lease"
        ));
        assert_eq!(err.diagnostic_code(), Some("DAG-CKPT-007"));
    }
}
