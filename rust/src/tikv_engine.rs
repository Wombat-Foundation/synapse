use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rezzy::hamt::{HamtNode, StructuralHash};
use sha2::{Digest, Sha256};
use tikv_client::{RawClient, TransactionClient};
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout, Duration};

use crate::state_hamt::{decode_persisted_node, materialize_from_node_map};

static RUNTIME: OnceCell<Runtime> = OnceCell::new();
static CLIENT: OnceCell<RawClient> = OnceCell::new();
static TX_CLIENT: OnceCell<TransactionClient> = OnceCell::new();
const READINESS_PROBE_VALUE: &[u8] = b"ok";
static READINESS_PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const OPEN_CLIENT_ATTEMPTS: u32 = 5;
const OPEN_CLIENT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const OPEN_CLIENT_RETRY_DELAY: Duration = Duration::from_secs(1);
const TRANSACTION_WRITE_ATTEMPTS: u32 = 5;
const TRANSACTION_WRITE_RETRY_DELAY: Duration = Duration::from_millis(10);

fn is_retryable_key_error(k: &tikv_client::ProtoKeyError) -> bool {
    k.conflict.is_some() || !k.retryable.is_empty() || k.locked.is_some()
}

fn is_retryable_region_error(r: &tikv_client::ProtoRegionError) -> bool {
    r.not_leader.is_some()
        || r.epoch_not_match.is_some()
        || r.server_is_busy.is_some()
        || r.stale_command.is_some()
        || r.region_not_found.is_some()
}

fn is_retryable_txn_error(err: &tikv_client::Error) -> bool {
    match err {
        tikv_client::Error::KeyError(k) => is_retryable_key_error(k),
        tikv_client::Error::RegionError(r) => is_retryable_region_error(r),
        tikv_client::Error::ResolveLockError(_) => true,
        tikv_client::Error::PessimisticLockError { inner, .. } => is_retryable_txn_error(inner),
        // Transaction commits can aggregate per-key errors. Retry only when
        // every member is retryable, rather than turning a fatal error into a
        // delayed retry loop.
        tikv_client::Error::MultipleKeyErrors(errors) => {
            !errors.is_empty() && errors.iter().all(is_retryable_txn_error)
        }
        // Replaying this operation is safe: it only writes immutable,
        // content-addressed HAMT nodes and the same state-group root value.
        tikv_client::Error::UndeterminedError(_) => true,
        tikv_client::Error::LeaderNotFound { .. }
        | tikv_client::Error::RegionNotFoundInResponse { .. }
        | tikv_client::Error::EntryNotFoundInRegionCache => true,
        _ => false,
    }
}

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime")
    })
}

async fn check_raw_kv_ready(client: &RawClient) -> Result<(), String> {
    let probe_key = format!(
        "synapse:tikv:readiness-probe:{}:{}",
        std::process::id(),
        READINESS_PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
    .into_bytes();
    client
        .put(probe_key.clone(), READINESS_PROBE_VALUE.to_vec())
        .await
        .map_err(|e| e.to_string())?;

    let probe_result = async {
        let value = client
            .get(probe_key.clone())
            .await
            .map_err(|e| e.to_string())?;
        if value.as_deref() != Some(READINESS_PROBE_VALUE) {
            return Err("TiKV readiness probe returned an unexpected value".to_owned());
        }
        Ok(())
    }
    .await;

    // A successful write must be cleaned up even if the read failed. The
    // readiness key is process-unique, so this can never delete another
    // process's probe.
    let delete_result = client.delete(probe_key).await.map_err(|e| e.to_string());
    probe_result.and(delete_result)
}

async fn open_ready_client(pd_endpoints: Vec<String>) -> Result<RawClient, String> {
    let mut last_error = String::new();

    for attempt in 1..=OPEN_CLIENT_ATTEMPTS {
        match timeout(OPEN_CLIENT_ATTEMPT_TIMEOUT, async {
            let client = RawClient::new(pd_endpoints.clone())
                .await
                .map_err(|e| e.to_string())?;
            check_raw_kv_ready(&client).await?;
            Ok::<RawClient, String>(client)
        })
        .await
        {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(e)) => last_error = e,
            Err(_) => {
                last_error = format!(
                    "timed out after {} seconds",
                    OPEN_CLIENT_ATTEMPT_TIMEOUT.as_secs()
                );
            }
        }

        if attempt < OPEN_CLIENT_ATTEMPTS {
            sleep(OPEN_CLIENT_RETRY_DELAY).await;
        }
    }

    Err(format!(
        "TiKV cluster did not become ready for raw KV operations after {} attempts: {}",
        OPEN_CLIENT_ATTEMPTS, last_error
    ))
}

#[pyfunction]
pub fn open_client(py: Python<'_>, pd_endpoints: Vec<String>) -> PyResult<()> {
    if CLIENT.get().is_some() && TX_CLIENT.get().is_some() {
        return Ok(());
    }
    if CLIENT.get().is_some() || TX_CLIENT.get().is_some() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is only partially initialized",
        ));
    }
    let rt = get_runtime();
    let client = py
        .detach(|| rt.block_on(async { open_ready_client(pd_endpoints.clone()).await }))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    let tx_client = py
        .detach(|| {
            get_runtime()
                .block_on(TransactionClient::new(pd_endpoints))
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    // Publish both clients only after both connections have been established;
    // a failed transaction-client connection must not leave open_client()
    // appearing successful on subsequent calls.
    TX_CLIENT.set(tx_client).map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err("Failed to set TiKV transaction client")
    })?;
    CLIENT.set(client).map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err("Failed to set TiKV Client instance")
    })?;
    Ok(())
}

#[pyfunction]
pub fn put(py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.put(key, value).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

#[pyfunction]
pub fn get(py: Python<'_>, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    let val = py
        .detach(|| {
            rt.block_on(async { client.get(key).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(val)
}

#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    let pairs = py
        .detach(|| {
            rt.block_on(async { client.batch_get(keys).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    let results: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .into_iter()
        .map(|pair| {
            let (k, v): (tikv_client::Key, tikv_client::Value) = pair.into();
            (k.into(), v)
        })
        .collect();
    Ok(results)
}

#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.batch_put(pairs).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

/// Atomically publish a set of immutable HAMT nodes and its root/index
/// record. RawClient::batch_put is not transactional; using it for a tree
/// plus its root can expose a root whose children are not committed yet.
#[pyfunction]
pub fn transactional_batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    let client = TX_CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV transaction client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async {
            let mut last_error: Option<tikv_client::Error> = None;

            for attempt in 1..=TRANSACTION_WRITE_ATTEMPTS {
                let result: Result<(), tikv_client::Error> = async {
                    let mut txn = client.begin_optimistic().await?;
                    for (key, value) in &pairs {
                        txn.put(key.clone(), value.clone()).await?;
                    }
                    txn.commit().await?;
                    Ok(())
                }
                .await;

                match result {
                    Ok(_) => return Ok(()),
                    Err(error) => {
                        if !is_retryable_txn_error(&error) {
                            return Err(error.to_string());
                        }
                        last_error = Some(error);
                    }
                }

                if attempt < TRANSACTION_WRITE_ATTEMPTS {
                    sleep(TRANSACTION_WRITE_RETRY_DELAY * attempt).await;
                }
            }

            Err(last_error
                .expect("transaction write loop always records an error")
                .to_string())
        })
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

#[pyfunction]
pub fn delete(py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.delete(key).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.batch_delete(keys).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

/// Computes the exclusive upper bound of a byte-string prefix range: `prefix`
/// incremented as a big-endian number. Strips any trailing 0xFF bytes (they
/// can't be incremented in place) and increments the first byte, from the
/// end, that isn't 0xFF -- e.g. `[0x01, 0xFF] -> [0x02]`, not `[0x01]` (which
/// would sort *before* `prefix`, producing an inverted, empty scan range).
///
/// Returns an empty `Vec` if every byte in `prefix` is 0xFF (or `prefix` is
/// itself empty): there is no finite successor in that case, so the caller
/// must scan with no upper bound and filter results by prefix explicitly.
fn prefix_scan_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    loop {
        match end.pop() {
            None => break,
            Some(0xFF) => continue,
            Some(b) => {
                end.push(b + 1);
                break;
            }
        }
    }
    end
}

#[pyfunction]
pub fn scan_prefix(
    py: Python<'_>,
    prefix: Vec<u8>,
    limit: u32,
) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();

    let start = prefix.clone();
    let end = prefix_scan_upper_bound(&prefix);

    let pairs = if end.is_empty() {
        py.detach(|| {
            rt.block_on(async { client.scan(start.., limit).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?
    } else {
        py.detach(|| {
            rt.block_on(async { client.scan(start..end, limit).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?
    };

    let results: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .into_iter()
        .map(|pair| {
            let (k, v): (tikv_client::Key, tikv_client::Value) = pair.into();
            let key_bytes: Vec<u8> = k.into();
            (key_bytes, v)
        })
        // Only matters for the unbounded (all-0xFF-prefix) branch above --
        // an incremented finite end bound already scopes the range exactly.
        .filter(|(k, _): &(Vec<u8>, Vec<u8>)| k.starts_with(&prefix))
        .collect();
    Ok(results)
}

/// Batch size for TiKV `batch_get` calls while walking the HAMT. Matches the
/// batching Python previously did in `_get_state_groups_from_hamt_txn`.
const NODE_FETCH_BATCH_SIZE: usize = 100;

/// Fixed width of the room-scoped TiKV key prefix. See `room_tikv_prefix_raw`
/// for why 8 bytes is enough: it's a locality hint, not an identity -- a
/// prefix collision between two rooms just means their nodes interleave in
/// the same key range, not a correctness issue (the full key is always
/// `prefix || structural_hash`, which stays unique regardless). At 8 bytes,
/// the birthday bound puts a 50% chance of *any* collision existing at
/// around 4 billion rooms -- far beyond any realistic homeserver.
const ROOM_PREFIX_LEN: usize = 8;

/// Namespaced, room-prefixed HAMT node key:
/// `hamt:node:<namespace_hash>:<room_prefix_hex>:<structural_hash_hex>`.
/// The room prefix gives nodes belonging to the same room contiguous byte
/// ranges in TiKV's sorted keyspace, for locality -- see `room_tikv_prefix_raw`.
fn node_tikv_key(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    structural_hash: &StructuralHash,
) -> Vec<u8> {
    let namespace_hash = Sha256::digest(namespace.as_bytes());
    let mut key = Vec::with_capacity(10 + 32 + 1 + ROOM_PREFIX_LEN * 2 + 1 + 32);
    key.extend_from_slice(b"hamt:node:");
    key.extend_from_slice(hex::encode(&namespace_hash[..16]).as_bytes());
    key.push(b':');
    key.extend_from_slice(hex::encode(room_prefix).as_bytes());
    key.push(b':');
    key.extend_from_slice(hex::encode(structural_hash).as_bytes());
    key
}

async fn get_client() -> Result<&'static RawClient, String> {
    CLIENT
        .get()
        .ok_or_else(|| "TiKV client is not open. Call open_client first.".to_owned())
}

/// Fetch a state group's HAMT from TiKV, entirely in Rust: BFS-fetch the
/// reachable nodes (batched), decode each one, and materialize
/// `(event_type, state_key, event_id)` triples -- without crossing back into
/// Python per node.
///
/// The caller supplies the TiKV namespace, room prefix, and root hash. Node
/// keys are namespaced because content-addressing is only safe to share when
/// every deployment uses the same HAMT secret.
async fn materialize_state_hamt_async(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    root_structural_hash: StructuralHash,
) -> Result<Vec<(String, String, String)>, String> {
    let client = get_client().await?;

    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);
    let mut to_fetch: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);

    while !to_fetch.is_empty() {
        let current_batch: Vec<StructuralHash> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let keys: Vec<Vec<u8>> = chunk
                .iter()
                .map(|hash| node_tikv_key(namespace, room_prefix, hash))
                .collect();
            let rows = client.batch_get(keys).await.map_err(|e| e.to_string())?;

            if rows.len() != chunk.len() {
                return Err(format!(
                    "Missing HAMT node(s): expected {}, got {}",
                    chunk.len(),
                    rows.len()
                ));
            }

            for pair in rows {
                let (_, node_bytes): (tikv_client::Key, tikv_client::Value) = pair.into();
                // We fetched by exact key per hash, so re-derive which hash this
                // row belongs to by decoding the node itself rather than trusting
                // key-order (TiKV batch_get does not guarantee response order).
                let node = decode_persisted_node(&node_bytes)?;
                let hash = node.structural_hash;

                for child in &node.children {
                    let child_hash = child.structural_hash();
                    if seen.insert(child_hash) {
                        to_fetch.insert(child_hash);
                    }
                }

                node_map.insert(hash, node);
            }
        }
    }

    materialize_from_node_map(&root_structural_hash, &node_map)
}

/// Materialize several HAMT roots in one TiKV traversal.
///
/// Each BFS layer is fetched across every requested root at once. Nodes are
/// keyed by both their room prefix and structural hash: structural hashes are
/// only meaningful inside the room-scoped TiKV keyspace, while roots for the
/// same room can share fetched subtrees.
async fn materialize_state_hamts_async(
    namespace: &str,
    roots: Vec<([u8; ROOM_PREFIX_LEN], StructuralHash)>,
) -> Result<Vec<Vec<(String, String, String)>>, String> {
    let client = get_client().await?;
    type NodeLocation = ([u8; ROOM_PREFIX_LEN], StructuralHash);

    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = roots.iter().copied().collect();
    let mut to_fetch = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let key_to_location: HashMap<Vec<u8>, NodeLocation> = chunk
                .iter()
                .map(|(room_prefix, hash)| {
                    (
                        node_tikv_key(namespace, room_prefix, hash),
                        (*room_prefix, *hash),
                    )
                })
                .collect();
            let keys: Vec<Vec<u8>> = key_to_location.keys().cloned().collect();
            let rows = client.batch_get(keys).await.map_err(|e| e.to_string())?;

            if rows.len() != chunk.len() {
                return Err(format!(
                    "Missing HAMT node(s): expected {}, got {}",
                    chunk.len(),
                    rows.len()
                ));
            }

            for pair in rows {
                let (key, node_bytes): (tikv_client::Key, tikv_client::Value) = pair.into();
                let key: Vec<u8> = key.into();
                let (room_prefix, expected_hash) = key_to_location
                    .get(&key)
                    .copied()
                    .ok_or_else(|| "TiKV returned an unexpected HAMT node key".to_owned())?;
                let node = decode_persisted_node(&node_bytes)?;
                if node.structural_hash != expected_hash {
                    return Err("HAMT node hash does not match its TiKV key".to_owned());
                }

                for child in &node.children {
                    let child_location = (room_prefix, child.structural_hash());
                    if seen.insert(child_location) {
                        to_fetch.insert(child_location);
                    }
                }

                node_map.insert((room_prefix, expected_hash), node);
            }
        }
    }

    roots
        .into_iter()
        .map(|(room_prefix, root_hash)| {
            let nodes = node_map
                .iter()
                .filter_map(|((node_prefix, hash), node)| {
                    (*node_prefix == room_prefix).then_some((*hash, node.clone()))
                })
                .collect();
            materialize_from_node_map(&root_hash, &nodes)
        })
        .collect()
}

/// Materialize a state group's full state map directly from TiKV, in pure
/// Rust. `room_prefix` and `root_structural_hash` come from the TiKV root
/// record, so this needs no room_id or room-version lookup.
#[pyfunction]
#[pyo3(text_signature = "(namespace, room_prefix, root_structural_hash, /)")]
pub fn materialize_state_hamt(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    root_structural_hash: Vec<u8>,
) -> PyResult<Option<Vec<(String, String, String)>>> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {ROOM_PREFIX_LEN} bytes"
        ))
    })?;
    let root_structural_hash: StructuralHash = root_structural_hash.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 16 bytes")
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(materialize_state_hamt_async(
            &namespace,
            &room_prefix,
            root_structural_hash,
        ))
    })
    .map(Some)
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

/// Materialize several state groups' full state maps directly from TiKV.
///
/// The input and output order is preserved. Callers should use this only for
/// multi-root reads; the single-root function avoids the small setup cost for
/// the common case.
#[pyfunction]
#[pyo3(text_signature = "(namespace, roots, /)")]
pub fn materialize_state_hamts(
    py: Python<'_>,
    namespace: String,
    roots: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<Vec<Vec<(String, String, String)>>> {
    let roots = roots
        .into_iter()
        .map(|(room_prefix, root_hash)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {ROOM_PREFIX_LEN} bytes"
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 16 bytes")
            })?;
            Ok((room_prefix, root_hash))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let rt = get_runtime();
    py.detach(|| rt.block_on(materialize_state_hamts_async(&namespace, roots)))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "tikv_engine")?;
    child_module.add_function(wrap_pyfunction!(open_client, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(transactional_batch_put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(scan_prefix, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_hamt, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_hamts, &child_module)?)?;

    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.tikv_engine", &child_module)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_scan_upper_bound_increments_last_non_ff_byte() {
        assert_eq!(prefix_scan_upper_bound(&[0x01]), vec![0x02]);
        assert_eq!(prefix_scan_upper_bound(&[0x01, 0x02]), vec![0x01, 0x03]);
    }

    #[test]
    fn prefix_scan_upper_bound_carries_past_trailing_ff_bytes() {
        // The bug this guards against: naively popping a single trailing
        // 0xFF byte gives [0x01], which sorts *before* [0x01, 0xFF] --
        // an inverted, empty range. The correct successor carries into
        // the preceding byte instead.
        let end = prefix_scan_upper_bound(&[0x01, 0xFF]);
        assert_eq!(end, vec![0x02]);
        assert!(end.as_slice() > [0x01, 0xFF].as_slice());

        assert_eq!(prefix_scan_upper_bound(&[0x01, 0xFF, 0xFF]), vec![0x02]);
    }

    #[test]
    fn prefix_scan_upper_bound_has_no_successor_for_all_ff_or_empty() {
        assert_eq!(prefix_scan_upper_bound(&[0xFF, 0xFF]), Vec::<u8>::new());
        assert_eq!(prefix_scan_upper_bound(&[]), Vec::<u8>::new());
    }

    #[test]
    fn retryable_key_error_does_not_retry_an_abort() {
        let key_err = tikv_client::ProtoKeyError::default();
        assert!(!is_retryable_key_error(&key_err));

        let retryable_err = tikv_client::ProtoKeyError {
            retryable: "retryable lock".to_string(),
            ..Default::default()
        };
        assert!(is_retryable_key_error(&retryable_err));

        let abort_err = tikv_client::ProtoKeyError {
            abort: "txn aborted".to_string(),
            ..Default::default()
        };
        assert!(!is_retryable_key_error(&abort_err));
    }

    #[test]
    fn retryable_region_error_detects_flags() {
        let region_err = tikv_client::ProtoRegionError::default();
        assert!(!is_retryable_region_error(&region_err));
    }

    #[test]
    fn fatal_errors_fail_fast_without_retrying() {
        assert!(!is_retryable_txn_error(&tikv_client::Error::Unimplemented));
        assert!(!is_retryable_txn_error(
            &tikv_client::Error::UnsupportedMode
        ));
        assert!(!is_retryable_txn_error(
            &tikv_client::Error::ColumnFamilyError("default".to_string())
        ));
        assert!(!is_retryable_txn_error(&tikv_client::Error::StringError(
            "generic string error".to_string()
        )));
    }

    #[test]
    fn wrapped_key_conflicts_are_retryable() {
        let conflict = tikv_client::Error::KeyError(Box::new(tikv_client::ProtoKeyError {
            retryable: "write conflict".to_owned(),
            ..Default::default()
        }));
        assert!(is_retryable_txn_error(
            &tikv_client::Error::MultipleKeyErrors(vec![conflict])
        ));

        assert!(!is_retryable_txn_error(
            &tikv_client::Error::MultipleKeyErrors(vec![tikv_client::Error::Unimplemented])
        ));
    }
}
