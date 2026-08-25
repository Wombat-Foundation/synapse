use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rezzy::hamt::{HamtNode, StructuralHash};
use tikv_client::RawClient;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};

use crate::state_hamt::{decode_persisted_node, materialize_from_node_map};

static RUNTIME: OnceCell<Runtime> = OnceCell::new();
static CLIENT: OnceCell<RawClient> = OnceCell::new();
const READINESS_PROBE_KEY: &[u8] = b"synapse:tikv:readiness-probe";
const READINESS_PROBE_VALUE: &[u8] = b"ok";
const OPEN_CLIENT_ATTEMPTS: u32 = 60;
const OPEN_CLIENT_RETRY_DELAY: Duration = Duration::from_secs(2);

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
    client
        .put(READINESS_PROBE_KEY.to_vec(), READINESS_PROBE_VALUE.to_vec())
        .await
        .map_err(|e| e.to_string())?;

    let value = client
        .get(READINESS_PROBE_KEY.to_vec())
        .await
        .map_err(|e| e.to_string())?;
    if value.as_deref() != Some(READINESS_PROBE_VALUE) {
        return Err("TiKV readiness probe returned an unexpected value".to_owned());
    }

    client
        .delete(READINESS_PROBE_KEY.to_vec())
        .await
        .map_err(|e| e.to_string())
}

async fn open_ready_client(pd_endpoints: Vec<String>) -> Result<RawClient, String> {
    let mut last_error = String::new();

    for attempt in 1..=OPEN_CLIENT_ATTEMPTS {
        match RawClient::new(pd_endpoints.clone()).await {
            Ok(client) => match check_raw_kv_ready(&client).await {
                Ok(()) => return Ok(client),
                Err(e) => last_error = e,
            },
            Err(e) => last_error = e.to_string(),
        }

        if attempt < OPEN_CLIENT_ATTEMPTS {
            sleep(OPEN_CLIENT_RETRY_DELAY).await;
        }
    }

    Err(format!(
        "TiKV cluster is reachable but not ready for raw KV operations after {} attempts: {}",
        OPEN_CLIENT_ATTEMPTS, last_error
    ))
}

#[pyfunction]
pub fn open_client(py: Python<'_>, pd_endpoints: Vec<String>) -> PyResult<()> {
    if CLIENT.get().is_some() {
        return Ok(());
    }
    let rt = get_runtime();
    let client = py
        .detach(|| rt.block_on(async { open_ready_client(pd_endpoints).await }))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

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
    let mut end = prefix.clone();
    if let Some(last) = end.last_mut() {
        if *last == 255 {
            end.pop();
        } else {
            *last += 1;
        }
    } else {
        end.push(255);
    }

    let pairs = py
        .detach(|| {
            rt.block_on(async { client.scan(start..end, limit).await })
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

/// Room-prefixed HAMT node key: `hamt:node:<room_prefix_hex>:<structural_hash_hex>`.
/// The room prefix gives nodes belonging to the same room contiguous byte
/// ranges in TiKV's sorted keyspace, for locality -- see `room_tikv_prefix_raw`.
fn node_tikv_key(room_prefix: &[u8; ROOM_PREFIX_LEN], structural_hash: &StructuralHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(10 + ROOM_PREFIX_LEN * 2 + 1 + 32);
    key.extend_from_slice(b"hamt:node:");
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
/// The root pointer (`state_group -> room_prefix + root_hash`) lives in
/// per-instance SQL `state_hamt_roots`, so the caller supplies `room_prefix`
/// and `root_structural_hash`; TiKV holds only content-addressed nodes keyed
/// by `hamt:node:<room_prefix>:<hash>`, which are globally unique and so safe
/// to share across Synapse instances.
async fn materialize_state_hamt_async(
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
                .map(|hash| node_tikv_key(room_prefix, hash))
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

/// Materialize a state group's full state map directly from TiKV, in pure
/// Rust. `room_prefix` and `root_structural_hash` come from the per-instance
/// SQL root pointer (`state_hamt_roots`), so this needs no room_id or
/// room-version lookup. TiKV stores only content-addressed nodes.
#[pyfunction]
#[pyo3(text_signature = "(room_prefix, root_structural_hash, /)")]
pub fn materialize_state_hamt(
    py: Python<'_>,
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
            &room_prefix,
            root_structural_hash,
        ))
    })
    .map(Some)
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "tikv_engine")?;
    child_module.add_function(wrap_pyfunction!(open_client, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(scan_prefix, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_hamt, &child_module)?)?;

    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.tikv_engine", &child_module)?;

    Ok(())
}
