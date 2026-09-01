//! Embedded, single-process HAMT node/root storage backed by [`fjall`], a
//! pure-Rust LSM engine. Replaces the former `tikv_engine` (a TiKV/PD raw-KV
//! client): the storage requirements here -- write-once immutable,
//! content-addressed 16-32 byte keys, point lookups only, no range scans
//! other than room-prefix purge -- don't need a distributed cluster, and
//! fjall gives a local single-writer transactional batch for free via its
//! `single_writer_tx`-backed [`fjall::Keyspace::batch`].
//!
//! fjall's LSM tree is opened by exactly one OS process at a time (it takes
//! an exclusive lock on the keyspace directory). In a worker deployment,
//! only the process designated as the HAMT storage writer calls
//! [`open_client`] against the local path; every other process must be
//! configured to reach that writer over the `hamt_rpc` Unix-socket bridge
//! (see that module) instead of calling this module's functions directly.
//! `synapse/storage/databases/state/store.py` picks which one to wire up
//! based on config.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use lru::LruCache;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rezzy::hamt::{HamtNode, StructuralHash};
use sha2::{Digest, Sha256};

use crate::state_hamt::{
    decode_persisted_node_verified, lookup_from_node_map, materialize_from_node_map,
    room_structural_key_raw,
};

static KEYSPACE: OnceCell<Keyspace> = OnceCell::new();
static PARTITION: OnceCell<PartitionHandle> = OnceCell::new();

/// Single partition holding every HAMT node, root record, and any other
/// key this engine is asked to store -- mirrors the flat keyspace
/// `tikv_engine` used (TiKV also had no notion of separate namespaces
/// beyond the key prefix scheme already baked into the keys themselves).
const PARTITION_NAME: &str = "hamt";

/// Process-wide in-memory cache of decoded HAMT nodes, keyed by their full
/// (namespaced, room-prefixed) fjall key. See `tikv_engine`'s former
/// `NODE_CACHE` doc comment (same rationale carries over unchanged): HAMT
/// nodes are immutable and content-addressed, so a cache hit is always
/// correct modulo the structural-hash check performed on every hit.
const NODE_CACHE_CAPACITY: usize = 100_000;
type NodeCache = Mutex<LruCache<Vec<u8>, Arc<HamtNode<String, String>>>>;
static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(|| {
        Mutex::new(LruCache::new(
            NonZeroUsize::new(NODE_CACHE_CAPACITY).expect("cache capacity is nonzero"),
        ))
    })
}

fn partition() -> PyResult<&'static PartitionHandle> {
    PARTITION.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "fjall keyspace is not open. Call open_client first.",
        )
    })
}

fn keyspace() -> PyResult<&'static Keyspace> {
    KEYSPACE.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "fjall keyspace is not open. Call open_client first.",
        )
    })
}

fn map_fjall_err(e: fjall::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

/// Opens (or creates) the fjall keyspace at `path`. Must be called at most
/// once per process, from whichever single process this deployment has
/// designated as the HAMT storage writer -- see the module doc comment.
#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    if KEYSPACE.get().is_some() {
        return Ok(());
    }
    py.detach(|| -> PyResult<()> {
        let keyspace = Config::new(&path).open().map_err(map_fjall_err)?;
        let partition = keyspace
            .open_partition(PARTITION_NAME, PartitionCreateOptions::default())
            .map_err(map_fjall_err)?;
        if KEYSPACE.set(keyspace).is_err() || PARTITION.set(partition).is_err() {
            // Another thread raced us to it; both OnceCells reject the
            // loser's value, which is fine -- the first writer wins and
            // subsequent callers all observe a consistent, open keyspace.
        }
        Ok(())
    })
}

#[pyfunction]
pub fn put(py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
    let part = partition()?;
    py.detach(|| part.insert(key, value).map_err(map_fjall_err))
}

#[pyfunction]
pub fn get(py: Python<'_>, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
    let part = partition()?;
    py.detach(|| {
        part.get(&key)
            .map(|opt| opt.map(|v| v.to_vec()))
            .map_err(map_fjall_err)
    })
}

#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let part = partition()?;
    py.detach(|| {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = part.get(&key).map_err(map_fjall_err)? {
                results.push((key, value.to_vec()));
            }
        }
        Ok(results)
    })
}

#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    let ks = keyspace()?;
    let part = partition()?;
    py.detach(|| {
        let mut batch = ks.batch();
        for (key, value) in pairs {
            batch.insert(part, key, value);
        }
        batch.commit().map_err(map_fjall_err)
    })
}

/// Atomically publish a set of immutable HAMT nodes and its root/index
/// record. fjall's `Batch::commit` is a single journaled write, so unlike
/// the TiKV version this needs no optimistic-transaction retry loop: there
/// is exactly one writer process for this keyspace (see module doc
/// comment), so there is no concurrent-writer conflict to retry against.
#[pyfunction]
pub fn transactional_batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    batch_put(py, pairs)
}

#[pyfunction]
pub fn delete(py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
    let part = partition()?;
    py.detach(|| part.remove(key).map_err(map_fjall_err))
}

#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    let ks = keyspace()?;
    let part = partition()?;
    py.detach(|| {
        let mut batch = ks.batch();
        for key in keys {
            batch.remove(part, key);
        }
        batch.commit().map_err(map_fjall_err)
    })
}

#[pyfunction]
pub fn scan_prefix(
    py: Python<'_>,
    prefix: Vec<u8>,
    limit: u32,
) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let part = partition()?;
    py.detach(|| {
        let mut results = Vec::new();
        for entry in part.prefix(&prefix) {
            if results.len() >= limit as usize {
                break;
            }
            let (k, v) = entry.map_err(map_fjall_err)?;
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    })
}

/// Fixed width of the room-scoped key prefix. Kept identical to the former
/// TiKV scheme (see that module's removed comment): 8 bytes is a locality
/// hint, not an identity -- the full key is always
/// `prefix || structural_hash`, which stays unique regardless of prefix
/// collisions.
const ROOM_PREFIX_LEN: usize = 8;

/// Namespaced, room-prefixed HAMT node key:
/// `hamt:node:<namespace_hash>:<room_prefix_hex>:<structural_hash_hex>`.
fn node_key(
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

/// Batch size for fjall reads while walking the HAMT -- kept only to bound
/// how much of the node cache's lock we hold at once per round; fjall
/// reads themselves are local and don't benefit from network batching the
/// way TiKV's `batch_get` did.
const NODE_FETCH_BATCH_SIZE: usize = 100;

/// Fetch a state group's HAMT: BFS-fetch the reachable nodes (batched),
/// decode each one, and materialize `(event_type, state_key, event_id)`
/// triples -- without crossing back into Python per node. Mirrors
/// `tikv_engine::materialize_state_hamt_async` but synchronously, since
/// fjall has no network round-trip to overlap.
fn materialize_state_hamt_sync(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    root_structural_hash: StructuralHash,
    structural_key: &[u8; 32],
) -> Result<Vec<(String, String, String)>, String> {
    let part = PARTITION
        .get()
        .ok_or_else(|| "fjall keyspace is not open. Call open_client first.".to_owned())?;

    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);
    let mut to_fetch: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);

    while !to_fetch.is_empty() {
        let current_batch: Vec<StructuralHash> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = node_cache().lock().unwrap();
                for hash in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *hash));
                            } else {
                                let node = node.clone();
                                for child in &node.children {
                                    let child_hash = child.structural_hash();
                                    if seen.insert(child_hash) {
                                        to_fetch.insert(child_hash);
                                    }
                                }
                                node_map.insert(*hash, node);
                            }
                        }
                        None => still_missing.push((key, *hash)),
                    }
                }
            }

            for (key, expected_hash) in still_missing {
                let node_bytes = part
                    .get(&key)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, structural_key, expected_hash)?;

                for child in &node.children {
                    let child_hash = child.structural_hash();
                    if seen.insert(child_hash) {
                        to_fetch.insert(child_hash);
                    }
                }

                node_cache().lock().unwrap().put(key, node.clone());
                node_map.insert(expected_hash, node);
            }
        }
    }

    materialize_from_node_map(&root_structural_hash, &node_map)
}

#[pyfunction]
#[pyo3(
    text_signature = "(namespace, room_prefix, root_structural_hash, server_secret, room_id, /)"
)]
pub fn materialize_state_hamt(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    root_structural_hash: Vec<u8>,
    server_secret: Vec<u8>,
    room_id: &str,
) -> PyResult<Option<StateEntries>> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {ROOM_PREFIX_LEN} bytes"
        ))
    })?;
    let root_structural_hash: StructuralHash = root_structural_hash.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
    })?;
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    let structural_key = room_structural_key_raw(&server_secret, room_id);
    py.detach(|| {
        materialize_state_hamt_sync(
            &namespace,
            &room_prefix,
            root_structural_hash,
            &structural_key,
        )
    })
    .map(Some)
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

type NodeLocation = ([u8; ROOM_PREFIX_LEN], [u8; 32], StructuralHash);

/// One materialized state group: `(event_type, state_key, event_id)` triples.
type StateEntries = Vec<(String, String, String)>;

fn materialize_state_hamts_sync(
    namespace: &str,
    roots: Vec<NodeLocation>,
) -> Result<Vec<StateEntries>, String> {
    let part = PARTITION
        .get()
        .ok_or_else(|| "fjall keyspace is not open. Call open_client first.".to_owned())?;

    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = roots.iter().copied().collect();
    let mut to_fetch = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = node_cache().lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *room_prefix, *structural_key, *hash));
                            } else {
                                let node = node.clone();
                                for child in &node.children {
                                    let child_location =
                                        (*room_prefix, *structural_key, child.structural_hash());
                                    if seen.insert(child_location) {
                                        to_fetch.insert(child_location);
                                    }
                                }
                                node_map.insert((*room_prefix, *structural_key, *hash), node);
                            }
                        }
                        None => still_missing.push((key, *room_prefix, *structural_key, *hash)),
                    }
                }
            }

            for (key, room_prefix, structural_key, expected_hash) in still_missing {
                let node_bytes = part
                    .get(&key)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, &structural_key, expected_hash)?;

                for child in &node.children {
                    let child_location = (room_prefix, structural_key, child.structural_hash());
                    if seen.insert(child_location) {
                        to_fetch.insert(child_location);
                    }
                }

                node_cache().lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, structural_key, expected_hash), node);
            }
        }
    }

    type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
    let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
    for ((room_prefix, _, hash), node) in node_map {
        nodes_by_prefix
            .entry(room_prefix)
            .or_default()
            .insert(hash, node);
    }

    roots
        .into_iter()
        .map(|(room_prefix, _, root_hash)| {
            let nodes = nodes_by_prefix.get(&room_prefix).ok_or_else(|| {
                format!(
                    "Missing nodes for room prefix: {}",
                    hex::encode(room_prefix)
                )
            })?;
            materialize_from_node_map(&root_hash, nodes)
        })
        .collect()
}

#[pyfunction]
#[pyo3(text_signature = "(namespace, server_secret, roots, /)")]
pub fn materialize_state_hamts(
    py: Python<'_>,
    namespace: String,
    server_secret: Vec<u8>,
    roots: Vec<(Vec<u8>, Vec<u8>, String)>,
) -> PyResult<Vec<StateEntries>> {
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    let roots = roots
        .into_iter()
        .map(|(room_prefix, root_hash, room_id)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {ROOM_PREFIX_LEN} bytes"
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
            })?;
            let structural_key = room_structural_key_raw(&server_secret, &room_id);
            Ok((room_prefix, structural_key, root_hash))
        })
        .collect::<PyResult<Vec<_>>>()?;
    py.detach(|| materialize_state_hamts_sync(&namespace, roots))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

type SelectiveQuery = (
    [u8; ROOM_PREFIX_LEN],
    StructuralHash,
    [u8; 32],
    Vec<(String, String)>,
);
type PySelectiveQuery = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<(String, String)>);

fn lookup_state_hamts_sync(
    namespace: &str,
    queries: Vec<SelectiveQuery>,
) -> Result<Vec<StateEntries>, String> {
    let part = PARTITION
        .get()
        .ok_or_else(|| "fjall keyspace is not open. Call open_client first.".to_owned())?;

    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = queries.iter().map(|(p, h, k, _)| (*p, *k, *h)).collect();
    let mut to_fetch: HashSet<NodeLocation> = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = node_cache().lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *room_prefix, *structural_key, *hash));
                            } else {
                                node_map
                                    .insert((*room_prefix, *structural_key, *hash), node.clone());
                            }
                        }
                        None => still_missing.push((key, *room_prefix, *structural_key, *hash)),
                    }
                }
            }

            for (key, room_prefix, structural_key, expected_hash) in still_missing {
                let node_bytes = part
                    .get(&key)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, &structural_key, expected_hash)?;

                node_cache().lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, structural_key, expected_hash), node);
            }
        }

        type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
        let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
        for ((room_prefix, _, hash), node) in &node_map {
            nodes_by_prefix
                .entry(*room_prefix)
                .or_default()
                .insert(*hash, Arc::clone(node));
        }

        for (room_prefix, root_hash, structural_key, keys) in &queries {
            if let Some(prefix_nodes) = nodes_by_prefix.get(room_prefix) {
                if prefix_nodes.contains_key(root_hash) {
                    let (_entries, missing) =
                        lookup_from_node_map(root_hash, structural_key, keys, prefix_nodes)?;
                    for missing_hash in missing {
                        let child_loc = (*room_prefix, *structural_key, missing_hash);
                        if seen.insert(child_loc) {
                            to_fetch.insert(child_loc);
                        }
                    }
                }
            }
        }
    }

    type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
    let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
    for ((room_prefix, _, hash), node) in node_map {
        nodes_by_prefix
            .entry(room_prefix)
            .or_default()
            .insert(hash, node);
    }

    queries
        .into_iter()
        .map(|(room_prefix, root_hash, structural_key, keys)| {
            let prefix_nodes = nodes_by_prefix.get(&room_prefix).ok_or_else(|| {
                format!(
                    "Missing nodes for room prefix: {}",
                    hex::encode(room_prefix)
                )
            })?;
            let (entries, missing) =
                lookup_from_node_map(&root_hash, &structural_key, &keys, prefix_nodes)?;
            if !missing.is_empty() {
                return Err(format!(
                    "Unresolved missing nodes after fetch loop for root {:02x?}",
                    root_hash
                ));
            }
            Ok(entries)
        })
        .collect()
}

#[pyfunction]
#[pyo3(text_signature = "(namespace, queries, /)")]
pub fn lookup_state_hamts(
    py: Python<'_>,
    namespace: String,
    queries: Vec<PySelectiveQuery>,
) -> PyResult<Vec<StateEntries>> {
    let parsed_queries = queries
        .into_iter()
        .map(|(room_prefix, root_hash, structural_key, keys)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {ROOM_PREFIX_LEN} bytes"
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
            })?;
            let structural_key: [u8; 32] = structural_key.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("structural_key must be 32 bytes")
            })?;
            Ok((room_prefix, root_hash, structural_key, keys))
        })
        .collect::<PyResult<Vec<_>>>()?;
    py.detach(|| lookup_state_hamts_sync(&namespace, parsed_queries))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "fjall_engine")?;
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
    child_module.add_function(wrap_pyfunction!(lookup_state_hamts, &child_module)?)?;
    m.add_submodule(&child_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.fjall_engine", child_module)?;
    Ok(())
}
