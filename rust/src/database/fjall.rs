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
//! configured to reach that writer over a bridge instead of calling this
//! module's functions directly. `synapse/storage/databases/state/store.py`
//! picks which engine to wire up based on config.
//!
//! The BFS materialize/selective-lookup walk and key encoding are shared
//! with [`crate::database::mdbx`] via [`crate::database::core`]; this
//! module only implements [`core::NodeStore`] over a fjall partition.

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rezzy::hamt::StructuralHash;

use crate::database::core::{self, NodeCache, NodeStore, StateEntries, ROOM_PREFIX_LEN};
use crate::state_hamt::room_structural_key_raw;

static KEYSPACE: OnceCell<Keyspace> = OnceCell::new();
static PARTITION: OnceCell<PartitionHandle> = OnceCell::new();

/// Single partition holding every HAMT node, root record, and any other
/// key this engine is asked to store -- mirrors the flat keyspace
/// `tikv_engine` used (TiKV also had no notion of separate namespaces
/// beyond the key prefix scheme already baked into the keys themselves).
const PARTITION_NAME: &str = "hamt";

static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(core::new_node_cache)
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

/// Adapter implementing the generic [`core::NodeStore`] surface over a
/// fjall partition, for the shared BFS walk in `database::core`.
struct FjallStore<'a>(&'a PartitionHandle);

impl NodeStore for FjallStore<'_> {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.0
            .get(key)
            .map(|opt| opt.map(|v| v.to_vec()))
            .map_err(|e| e.to_string())
    }
}

fn open_client_sync(path: &str) -> Result<(), String> {
    if KEYSPACE.get().is_some() {
        return Ok(());
    }
    let keyspace = Config::new(path).open().map_err(|e| e.to_string())?;
    let partition = keyspace
        .open_partition(PARTITION_NAME, PartitionCreateOptions::default())
        .map_err(|e| e.to_string())?;
    if KEYSPACE.set(keyspace).is_err() || PARTITION.set(partition).is_err() {
        // Another thread raced us to it; both OnceCells reject the loser's
        // value, which is fine -- the first writer wins and subsequent
        // callers all observe a consistent, open keyspace.
    }
    Ok(())
}

/// Opens (or creates) the fjall keyspace at `path`. Must be called at most
/// once per process, from whichever single process this deployment has
/// designated as the HAMT storage writer -- see the module doc comment.
#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| open_client_sync(&path))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
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

/// Persists a batch of `(structural_hash, node_bytes)` pairs under their
/// namespaced, room-prefixed keys (`core::node_key`) -- the only correct
/// way to write nodes this engine's materialize/lookup walk can later
/// find; writing under a raw `structural_hash` key (as a naive `batch_put`
/// call would) is invisible to the BFS walk.
#[pyfunction]
#[pyo3(text_signature = "(namespace, room_prefix, nodes, /)")]
pub fn put_state_hamt_nodes(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<()> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {ROOM_PREFIX_LEN} bytes"
        ))
    })?;
    let nodes = nodes
        .into_iter()
        .map(|(hash, bytes)| {
            let hash: StructuralHash = hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("structural_hash must be 32 bytes")
            })?;
            Ok((hash, bytes))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let pairs = core::encode_node_writes(&namespace, &room_prefix, nodes);
    batch_put(py, pairs)
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
    let part = partition()?;
    py.detach(|| {
        core::materialize_state_hamt(
            &FjallStore(part),
            node_cache(),
            &namespace,
            &room_prefix,
            root_structural_hash,
            &structural_key,
        )
    })
    .map(Some)
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
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
    let part = partition()?;
    py.detach(|| core::materialize_state_hamts(&FjallStore(part), node_cache(), &namespace, roots))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

type PySelectiveQuery = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<(String, String)>);

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
    let part = partition()?;
    py.detach(|| {
        core::lookup_state_hamts(&FjallStore(part), node_cache(), &namespace, parsed_queries)
    })
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
    child_module.add_function(wrap_pyfunction!(put_state_hamt_nodes, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_hamt, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_hamts, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(lookup_state_hamts, &child_module)?)?;
    m.add_submodule(&child_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.fjall_engine", child_module)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::core::node_key;
    use crate::state_hamt::build_root_handle_and_nodes;

    /// Opens the (process-global) fjall keyspace against a fresh temp
    /// directory exactly once, sharing it across every test below --
    /// KEYSPACE/PARTITION are `OnceCell`s and can only be initialized once
    /// per test binary. Each test uses its own namespace/room-prefix keys,
    /// so sharing the keyspace doesn't let tests interfere with each other.
    fn ensure_open() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let dir = tempfile::tempdir().expect("tempdir");
            // Leak the tempdir so it outlives the process-global keyspace
            // instead of being cleaned up while still in use.
            let path = dir.keep();
            open_client_sync(path.to_str().unwrap()).expect("open fjall keyspace");
        });
    }

    #[test]
    fn put_get_roundtrip() {
        ensure_open();
        let part = PARTITION.get().unwrap();
        part.insert(b"fjall:test:roundtrip".to_vec(), b"hello".to_vec())
            .unwrap();
        assert_eq!(
            part.get(b"fjall:test:roundtrip")
                .unwrap()
                .map(|v| v.to_vec()),
            Some(b"hello".to_vec())
        );
    }

    #[test]
    fn batch_commit_is_atomic_and_scan_prefix_finds_it() {
        ensure_open();
        let ks = KEYSPACE.get().unwrap();
        let part = PARTITION.get().unwrap();
        let mut batch = ks.batch();
        batch.insert(part, b"fjall:test:scan:a".to_vec(), b"1".to_vec());
        batch.insert(part, b"fjall:test:scan:b".to_vec(), b"2".to_vec());
        batch.commit().unwrap();

        let mut results: Vec<(Vec<u8>, Vec<u8>)> = part
            .prefix(b"fjall:test:scan:")
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        results.sort();
        assert_eq!(
            results,
            vec![
                (b"fjall:test:scan:a".to_vec(), b"1".to_vec()),
                (b"fjall:test:scan:b".to_vec(), b"2".to_vec()),
            ]
        );
    }

    /// End-to-end: build a real HAMT via `build_root_handle_and_nodes`,
    /// persist its nodes through the fjall engine exactly as
    /// `put_state_hamt_objects` would, then materialize it back out via
    /// the shared `core::materialize_state_hamt` walk and check the round
    /// trip.
    #[test]
    fn materialize_state_hamt_round_trips_through_fjall() {
        ensure_open();
        let namespace = "test-namespace-materialize";
        let server_secret = [7u8; 32];
        let room_id = "!room:example.org";
        let room_prefix: [u8; ROOM_PREFIX_LEN] = [1, 2, 3, 4, 5, 6, 7, 8];

        let entries = vec![
            (
                "m.room.create".to_owned(),
                "".to_owned(),
                "$create:example.org".to_owned(),
            ),
            (
                "m.room.member".to_owned(),
                "@alice:example.org".to_owned(),
                "$join:example.org".to_owned(),
            ),
        ];
        let ((root_hash, _state_group_id), nodes) =
            build_root_handle_and_nodes(&server_secret, room_id, entries.clone())
                .expect("build HAMT");

        for (hash, bytes) in nodes {
            let hash: StructuralHash = hash;
            let key = node_key(namespace, &room_prefix, &hash);
            put_raw(&key, &bytes);
        }

        let root_hash: StructuralHash = root_hash;
        let structural_key = room_structural_key_raw(&server_secret, room_id);
        let part = PARTITION.get().unwrap();
        let mut materialized = core::materialize_state_hamt(
            &FjallStore(part),
            node_cache(),
            namespace,
            &room_prefix,
            root_hash,
            &structural_key,
        )
        .expect("materialize");
        materialized.sort();

        let mut expected = entries;
        expected.sort();
        assert_eq!(materialized, expected);
    }

    fn put_raw(key: &[u8], value: &[u8]) {
        let part = PARTITION.get().unwrap();
        part.insert(key.to_vec(), value.to_vec()).unwrap();
    }
}
