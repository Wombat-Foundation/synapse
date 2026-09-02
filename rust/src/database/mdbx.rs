//! Embedded, single-process (but natively multi-process-mmap-safe) HAMT
//! node/root storage backed by [`libmdbx`] (a Rust wrapper over the real C
//! libmdbx library). Unlike [`crate::database::fjall`], every worker
//! process can open this database directly -- mdbx supports concurrent
//! multi-process readers/writer via mmap and its own file locking, so no
//! bridge daemon is needed as long as all processes share a filesystem
//! (see the module-level architecture doc for the single-host assumption).
//!
//! The BFS materialize/selective-lookup walk and key encoding are shared
//! with [`crate::database::fjall`] via [`crate::database::core`]; this
//! module only implements [`core::NodeStore`] over an mdbx read
//! transaction.

use std::sync::Mutex;

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, Table, Transaction, WriteFlags,
    RO,
};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rezzy::hamt::StructuralHash;

use crate::database::core::{self, NodeCache, NodeStore, StateEntries, ROOM_PREFIX_LEN};
use crate::state_hamt::room_structural_key_raw;

static DB: OnceCell<Database<NoWriteMap>> = OnceCell::new();
// libmdbx tables are borrowed from a txn's lifetime in this crate version;
// the simplest correct approach is to open a table handle per-txn via
// `begin_*_txn().open_table(None)` on the default unnamed table, rather
// than caching a `Table<'static>`.
static WRITE_LOCK: Mutex<()> = Mutex::new(());

static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(core::new_node_cache)
}

fn db() -> PyResult<&'static Database<NoWriteMap>> {
    DB.get()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mdbx not opened"))
}

fn map_mdbx_err(e: impl ToString) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

/// Adapter implementing the generic [`core::NodeStore`] surface over one
/// mdbx read transaction + table, for the shared BFS walk in
/// `database::core`. A single read txn is held for the whole walk, which
/// gives it a consistent mmap snapshot (mdbx's usual MVCC read-view).
struct MdbxStore<'a> {
    txn: &'a Transaction<'a, RO, NoWriteMap>,
    table: &'a Table<'a>,
}

impl NodeStore for MdbxStore<'_> {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.txn.get(self.table, key).map_err(|e| e.to_string())
    }
}

fn open_client_sync(path: &str) -> Result<(), String> {
    if DB.get().is_some() {
        return Ok(());
    }
    let opts = DatabaseOptions {
        // Big enough ceiling for a real HAMT corpus; mdbx grows the mmap
        // lazily so this isn't pre-allocated disk usage.
        mode: Mode::ReadWrite(ReadWriteOptions {
            max_size: Some(64isize * 1024 * 1024 * 1024),
            ..Default::default()
        }),
        ..Default::default()
    };
    let database =
        Database::<NoWriteMap>::open_with_options(path, opts).map_err(|e| e.to_string())?;
    let _ = DB.set(database);
    Ok(())
}

/// Opens (or creates) the mdbx database at `path`. Safe to call from every
/// worker process concurrently, unlike `fjall::open_client` -- mdbx's mmap
/// + file locking supports multiple processes attaching to the same
/// database directory, one writer at a time, many concurrent readers.
#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| open_client_sync(&path)).map_err(map_mdbx_err)
}

#[pyfunction]
pub fn put(py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
    batch_put(py, vec![(key, value)])
}

#[pyfunction]
pub fn get(py: Python<'_>, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
    py.detach(|| {
        let database = db()?;
        let txn = database.begin_ro_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        txn.get(&table, &key).map_err(map_mdbx_err)
    })
}

#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    py.detach(|| {
        let database = db()?;
        let txn = database.begin_ro_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        let mut out = Vec::with_capacity(keys.len());
        for k in &keys {
            let v: Option<Vec<u8>> = txn.get(&table, k).map_err(map_mdbx_err)?;
            if let Some(v) = v {
                out.push((k.clone(), v));
            }
        }
        Ok(out)
    })
}

#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    py.detach(|| {
        let _guard = WRITE_LOCK.lock().unwrap();
        let database = db()?;
        let txn = database.begin_rw_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        for (k, v) in &pairs {
            txn.put(&table, k, v, WriteFlags::UPSERT)
                .map_err(map_mdbx_err)?;
        }
        txn.commit().map_err(map_mdbx_err)?;
        Ok(())
    })
}

/// Same as batch_put -- there is no separate optimistic-retry path needed
/// here (unlike the old TiKV engine): mdbx's single-writer-per-txn commit
/// already serializes concurrent writers at the mmap/file-lock level.
#[pyfunction]
pub fn transactional_batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    batch_put(py, pairs)
}

#[pyfunction]
pub fn delete(py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
    batch_delete(py, vec![key])
}

#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    py.detach(|| {
        let _guard = WRITE_LOCK.lock().unwrap();
        let database = db()?;
        let txn = database.begin_rw_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        for k in &keys {
            txn.del(&table, k, None).map_err(map_mdbx_err)?;
        }
        txn.commit().map_err(map_mdbx_err)?;
        Ok(())
    })
}

#[pyfunction]
pub fn scan_prefix(
    py: Python<'_>,
    prefix: Vec<u8>,
    limit: u32,
) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    py.detach(|| {
        let database = db()?;
        let txn = database.begin_ro_txn().map_err(map_mdbx_err)?;
        let table: Table = txn.open_table(None).map_err(map_mdbx_err)?;
        let mut cursor = txn.cursor(&table).map_err(map_mdbx_err)?;
        let mut results = Vec::new();
        let iter = cursor.iter_from::<Vec<u8>, Vec<u8>>(&prefix);
        for entry in iter {
            if results.len() >= limit as usize {
                break;
            }
            let (k, v) = entry.map_err(map_mdbx_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            results.push((k, v));
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
    py.detach(|| {
        let database = db().map_err(|e| e.to_string())?;
        let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
        let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        core::materialize_state_hamt(
            &store,
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
    py.detach(|| {
        let database = db().map_err(|e| e.to_string())?;
        let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
        let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        core::materialize_state_hamts(&store, node_cache(), &namespace, roots)
    })
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
    py.detach(|| {
        let database = db().map_err(|e| e.to_string())?;
        let txn = database.begin_ro_txn().map_err(|e| e.to_string())?;
        let table: Table = txn.open_table(None).map_err(|e| e.to_string())?;
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        core::lookup_state_hamts(&store, node_cache(), &namespace, parsed_queries)
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

pub fn register_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let child = PyModule::new(py, "mdbx_engine")?;
    child.add_function(wrap_pyfunction!(open_client, &child)?)?;
    child.add_function(wrap_pyfunction!(put, &child)?)?;
    child.add_function(wrap_pyfunction!(get, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_get, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_put, &child)?)?;
    child.add_function(wrap_pyfunction!(transactional_batch_put, &child)?)?;
    child.add_function(wrap_pyfunction!(delete, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_delete, &child)?)?;
    child.add_function(wrap_pyfunction!(scan_prefix, &child)?)?;
    child.add_function(wrap_pyfunction!(put_state_hamt_nodes, &child)?)?;
    child.add_function(wrap_pyfunction!(materialize_state_hamt, &child)?)?;
    child.add_function(wrap_pyfunction!(materialize_state_hamts, &child)?)?;
    child.add_function(wrap_pyfunction!(lookup_state_hamts, &child)?)?;
    parent.add_submodule(&child)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.mdbx_engine", &child)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::core::node_key;
    use crate::state_hamt::build_root_handle_and_nodes;

    fn ensure_open() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.keep();
            open_client_sync(path.to_str().unwrap()).expect("open mdbx database");
        });
    }

    #[test]
    fn put_get_roundtrip() {
        ensure_open();
        let database = DB.get().unwrap();
        let txn = database.begin_rw_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        txn.put(&table, b"mdbx:test:roundtrip", b"hello", WriteFlags::UPSERT)
            .unwrap();
        txn.commit().unwrap();

        let txn = database.begin_ro_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        let v: Option<Vec<u8>> = txn.get(&table, b"mdbx:test:roundtrip").unwrap();
        assert_eq!(v, Some(b"hello".to_vec()));
    }

    /// End-to-end: build a real HAMT via `build_root_handle_and_nodes`,
    /// persist its nodes exactly as `put_state_hamt_objects` would, then
    /// materialize it back out via the shared `core::materialize_state_hamt`
    /// walk and check the round trip.
    #[test]
    fn materialize_state_hamt_round_trips_through_mdbx() {
        ensure_open();
        let namespace = "test-namespace-materialize-mdbx";
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

        let database = DB.get().unwrap();
        let txn = database.begin_rw_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        for (hash, bytes) in nodes {
            let hash: StructuralHash = hash;
            let key = node_key(namespace, &room_prefix, &hash);
            txn.put(&table, &key, &bytes, WriteFlags::UPSERT).unwrap();
        }
        txn.commit().unwrap();

        let root_hash: StructuralHash = root_hash;
        let structural_key = room_structural_key_raw(&server_secret, room_id);

        let txn = database.begin_ro_txn().unwrap();
        let table: Table = txn.open_table(None).unwrap();
        let store = MdbxStore {
            txn: &txn,
            table: &table,
        };
        let mut materialized = core::materialize_state_hamt(
            &store,
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
}
