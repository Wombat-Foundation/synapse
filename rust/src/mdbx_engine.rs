//! Benchmark-only libmdbx engine: a minimal PyO3 surface (open/put/
//! batch_put/get/batch_get) mirroring [`crate::fjall_engine`]'s shape,
//! used solely to get real measured numbers for libmdbx vs. fjall vs.
//! Postgres. Not wired into `register_module` in `lib.rs` -- this is a
//! benchmarking scaffold, not a storage-engine candidate ready for
//! production use (no HAMT materialize/lookup port, no LRU node cache).
use std::sync::Mutex;

use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, Table, WriteFlags};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;

static DB: OnceCell<Database<NoWriteMap>> = OnceCell::new();
// libmdbx tables are borrowed from a txn's lifetime in this crate version;
// simplest correct approach for a throwaway benchmark is to open/close a
// table handle per-txn via `begin_*_txn().open_table(None)` on the default
// unnamed table, rather than caching a Table<'static>.
static WRITE_LOCK: Mutex<()> = Mutex::new(());

fn db() -> PyResult<&'static Database<NoWriteMap>> {
    DB.get()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mdbx not opened"))
}

#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| {
        if DB.get().is_some() {
            return Ok(());
        }
        let opts = DatabaseOptions {
            // Big enough ceiling for the benchmark corpus; mdbx grows the
            // mmap lazily so this isn't pre-allocated disk usage.
            mode: Mode::ReadWrite(ReadWriteOptions {
                max_size: Some(64isize * 1024 * 1024 * 1024),
                ..Default::default()
            }),
            ..Default::default()
        };
        let database = Database::<NoWriteMap>::open_with_options(&path, opts)
            .map_err(|e| e.to_string())
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        let _ = DB.set(database);
        Ok(())
    })
}

#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    py.detach(|| {
        let _guard = WRITE_LOCK.lock().unwrap();
        let database = db()?;
        let txn = database
            .begin_rw_txn()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let table: Table = txn
            .open_table(None)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        for (k, v) in &pairs {
            txn.put(&table, k, v, WriteFlags::UPSERT)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    })
}

/// Same as batch_put -- included so the benchmark script's "commit" naming
/// (mirroring fjall's transactional_batch_put / Postgres's execute_values)
/// lines up; there is no separate optimistic-retry path needed here since
/// this is a single designated writer, same as fjall.
#[pyfunction]
pub fn transactional_batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    batch_put(py, pairs)
}

#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    py.detach(|| {
        let database = db()?;
        let txn = database
            .begin_ro_txn()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let table: Table = txn
            .open_table(None)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let mut out = Vec::with_capacity(keys.len());
        for k in &keys {
            let v: Option<Vec<u8>> = txn
                .get(&table, k)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            if let Some(v) = v {
                out.push((k.clone(), v));
            }
        }
        Ok(out)
    })
}

pub fn register_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let child = PyModule::new(py, "mdbx_engine")?;
    child.add_function(wrap_pyfunction!(open_client, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_put, &child)?)?;
    child.add_function(wrap_pyfunction!(transactional_batch_put, &child)?)?;
    child.add_function(wrap_pyfunction!(batch_get, &child)?)?;
    parent.add_submodule(&child)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.mdbx_engine", &child)?;
    Ok(())
}
