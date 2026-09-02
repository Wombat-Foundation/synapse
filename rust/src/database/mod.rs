//! Embedded single-process HAMT node/root storage. `core` holds the
//! generic BFS materialize/selective-lookup walk and key encoding shared
//! by every backend; `fjall` and `mdbx` are thin drivers that implement
//! `core::NodeStore` over their own storage primitive and expose the
//! Python-facing module. `synapse/storage/databases/state/store.py` picks
//! which driver to open based on `embedded_hamt_engine` config.

pub mod core;
pub mod fjall;
pub mod mdbx;

use pyo3::prelude::*;

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    fjall::register_module(py, m)?;
    mdbx::register_module(py, m)?;
    Ok(())
}
