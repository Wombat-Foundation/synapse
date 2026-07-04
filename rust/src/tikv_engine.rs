use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use tikv_client::RawClient;
use tokio::runtime::Runtime;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();
static CLIENT: OnceCell<RawClient> = OnceCell::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime")
    })
}

#[pyfunction]
pub fn open_client(pd_endpoints: Vec<String>) -> PyResult<()> {
    if CLIENT.get().is_some() {
        return Ok(());
    }
    let rt = get_runtime();
    let client = rt.block_on(async {
        RawClient::new(pd_endpoints).await
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    
    CLIENT.set(client).map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err("Failed to set TiKV Client instance")
    })?;
    Ok(())
}

#[pyfunction]
pub fn put(key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("TiKV client is not open. Call open_client first.")
    })?;
    let rt = get_runtime();
    rt.block_on(async {
        client.put(key, value).await
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

#[pyfunction]
pub fn get(key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("TiKV client is not open. Call open_client first.")
    })?;
    let rt = get_runtime();
    let val = rt.block_on(async {
        client.get(key).await
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(val)
}

#[pyfunction]
pub fn batch_get(keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("TiKV client is not open. Call open_client first.")
    })?;
    let rt = get_runtime();
    let pairs = rt.block_on(async {
        client.batch_get(keys).await
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    
    let results: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .into_iter()
        .map(|pair| {
            let (k, v): (tikv_client::Key, tikv_client::Value) = pair.into();
            (k.into(), v.into())
        })
        .collect();
    Ok(results)
}

#[pyfunction]
pub fn batch_put(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("TiKV client is not open. Call open_client first.")
    })?;
    let rt = get_runtime();
    rt.block_on(async {
        client.batch_put(pairs).await
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

#[pyfunction]
pub fn delete(key: Vec<u8>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("TiKV client is not open. Call open_client first.")
    })?;
    let rt = get_runtime();
    rt.block_on(async {
        client.delete(key).await
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

#[pyfunction]
pub fn scan_prefix(prefix: Vec<u8>, limit: u32) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("TiKV client is not open. Call open_client first.")
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
    
    let pairs = rt.block_on(async {
        client.scan(start..end, limit).await
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    
    let results: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .into_iter()
        .map(|pair| {
            let (k, v): (tikv_client::Key, tikv_client::Value) = pair.into();
            (k.into(), v.into())
        })
        .collect();
    Ok(results)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "tikv_engine")?;
    child_module.add_function(wrap_pyfunction!(open_client, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(scan_prefix, &child_module)?)?;

    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.tikv_engine", &child_module)?;

    Ok(())
}
