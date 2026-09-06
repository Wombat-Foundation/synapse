use std::sync::Arc;

use mtxdb::{NodeData, NodeId, PackfileStorage, StorageEngine};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;

use crate::database::core::{NodeStore, ROOM_PREFIX_LEN};

static DB: OnceCell<Arc<dyn StorageEngine>> = OnceCell::new();

fn db() -> PyResult<&'static Arc<dyn StorageEngine>> {
    DB.get()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mtxdb not opened"))
}

/// Extracts the room prefix and structural hash from a full node key.
/// Key format: hamt:node:<namespace_hex_16>:<room_prefix_hex>:<structural_hash_hex>
fn parse_node_key(key: &[u8]) -> Option<([u8; ROOM_PREFIX_LEN], [u8; 32])> {
    if !key.starts_with(b"hamt:node:") {
        return None;
    }

    if key.len() != 124 {
        return None;
    }

    let room_prefix_hex = &key[43..59];
    let mut room_prefix = [0u8; ROOM_PREFIX_LEN];
    if hex::decode_to_slice(room_prefix_hex, &mut room_prefix).is_err() {
        return None;
    }

    let hash_hex = &key[60..124];
    let mut hash = [0u8; 32];
    if hex::decode_to_slice(hash_hex, &mut hash).is_err() {
        return None;
    }

    Some((room_prefix, hash))
}

pub struct MtxdbStore {
    pub engine: Arc<dyn StorageEngine>,
}

impl NodeStore for MtxdbStore {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if let Some((room_prefix, structural_hash)) = parse_node_key(key) {
            let mut room_id = [0u8; 16];
            room_id[..ROOM_PREFIX_LEN].copy_from_slice(&room_prefix);

            let mut node_id = [0u8; 16];
            node_id.copy_from_slice(&structural_hash[..16]);

            let result = self
                .engine
                .get(&room_id, &node_id)
                .map_err(|e| e.to_string())?;
            Ok(result.map(|data| data.bytes.to_vec()))
        } else {
            Ok(None)
        }
    }
}

#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| {
        if DB.get().is_some() {
            return Ok(());
        }
        std::fs::create_dir_all(&path).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to create directory: {}", e))
        })?;

        let storage = PackfileStorage::open(std::path::PathBuf::from(&path)).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to open mtxdb: {}", e))
        })?;
        let _ = DB.set(Arc::new(storage));
        Ok(())
    })
}

#[pyfunction]
pub fn put_state_hamt_nodes(
    py: Python<'_>,
    _namespace: String,
    room_prefix: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<()> {
    let mut room_id = [0u8; 16];
    let prefix_len = std::cmp::min(room_prefix.len(), 16);
    room_id[..prefix_len].copy_from_slice(&room_prefix[..prefix_len]);

    let pairs: Vec<(NodeId, NodeData)> = nodes
        .into_iter()
        .filter_map(|(key_or_hash, bytes)| {
            let mut node_id = [0u8; 16];
            if key_or_hash.len() == 124 {
                if let Some((_, structural_hash)) = parse_node_key(&key_or_hash) {
                    node_id.copy_from_slice(&structural_hash[..16]);
                } else {
                    return None;
                }
            } else if key_or_hash.len() == 32 {
                node_id.copy_from_slice(&key_or_hash[..16]);
            } else if key_or_hash.len() == 16 {
                node_id.copy_from_slice(&key_or_hash);
            } else {
                return None;
            }
            Some((node_id, NodeData::new(bytes::Bytes::from(bytes))))
        })
        .collect();

    py.detach(|| {
        let engine = db()?;
        engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })
    })
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_client, m)?)?;
    m.add_function(wrap_pyfunction!(put_state_hamt_nodes, m)?)?;

    // Register as a submodule similar to mdbx_engine
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.mtxdb_engine", m)?;
    Ok(())
}
