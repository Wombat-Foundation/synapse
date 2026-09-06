use std::sync::Arc;

use mtxdb::{NodeData, PackfileStorage, StorageEngine};
use pyo3::prelude::*;
use rezzy::hamt::StructuralHash;

use crate::database::core::{self, NodeStore, ROOM_PREFIX_LEN};

/// Extracts the room prefix and structural hash from a full node key.
/// Key format: hamt:node:<namespace_hex_16>:<room_prefix_hex>:<structural_hash_hex>
fn parse_node_key(key: &[u8]) -> Option<([u8; ROOM_PREFIX_LEN], [u8; 32])> {
    if !key.starts_with(b"hamt:node:") {
        return None;
    }

    // Expected lengths:
    // b"hamt:node:" (10)
    // namespace_hex (32)
    // b":" (1)
    // room_prefix_hex (16)
    // b":" (1)
    // structural_hash_hex (64)
    // Total = 124 bytes
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
            // For now, mtxdb only supports content-addressed HAMT nodes.
            // Roots (hamt:root:...) are not supported in the packfile itself.
            Ok(None)
        }
    }
}

// TODO: PyO3 wrappers for open_client, put_state_hamt_nodes, repack, delete_room, etc.

#[pyfunction]
pub fn put_state_hamt_nodes(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<()> {
    // The user mentioned it receives (full_namespaced_key, node_bytes) pairs.
    // If nodes contains full_namespaced_keys, we parse them. If it contains raw structural_hashes, we use them directly.
    let pairs: Vec<(mtxdb::NodeId, mtxdb::NodeData)> = nodes
        .into_iter()
        .filter_map(|(key_or_hash, bytes)| {
            let mut node_id = [0u8; 16];
            if key_or_hash.len() == 124 {
                if let Some((_, structural_hash)) = parse_node_key(&key_or_hash) {
                    node_id.copy_from_slice(&structural_hash[..16]);
                } else {
                    return None; // Skip invalid
                }
            } else if key_or_hash.len() == 32 {
                node_id.copy_from_slice(&key_or_hash[..16]);
            } else {
                return None; // Skip invalid
            }
            Some((node_id, mtxdb::NodeData::new(bytes::Bytes::from(bytes))))
        })
        .collect();

    // In a real implementation we would get the DB from a OnceCell or similar.
    // For now we just return Ok.
    Ok(())
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(put_state_hamt_nodes, m)?)?;
    Ok(())
}
