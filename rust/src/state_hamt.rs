/*
 * This file is licensed under the Affero General Public License (AGPL) version 3.
 *
 * Copyright (C) 2026 Element Creations Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * See the GNU Affero General Public License for more details:
 * <https://www.gnu.org/licenses/agpl-3.0.html>.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use hmac::{Hmac, Mac};
use pyo3::prelude::*;
use pyo3::types::{PyModule, PyModuleMethods};
use rezzy::{
    hamt::{HamtNode, NodeRef, PersistedInternalNode, RootHandle, StructuralHash},
    LtHash,
};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
type RootHandleParts = ([u8; 16], [u8; 32]);
type PersistedNodeBytes = (StructuralHash, Vec<u8>);
type BuiltRoot = (RootHandleParts, Vec<PersistedNodeBytes>);
type PyRootHandleParts = (Vec<u8>, Vec<u8>);
type PyPersistedNodeBytes = (Vec<u8>, Vec<u8>);
type PyBuiltRoot = (PyRootHandleParts, Vec<PyPersistedNodeBytes>);
type PyStateEntry = (String, String, String);

#[must_use]
fn room_structural_key_raw(server_secret: &[u8; 32], room_id: &str) -> [u8; 32] {
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(server_secret).expect("HMAC can take key of any size");
    mac.update(room_id.as_bytes());
    mac.finalize().into_bytes().into()
}

fn root_handle_parts(root_handle: &RootHandle) -> RootHandleParts {
    (root_handle.structural_hash, root_handle.state_group_id)
}

fn collect_persisted_nodes(
    node: Arc<HamtNode<String, String>>,
    seen: &mut HashSet<StructuralHash>,
    nodes: &mut Vec<PersistedNodeBytes>,
) {
    if !seen.insert(node.structural_hash) {
        return;
    }

    for child in &node.children {
        if let NodeRef::Resolved(child_node) = child {
            collect_persisted_nodes(child_node.clone(), seen, nodes);
        }
    }

    let persisted: PersistedInternalNode<String, String> = node.as_ref().into();
    nodes.push((persisted.structural_hash, persisted.encode_v1()));
}

fn build_root_handle_and_nodes(
    server_secret: &[u8; 32],
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> Result<BuiltRoot, String> {
    let structural_key = room_structural_key_raw(server_secret, room_id);
    let lattice = LtHash::default();
    let entries = entries
        .into_iter()
        .map(|(event_type, state_key, event_id)| {
            (
                serde_json::to_string(&(event_type, state_key))
                    .expect("state key serialization should not fail"),
                event_id,
            )
        });
    let (root_handle, root_node) =
        rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
            .map_err(|e| format!("Failed to build HAMT root: {e:?}"))?;

    let mut seen = HashSet::new();
    let mut nodes = Vec::new();
    collect_persisted_nodes(root_node, &mut seen, &mut nodes);

    Ok((root_handle_parts(&root_handle), nodes))
}

fn decode_persisted_node(node_bytes: &[u8]) -> Result<Arc<HamtNode<String, String>>, String> {
    let persisted = PersistedInternalNode::<String, String>::decode_v1(node_bytes)
        .map_err(|e| format!("Failed to decode persisted HAMT node: {e}"))?;
    let node: HamtNode<String, String> = persisted
        .try_into()
        .map_err(|e| format!("Failed to reconstruct HAMT node: {e}"))?;
    Ok(Arc::new(node))
}

#[pyfunction]
#[pyo3(text_signature = "(server_secret, room_id, /)")]
pub fn room_structural_key(server_secret: Vec<u8>, room_id: &str) -> PyResult<Vec<u8>> {
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    Ok(room_structural_key_raw(&server_secret, room_id).to_vec())
}

#[pyfunction]
#[pyo3(text_signature = "(server_secret, room_id, entries, /)")]
pub fn build_root_handle(
    server_secret: Vec<u8>,
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> PyResult<PyBuiltRoot> {
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    let (root_handle_parts, nodes) = build_root_handle_and_nodes(&server_secret, room_id, entries)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    Ok((
        (root_handle_parts.0.to_vec(), root_handle_parts.1.to_vec()),
        nodes
            .into_iter()
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(root_node_bytes, nodes, /)")]
pub fn materialize_state_entries(
    root_node_bytes: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<Vec<PyStateEntry>> {
    let root_node = decode_persisted_node(&root_node_bytes)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();

    for (hash_bytes, node_bytes) in nodes {
        let hash: [u8; 16] = hash_bytes.try_into().map_err(|_| {
            pyo3::exceptions::PyValueError::new_err("structural hash must be 16 bytes")
        })?;
        let node = decode_persisted_node(&node_bytes)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        node_map.insert(hash, node);
    }

    let mut entries = Vec::new();
    let mut resolver = |hash: &StructuralHash| -> Result<Arc<HamtNode<String, String>>, String> {
        node_map
            .get(hash)
            .cloned()
            .ok_or_else(|| format!("Missing persisted HAMT node: {:02x?}", hash))
    };

    root_node
        .visit_entries(&mut resolver, &mut |key, value| {
            let (event_type, state_key): (String, String) = serde_json::from_str(key)
                .map_err(|e| format!("Failed to decode HAMT state key: {e}"))?;
            entries.push((event_type, state_key, value.clone()));
            Ok::<(), String>(())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    Ok(entries)
}

#[pyfunction]
#[pyo3(text_signature = "(node_bytes, /)")]
pub fn node_child_hashes(node_bytes: Vec<u8>) -> PyResult<Vec<Vec<u8>>> {
    let node = PersistedInternalNode::<String, String>::decode_v1(&node_bytes).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "Failed to decode persisted HAMT node: {e}"
        ))
    })?;
    Ok(node
        .child_hashes
        .into_iter()
        .map(|hash| hash.to_vec())
        .collect())
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "state_hamt")?;
    child_module.add_function(wrap_pyfunction!(room_structural_key, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(build_root_handle, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_entries, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(node_child_hashes, &child_module)?)?;
    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.state_hamt", &child_module)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structural_key_is_deterministic() {
        let server_secret = [7u8; 32];
        let room_id = "!room:test.example";

        let key1 = room_structural_key_raw(&server_secret, room_id);
        let key2 = room_structural_key_raw(&server_secret, room_id);
        let other_key = room_structural_key_raw(&server_secret, "!other:test.example");

        assert_eq!(key1, key2);
        assert_ne!(key1, other_key);
        assert_eq!(key1.len(), 32);
    }

    #[test]
    fn build_root_handle_returns_root_and_persisted_nodes() {
        let server_secret = [11u8; 32];
        let room_id = "!room:test.example";
        let entries = vec![
            (
                "m.room.member".to_owned(),
                "@alice:test.example".to_owned(),
                "$1".to_owned(),
            ),
            ("m.room.name".to_owned(), "".to_owned(), "$2".to_owned()),
            ("m.room.topic".to_owned(), "".to_owned(), "$3".to_owned()),
        ];

        let ((structural_hash, state_group_id), nodes) =
            build_root_handle_and_nodes(&server_secret, room_id, entries)
                .expect("HAMT root should build");

        assert_eq!(structural_hash.len(), 16);
        assert_eq!(state_group_id.len(), 32);
        assert!(!nodes.is_empty());
        assert!(nodes
            .iter()
            .all(|(hash, bytes)| hash.len() == 16 && !bytes.is_empty()));
    }

    #[test]
    fn materialize_state_entries_roundtrips_root() {
        let server_secret = [11u8; 32];
        let room_id = "!room:test.example";
        let entries = vec![
            (
                "m.room.member".to_owned(),
                "@alice:test.example".to_owned(),
                "$1".to_owned(),
            ),
            ("m.room.name".to_owned(), "".to_owned(), "$2".to_owned()),
        ];

        let ((_, _), nodes) = build_root_handle_and_nodes(&server_secret, room_id, entries)
            .expect("HAMT root should build");

        let (root_hash, root_bytes) = nodes.last().cloned().expect("root node should exist");

        let recovered = materialize_state_entries(
            root_bytes,
            nodes
                .into_iter()
                .map(|(hash, bytes)| (hash.to_vec(), bytes))
                .collect(),
        )
        .expect("HAMT materialization should work");

        assert!(!recovered.is_empty());
        assert!(recovered
            .iter()
            .any(|(etype, state_key, event_id)| etype == "m.room.member"
                && state_key == "@alice:test.example"
                && event_id == "$1"));
        assert!(recovered.iter().any(|(_, _, event_id)| event_id == "$2"));
        assert_eq!(root_hash.len(), 16);
    }
}
