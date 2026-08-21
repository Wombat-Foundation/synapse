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
    node: Arc<HamtNode<u64, u64>>,
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

    let persisted: PersistedInternalNode<u64, u64> = node.as_ref().into();
    nodes.push((persisted.structural_hash, persisted.encode_v1()));
}

fn build_root_handle_and_nodes(
    server_secret: &[u8; 32],
    room_id: &str,
    entries: Vec<(u64, u64)>,
) -> Result<BuiltRoot, String> {
    let structural_key = room_structural_key_raw(server_secret, room_id);
    let lattice = LtHash::default();
    let (root_handle, root_node) =
        rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
            .map_err(|e| format!("Failed to build HAMT root: {e:?}"))?;

    let mut seen = HashSet::new();
    let mut nodes = Vec::new();
    collect_persisted_nodes(root_node, &mut seen, &mut nodes);

    Ok((root_handle_parts(&root_handle), nodes))
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
    entries: Vec<(u64, u64)>,
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

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "state_hamt")?;
    child_module.add_function(wrap_pyfunction!(room_structural_key, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(build_root_handle, &child_module)?)?;
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
        let entries = vec![(1, 1001), (2, 1002), (3, 1003)];

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
}
