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
type PyReachabilityAudit = (Vec<Vec<u8>>, Vec<Vec<u8>>);
type PyStateLookup = (Vec<PyStateEntry>, Vec<Vec<u8>>);

const TYPED_ROOT_FORMAT: u8 = 0x02;

/// The compact directory at the root of a typed state HAMT. The directory is
/// sorted by event type and points at one state_key -> event_id HAMT per type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TypedRoot {
    pub structural_hash: StructuralHash,
    pub directory: Vec<(String, StructuralHash)>,
}

impl TypedRoot {
    pub(crate) fn encode_v1(&self) -> Result<Vec<u8>, String> {
        let count = u16::try_from(self.directory.len())
            .map_err(|_| "typed root has too many event types".to_owned())?;
        let mut bytes = Vec::new();
        bytes.push(TYPED_ROOT_FORMAT);
        bytes.extend_from_slice(&self.structural_hash);
        bytes.extend_from_slice(&count.to_le_bytes());
        for (event_type, hash) in &self.directory {
            let event_type_bytes = event_type.as_bytes();
            let len = u16::try_from(event_type_bytes.len())
                .map_err(|_| "event type is too long for typed root".to_owned())?;
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(event_type_bytes);
            bytes.extend_from_slice(hash);
        }
        Ok(bytes)
    }

    pub(crate) fn decode_v1(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = 0usize;
        let take = |cursor: &mut usize, count: usize| -> Result<&[u8], String> {
            let end = cursor
                .checked_add(count)
                .ok_or_else(|| "typed root length overflow".to_owned())?;
            let value = bytes
                .get(*cursor..end)
                .ok_or_else(|| "truncated typed root".to_owned())?;
            *cursor = end;
            Ok(value)
        };
        if take(&mut cursor, 1)?[0] != TYPED_ROOT_FORMAT {
            return Err("not a typed HAMT root".to_owned());
        }
        let structural_hash: StructuralHash = take(&mut cursor, 16)?.try_into().unwrap();
        let count = u16::from_le_bytes(take(&mut cursor, 2)?.try_into().unwrap());
        let mut directory = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let len = u16::from_le_bytes(take(&mut cursor, 2)?.try_into().unwrap());
            let event_type = std::str::from_utf8(take(&mut cursor, len as usize)?)
                .map_err(|_| "typed root event type is not UTF-8".to_owned())?
                .to_owned();
            let hash: StructuralHash = take(&mut cursor, 16)?.try_into().unwrap();
            directory.push((event_type, hash));
        }
        if cursor != bytes.len() {
            return Err("trailing bytes in typed root".to_owned());
        }
        if directory.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err("typed root directory is not strictly sorted".to_owned());
        }
        Ok(Self {
            structural_hash,
            directory,
        })
    }
}

fn typed_subtree_key(room_key: &[u8; 32], event_type: &str) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(room_key).expect("HMAC key is valid");
    mac.update(b"typed-state-subtree:");
    mac.update(event_type.as_bytes());
    mac.finalize().into_bytes().into()
}

fn typed_root_hash(room_key: &[u8; 32], directory: &[(String, StructuralHash)]) -> StructuralHash {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(room_key).expect("HMAC key is valid");
    mac.update(b"typed-state-root:");
    for (event_type, hash) in directory {
        mac.update(&(event_type.len() as u32).to_le_bytes());
        mac.update(event_type.as_bytes());
        mac.update(hash);
    }
    let digest = mac.finalize().into_bytes();
    digest[..16].try_into().unwrap()
}

fn build_typed_root_and_nodes(
    server_secret: &[u8; 32],
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> Result<(TypedRoot, Vec<PersistedNodeBytes>), String> {
    let room_key = room_structural_key_raw(server_secret, room_id);
    let mut by_type: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for (event_type, state_key, event_id) in entries {
        by_type
            .entry(event_type)
            .or_default()
            .push((state_key, event_id));
    }

    let mut directory = Vec::with_capacity(by_type.len());
    let mut nodes = Vec::new();
    let mut seen = HashSet::new();
    for (event_type, type_entries) in by_type {
        let subtree_key = typed_subtree_key(&room_key, &event_type);
        let subtree = rezzy::hamt::build_hamt(&subtree_key, type_entries)
            .map_err(|e| format!("Failed to build typed HAMT subtree: {e:?}"))?;
        let subtree_hash = subtree.structural_hash;
        collect_persisted_nodes(subtree, &mut seen, &mut nodes);
        directory.push((event_type, subtree_hash));
    }
    let root = TypedRoot {
        structural_hash: typed_root_hash(&room_key, &directory),
        directory,
    };
    Ok((root, nodes))
}

#[must_use]
fn room_structural_key_raw(server_secret: &[u8; 32], room_id: &str) -> [u8; 32] {
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(server_secret).expect("HMAC can take key of any size");
    mac.update(room_id.as_bytes());
    mac.finalize().into_bytes().into()
}

/// Derive a fixed-width, room-scoped prefix used to lay out this room's HAMT
/// nodes contiguously in TiKV's flat sorted keyspace (see `tikv_engine.rs`).
///
/// For MSC4291-style room versions the room ID *is* `!` + base64url(hash(create
/// event)) -- already a uniformly-distributed digest -- so we decode it directly
/// rather than hashing it again. For pre-MSC4291 versions the room ID is an
/// opaque, low-entropy string (`!<random localpart>:<server_name>`), so we reuse
/// the same HMAC-SHA256 derivation as `room_structural_key_raw` and truncate.
///
/// Callers must pass the real `room_version.msc4291_room_ids_as_hashes` flag
/// (the official per-room-version marker) rather than comparing version
/// numbers, since which versions get hash-based room IDs is not simply "v12
/// and above" (e.g. experimental/Hydra versions).
pub(crate) fn room_tikv_prefix_raw(
    server_secret: &[u8; 32],
    room_id: &str,
    msc4291_room_ids_as_hashes: bool,
) -> Result<[u8; 8], String> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    const PREFIX_LEN: usize = 8;
    let mut prefix = [0u8; PREFIX_LEN];

    if msc4291_room_ids_as_hashes {
        let body = room_id.strip_prefix('!').unwrap_or(room_id);
        let decoded = URL_SAFE_NO_PAD
            .decode(body)
            .map_err(|e| format!("Failed to decode MSC4291 room id as base64url: {e}"))?;
        if decoded.len() < PREFIX_LEN {
            return Err(format!(
                "Decoded MSC4291 room id hash is only {} bytes, expected at least {}",
                decoded.len(),
                PREFIX_LEN
            ));
        }
        prefix.copy_from_slice(&decoded[..PREFIX_LEN]);
    } else {
        let full_key = room_structural_key_raw(server_secret, room_id);
        prefix.copy_from_slice(&full_key[..PREFIX_LEN]);
    }

    Ok(prefix)
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

pub(crate) fn decode_persisted_node(
    node_bytes: &[u8],
) -> Result<Arc<HamtNode<String, String>>, String> {
    let persisted = PersistedInternalNode::<String, String>::decode_v1(node_bytes)
        .map_err(|e| format!("Failed to decode persisted HAMT node: {e}"))?;
    let node: HamtNode<String, String> = persisted
        .try_into()
        .map_err(|e| format!("Failed to reconstruct HAMT node: {e}"))?;
    Ok(Arc::new(node))
}

/// Decode just enough of a persisted node to read its children's hashes,
/// without reconstructing the full node. Used to drive BFS traversal.
pub(crate) fn node_child_hashes_raw(node_bytes: &[u8]) -> Result<Vec<StructuralHash>, String> {
    let node = PersistedInternalNode::<String, String>::decode_v1(node_bytes)
        .map_err(|e| format!("Failed to decode persisted HAMT node: {e}"))?;
    Ok(node.child_hashes)
}

/// Walk a fully-resolved HAMT (every reachable node present in `node_map`)
/// starting at `root_hash`, emitting `(event_type, state_key, event_id)`
/// triples for every entry.
pub(crate) fn materialize_from_node_map(
    root_hash: &StructuralHash,
    node_map: &HashMap<StructuralHash, Arc<HamtNode<String, String>>>,
) -> Result<Vec<(String, String, String)>, String> {
    let root_node = node_map
        .get(root_hash)
        .cloned()
        .ok_or_else(|| format!("Missing persisted HAMT root node: {:02x?}", root_hash))?;

    let mut entries = Vec::new();
    let mut resolver = |hash: &StructuralHash| -> Result<Arc<HamtNode<String, String>>, String> {
        node_map
            .get(hash)
            .cloned()
            .ok_or_else(|| format!("Missing persisted HAMT node: {:02x?}", hash))
    };

    root_node.visit_entries(&mut resolver, &mut |key, value| {
        let (event_type, state_key): (String, String) = serde_json::from_str(key)
            .map_err(|e| format!("Failed to decode HAMT state key: {e}"))?;
        entries.push((event_type, state_key, value.clone()));
        Ok::<(), String>(())
    })?;

    Ok(entries)
}

pub(crate) fn lookup_from_node_map(
    root_hash: &StructuralHash,
    structural_key: &[u8],
    keys: &[(String, String)],
    node_map: &HashMap<StructuralHash, Arc<HamtNode<String, String>>>,
) -> Result<(Vec<PyStateEntry>, HashSet<StructuralHash>), String> {
    let root_node = node_map
        .get(root_hash)
        .cloned()
        .ok_or_else(|| format!("Missing persisted HAMT root node: {:02x?}", root_hash))?;
    let mut entries = Vec::new();
    let mut missing = HashSet::new();

    for (event_type, state_key) in keys {
        let key = serde_json::to_string(&(event_type, state_key))
            .map_err(|e| format!("Failed to encode HAMT state key: {e}"))?;
        let mut resolver = |hash: &StructuralHash| {
            node_map.get(hash).cloned().ok_or_else(|| {
                missing.insert(*hash);
            })
        };
        if let Ok(Some(event_id)) = root_node.search(structural_key, &key, &mut resolver) {
            entries.push((event_type.clone(), state_key.clone(), event_id));
        }
    }

    Ok((entries, missing))
}

fn structural_hash_from_bytes(hash_bytes: Vec<u8>) -> Result<StructuralHash, PyErr> {
    hash_bytes
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("structural hash must be 16 bytes"))
}

#[pyfunction]
#[pyo3(text_signature = "(server_secret, room_id, /)")]
pub fn room_structural_key(server_secret: Vec<u8>, room_id: &str) -> PyResult<Vec<u8>> {
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    Ok(room_structural_key_raw(&server_secret, room_id).to_vec())
}

/// See `room_tikv_prefix_raw` for the derivation. `msc4291_room_ids_as_hashes`
/// must come from the caller's real `RoomVersion.msc4291_room_ids_as_hashes`.
#[pyfunction]
#[pyo3(text_signature = "(server_secret, room_id, msc4291_room_ids_as_hashes, /)")]
pub fn room_tikv_prefix(
    server_secret: Vec<u8>,
    room_id: &str,
    msc4291_room_ids_as_hashes: bool,
) -> PyResult<Vec<u8>> {
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    room_tikv_prefix_raw(&server_secret, room_id, msc4291_room_ids_as_hashes)
        .map(|prefix| prefix.to_vec())
        .map_err(pyo3::exceptions::PyValueError::new_err)
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
#[pyo3(text_signature = "(server_secret, room_id, entries, /)")]
pub fn build_typed_root(
    server_secret: Vec<u8>,
    room_id: &str,
    entries: Vec<(String, String, String)>,
) -> PyResult<(Vec<u8>, Vec<u8>, Vec<PyPersistedNodeBytes>)> {
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    let (root, nodes) = build_typed_root_and_nodes(&server_secret, room_id, entries)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    let root_bytes = root
        .encode_v1()
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok((
        root.structural_hash.to_vec(),
        root_bytes,
        nodes
            .into_iter()
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(root_bytes, /)")]
pub fn decode_typed_root(root_bytes: Vec<u8>) -> PyResult<(Vec<u8>, Vec<(String, Vec<u8>)>)> {
    let root =
        TypedRoot::decode_v1(&root_bytes).map_err(pyo3::exceptions::PyValueError::new_err)?;
    Ok((
        root.structural_hash.to_vec(),
        root.directory
            .into_iter()
            .map(|(event_type, hash)| (event_type, hash.to_vec()))
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
    let root_hash = root_node.structural_hash;
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    node_map.insert(root_hash, root_node);

    for (hash_bytes, node_bytes) in nodes {
        let hash = structural_hash_from_bytes(hash_bytes)?;
        let node = decode_persisted_node(&node_bytes)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        node_map.insert(hash, node);
    }

    materialize_from_node_map(&root_hash, &node_map)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

#[pyfunction]
#[pyo3(text_signature = "(server_secret, room_id, root_node_bytes, nodes, keys, /)")]
pub fn lookup_state_entries(
    server_secret: Vec<u8>,
    room_id: &str,
    root_node_bytes: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    keys: Vec<(String, String)>,
) -> PyResult<PyStateLookup> {
    let server_secret: [u8; 32] = server_secret
        .try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("server_secret must be 32 bytes"))?;
    let structural_key = room_structural_key_raw(&server_secret, room_id);
    let root_node = decode_persisted_node(&root_node_bytes)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    let root_hash = root_node.structural_hash;
    let mut node_map = HashMap::from([(root_hash, root_node)]);
    for (hash_bytes, node_bytes) in nodes {
        let hash = structural_hash_from_bytes(hash_bytes)?;
        let node = decode_persisted_node(&node_bytes)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        node_map.insert(hash, node);
    }
    let (entries, missing) = lookup_from_node_map(&root_hash, &structural_key, &keys, &node_map)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok((
        entries,
        missing.into_iter().map(|hash| hash.to_vec()).collect(),
    ))
}

#[pyfunction]
#[pyo3(text_signature = "(node_bytes, /)")]
pub fn node_child_hashes(node_bytes: Vec<u8>) -> PyResult<Vec<Vec<u8>>> {
    let hashes =
        node_child_hashes_raw(&node_bytes).map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(hashes.into_iter().map(|hash| hash.to_vec()).collect())
}

#[pyfunction]
#[pyo3(text_signature = "(root_node_bytes, roots, universe, nodes, /)")]
pub fn reachability_audit(
    root_node_bytes: Vec<u8>,
    roots: Vec<Vec<u8>>,
    universe: Vec<Vec<u8>>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<PyReachabilityAudit> {
    let root_node = decode_persisted_node(&root_node_bytes)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    let root_hash = root_node.structural_hash;

    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    node_map.insert(root_hash, root_node);

    for (hash_bytes, node_bytes) in nodes {
        let hash = structural_hash_from_bytes(hash_bytes)?;
        let node = decode_persisted_node(&node_bytes)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        node_map.insert(hash, node);
    }

    let roots = roots
        .into_iter()
        .map(structural_hash_from_bytes)
        .map(|hash| {
            hash.and_then(|hash| {
                node_map.get(&hash).cloned().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Missing HAMT root node: {:02x?}",
                        hash
                    ))
                })
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let universe = universe
        .into_iter()
        .map(structural_hash_from_bytes)
        .collect::<PyResult<Vec<_>>>()?;

    let mut resolver = |hash: &StructuralHash| -> Result<Arc<HamtNode<String, String>>, String> {
        node_map
            .get(hash)
            .cloned()
            .ok_or_else(|| format!("Missing persisted HAMT node: {:02x?}", hash))
    };

    let audit = rezzy::hamt::bitmap_node_reachability_audit(roots, universe, &mut resolver)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e:?}")))?;
    let reachable = audit
        .reachable
        .iter()
        .map(|idx| {
            audit.universe.hash_at(idx).ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "HAMT bitmap audit returned out-of-range reachable index: {idx}"
                ))
            })
        })
        .map(|hash| hash.map(|hash| hash.to_vec()))
        .collect::<PyResult<Vec<_>>>()?;
    let unreachable = audit
        .unreachable
        .iter()
        .map(|idx| {
            audit.universe.hash_at(idx).ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "HAMT bitmap audit returned out-of-range unreachable index: {idx}"
                ))
            })
        })
        .map(|hash| hash.map(|hash| hash.to_vec()))
        .collect::<PyResult<Vec<_>>>()?;

    Ok((reachable, unreachable))
}

#[pyfunction]
#[pyo3(text_signature = "(root_node_bytes, roots, universe, nodes, /)")]
pub fn unreachable_node_hashes(
    root_node_bytes: Vec<u8>,
    roots: Vec<Vec<u8>>,
    universe: Vec<Vec<u8>>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<Vec<Vec<u8>>> {
    reachability_audit(root_node_bytes, roots, universe, nodes).map(|(_, unreachable)| unreachable)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "state_hamt")?;
    child_module.add_function(wrap_pyfunction!(room_structural_key, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(room_tikv_prefix, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(build_root_handle, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(build_typed_root, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(decode_typed_root, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_entries, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(lookup_state_entries, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(node_child_hashes, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(reachability_audit, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(unreachable_node_hashes, &child_module)?)?;
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
    fn room_tikv_prefix_is_deterministic_and_room_scoped() {
        let server_secret = [7u8; 32];
        let room_id = "!AbCdEfGhIjKlMnOpQr:test.example";
        let other_room_id = "!ZyXwVuTsRqPoNmLkJi:test.example";

        let prefix1 = room_tikv_prefix_raw(&server_secret, room_id, false).unwrap();
        let prefix2 = room_tikv_prefix_raw(&server_secret, room_id, false).unwrap();
        let other_prefix = room_tikv_prefix_raw(&server_secret, other_room_id, false).unwrap();

        assert_eq!(prefix1, prefix2);
        assert_ne!(prefix1, other_prefix);
        assert_eq!(prefix1.len(), 8);
    }

    #[test]
    fn room_tikv_prefix_decodes_msc4291_room_ids_without_rehashing() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

        let server_secret = [7u8; 32];
        // A 32-byte "create event reference hash", as msc4291 room IDs encode.
        let create_event_hash = [42u8; 32];
        let room_id = format!("!{}", URL_SAFE_NO_PAD.encode(create_event_hash));

        let prefix = room_tikv_prefix_raw(&server_secret, &room_id, true).unwrap();

        // The prefix should be exactly the leading 8 bytes of the decoded
        // hash -- i.e. derived directly from the room ID, not re-hashed
        // through the (server-secret-salted) v1-v11 path.
        assert_eq!(prefix, create_event_hash[..8]);
    }

    #[test]
    fn room_tikv_prefix_rejects_invalid_msc4291_room_id() {
        let server_secret = [7u8; 32];
        assert!(room_tikv_prefix_raw(&server_secret, "!not-valid-base64!!!", true).is_err());
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
    fn typed_root_roundtrips_sorted_directory() {
        let server_secret = [11u8; 32];
        let entries = vec![
            (
                "m.room.member".to_owned(),
                "@alice:test".to_owned(),
                "$1".to_owned(),
            ),
            ("m.room.create".to_owned(), "".to_owned(), "$2".to_owned()),
        ];
        let (root, nodes) = build_typed_root_and_nodes(&server_secret, "!room:test", entries)
            .expect("typed root should build");
        assert_eq!(root.directory[0].0, "m.room.create");
        assert_eq!(root.directory[1].0, "m.room.member");
        assert!(!nodes.is_empty());
        let encoded = root.encode_v1().expect("typed root should encode");
        assert_eq!(
            TypedRoot::decode_v1(&encoded).expect("typed root should decode"),
            root
        );
        assert_eq!(encoded[0], TYPED_ROOT_FORMAT);
    }

    #[test]
    fn typed_root_rejects_unsorted_directory() {
        let root = TypedRoot {
            structural_hash: [0u8; 16],
            directory: vec![("z".to_owned(), [1u8; 16]), ("a".to_owned(), [2u8; 16])],
        };
        let mut encoded = vec![TYPED_ROOT_FORMAT];
        encoded.extend_from_slice(&root.structural_hash);
        encoded.extend_from_slice(&2u16.to_le_bytes());
        for (event_type, hash) in root.directory {
            encoded.extend_from_slice(&(event_type.len() as u16).to_le_bytes());
            encoded.extend_from_slice(event_type.as_bytes());
            encoded.extend_from_slice(&hash);
        }
        assert!(TypedRoot::decode_v1(&encoded).is_err());
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

    #[test]
    fn lookup_state_entries_fetches_only_requested_paths() {
        let server_secret = [11u8; 32];
        let room_id = "!room:test.example";
        let entries = (0..1_000)
            .map(|i| {
                (
                    "m.room.member".to_owned(),
                    format!("@user-{i}:test.example"),
                    format!("${i}"),
                )
            })
            .collect::<Vec<_>>();
        let ((root_hash, _), nodes) = build_root_handle_and_nodes(&server_secret, room_id, entries)
            .expect("HAMT root should build");
        let all_nodes = nodes
            .into_iter()
            .map(|(hash, bytes)| {
                decode_persisted_node(&bytes)
                    .map(|node| (hash, node))
                    .expect("node should decode")
            })
            .collect::<HashMap<_, _>>();
        let root_only = HashMap::from([(
            root_hash,
            all_nodes
                .get(&root_hash)
                .cloned()
                .expect("root should exist"),
        )]);
        let keys = vec![
            (
                "m.room.member".to_owned(),
                "@user-42:test.example".to_owned(),
            ),
            (
                "m.room.member".to_owned(),
                "@missing:test.example".to_owned(),
            ),
        ];
        let structural_key = room_structural_key_raw(&server_secret, room_id);

        let (partial, missing) =
            lookup_from_node_map(&root_hash, &structural_key, &keys, &root_only)
                .expect("partial lookup should identify missing nodes");
        assert!(partial.is_empty());
        assert!(!missing.is_empty());
        assert!(missing.len() < all_nodes.len());

        let (found, missing) = lookup_from_node_map(&root_hash, &structural_key, &keys, &all_nodes)
            .expect("complete lookup should work");
        assert!(missing.is_empty());
        assert_eq!(
            found,
            vec![(
                "m.room.member".to_owned(),
                "@user-42:test.example".to_owned(),
                "$42".to_owned()
            )]
        );
    }

    #[test]
    fn unreachable_node_hashes_reports_orphan_root() {
        let server_secret = [11u8; 32];
        let room_id = "!room:test.example";
        let live_entries = vec![("m.room.name".to_owned(), "".to_owned(), "$live".to_owned())];
        let orphan_entries = vec![(
            "m.room.topic".to_owned(),
            "".to_owned(),
            "$orphan".to_owned(),
        )];

        let ((_, _), live_nodes) =
            build_root_handle_and_nodes(&server_secret, room_id, live_entries)
                .expect("live HAMT root should build");
        let ((_, _), orphan_nodes) =
            build_root_handle_and_nodes(&server_secret, room_id, orphan_entries)
                .expect("orphan HAMT root should build");

        let (live_root_hash, live_root_bytes) = live_nodes
            .last()
            .cloned()
            .expect("live root node should exist");
        let (orphan_root_hash, _) = orphan_nodes
            .last()
            .cloned()
            .expect("orphan root node should exist");
        let all_nodes: Vec<_> = live_nodes
            .into_iter()
            .chain(orphan_nodes)
            .map(|(hash, bytes)| (hash.to_vec(), bytes))
            .collect();
        let universe: Vec<_> = all_nodes.iter().map(|(hash, _)| hash.clone()).collect();

        let (reachable, unreachable) = reachability_audit(
            live_root_bytes,
            vec![live_root_hash.to_vec()],
            universe,
            all_nodes,
        )
        .expect("reachability audit should succeed");

        assert!(reachable.contains(&live_root_hash.to_vec()));
        assert!(!reachable.contains(&orphan_root_hash.to_vec()));
        assert!(unreachable.contains(&orphan_root_hash.to_vec()));
        assert!(!unreachable.contains(&live_root_hash.to_vec()));
    }
}
