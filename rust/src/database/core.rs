//! Generic HAMT node-store logic shared by every embedded single-process KV
//! backend ([`crate::fjall_engine`], [`crate::mdbx_engine`]). Each backend
//! implements only [`NodeStore`] (a thin point-lookup/write surface over its
//! own storage primitive) and owns its own process-global handle + node
//! cache; the BFS materialize/selective-lookup walk, the node-cache
//! verify-on-hit logic, and the key-encoding scheme live here exactly once.
//!
//! `synapse/storage/databases/state/store.py` picks which backend module to
//! call based on config (`embedded_hamt_engine = "fjall" | "mdbx"`); both
//! expose an identical Python surface (see each module's `.pyi` stub) so the
//! choice is a drop-in swap at the call site.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use lru::LruCache;
use rezzy::hamt::{HamtNode, StructuralHash};
use sha2::{Digest, Sha256};

use crate::state_hamt::{
    decode_persisted_node_verified, lookup_from_node_map, materialize_from_node_map,
};

/// Minimal point-lookup/write surface every embedded HAMT KV backend must
/// provide. Prefix scan / open / register-module stay backend-specific
/// (each engine module exposes its own `scan_prefix`, `open_client`, etc.)
/// since those aren't used by the generic BFS walk below.
pub trait NodeStore {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;
}

/// Fixed width of the room-scoped key prefix -- a locality hint, not an
/// identity: the full key is always `prefix || structural_hash`, which
/// stays unique regardless of prefix collisions.
pub const ROOM_PREFIX_LEN: usize = 8;

/// One materialized state group: `(event_type, state_key, event_id)` triples.
pub type StateEntries = Vec<(String, String, String)>;

pub type NodeLocation = ([u8; ROOM_PREFIX_LEN], [u8; 32], StructuralHash);
pub type SelectiveQuery = (
    [u8; ROOM_PREFIX_LEN],
    StructuralHash,
    [u8; 32],
    Vec<(String, String)>,
);

/// Process-wide in-memory cache of decoded HAMT nodes, keyed by their full
/// (namespaced, room-prefixed) storage key. HAMT nodes are immutable and
/// content-addressed, so a cache hit is always correct modulo the
/// structural-hash check performed on every hit.
const NODE_CACHE_CAPACITY: usize = 100_000;
pub type NodeCache = Mutex<LruCache<Vec<u8>, Arc<HamtNode<String, String>>>>;

pub fn new_node_cache() -> NodeCache {
    Mutex::new(LruCache::new(
        NonZeroUsize::new(NODE_CACHE_CAPACITY).expect("cache capacity is nonzero"),
    ))
}

/// Namespaced, room-prefixed HAMT node key:
/// `hamt:node:<namespace_hash>:<room_prefix_hex>:<structural_hash_hex>`.
pub fn node_key(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    structural_hash: &StructuralHash,
) -> Vec<u8> {
    let namespace_hash = Sha256::digest(namespace.as_bytes());
    let mut key = Vec::with_capacity(10 + 32 + 1 + ROOM_PREFIX_LEN * 2 + 1 + 32);
    key.extend_from_slice(b"hamt:node:");
    key.extend_from_slice(hex::encode(&namespace_hash[..16]).as_bytes());
    key.push(b':');
    key.extend_from_slice(hex::encode(room_prefix).as_bytes());
    key.push(b':');
    key.extend_from_slice(hex::encode(structural_hash).as_bytes());
    key
}

/// Encodes a batch of `(structural_hash, node_bytes)` pairs (the shape
/// `state_hamt.build_root_handle_with_lattice`/`apply_flat_state_updates`
/// return) into `(node_key, node_bytes)` pairs ready for `batch_put` --
/// shared by both engines' `put_state_hamt_nodes` so callers never write
/// under a raw structural_hash key (which the BFS walk above can't find,
/// since it always looks up the namespaced/room-prefixed key).
pub fn encode_node_writes(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    nodes: Vec<(StructuralHash, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    nodes
        .into_iter()
        .map(|(hash, bytes)| (node_key(namespace, room_prefix, &hash), bytes))
        .collect()
}

/// Batch size while walking the HAMT -- bounds how much of the node cache's
/// lock is held at once per round.
const NODE_FETCH_BATCH_SIZE: usize = 100;

/// Fetch a state group's HAMT: BFS-fetch the reachable nodes (batched),
/// decode each one, and materialize `(event_type, state_key, event_id)`
/// triples -- without crossing back into Python per node.
pub fn materialize_state_hamt(
    store: &dyn NodeStore,
    cache: &NodeCache,
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    root_structural_hash: StructuralHash,
    structural_key: &[u8; 32],
) -> Result<StateEntries, String> {
    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);
    let mut to_fetch: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);

    while !to_fetch.is_empty() {
        let current_batch: Vec<StructuralHash> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for hash in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *hash));
                            } else {
                                let node = node.clone();
                                for child in &node.children {
                                    let child_hash = child.structural_hash();
                                    if seen.insert(child_hash) {
                                        to_fetch.insert(child_hash);
                                    }
                                }
                                node_map.insert(*hash, node);
                            }
                        }
                        None => still_missing.push((key, *hash)),
                    }
                }
            }

            for (key, expected_hash) in still_missing {
                let node_bytes = store
                    .get_raw(&key)?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, structural_key, expected_hash)?;

                for child in &node.children {
                    let child_hash = child.structural_hash();
                    if seen.insert(child_hash) {
                        to_fetch.insert(child_hash);
                    }
                }

                cache.lock().unwrap().put(key, node.clone());
                node_map.insert(expected_hash, node);
            }
        }
    }

    materialize_from_node_map(&root_structural_hash, &node_map)
}

pub fn materialize_state_hamts(
    store: &dyn NodeStore,
    cache: &NodeCache,
    namespace: &str,
    roots: Vec<NodeLocation>,
) -> Result<Vec<StateEntries>, String> {
    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = roots.iter().copied().collect();
    let mut to_fetch = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *room_prefix, *structural_key, *hash));
                            } else {
                                let node = node.clone();
                                for child in &node.children {
                                    let child_location =
                                        (*room_prefix, *structural_key, child.structural_hash());
                                    if seen.insert(child_location) {
                                        to_fetch.insert(child_location);
                                    }
                                }
                                node_map.insert((*room_prefix, *structural_key, *hash), node);
                            }
                        }
                        None => still_missing.push((key, *room_prefix, *structural_key, *hash)),
                    }
                }
            }

            for (key, room_prefix, structural_key, expected_hash) in still_missing {
                let node_bytes = store
                    .get_raw(&key)?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, &structural_key, expected_hash)?;

                for child in &node.children {
                    let child_location = (room_prefix, structural_key, child.structural_hash());
                    if seen.insert(child_location) {
                        to_fetch.insert(child_location);
                    }
                }

                cache.lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, structural_key, expected_hash), node);
            }
        }
    }

    type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
    let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
    for ((room_prefix, _, hash), node) in node_map {
        nodes_by_prefix
            .entry(room_prefix)
            .or_default()
            .insert(hash, node);
    }

    roots
        .into_iter()
        .map(|(room_prefix, _, root_hash)| {
            let nodes = nodes_by_prefix.get(&room_prefix).ok_or_else(|| {
                format!(
                    "Missing nodes for room prefix: {}",
                    hex::encode(room_prefix)
                )
            })?;
            materialize_from_node_map(&root_hash, nodes)
        })
        .collect()
}

pub fn lookup_state_hamts(
    store: &dyn NodeStore,
    cache: &NodeCache,
    namespace: &str,
    queries: Vec<SelectiveQuery>,
) -> Result<Vec<StateEntries>, String> {
    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = queries.iter().map(|(p, h, k, _)| (*p, *k, *h)).collect();
    let mut to_fetch: HashSet<NodeLocation> = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        for chunk in current_batch.chunks(NODE_FETCH_BATCH_SIZE) {
            let mut still_missing = Vec::with_capacity(chunk.len());
            {
                let mut cache = cache.lock().unwrap();
                for (room_prefix, structural_key, hash) in chunk {
                    let key = node_key(namespace, room_prefix, hash);
                    match cache.get(&key) {
                        Some(node) => {
                            if node.structural_hash != *hash {
                                cache.pop(&key);
                                still_missing.push((key, *room_prefix, *structural_key, *hash));
                            } else {
                                node_map
                                    .insert((*room_prefix, *structural_key, *hash), node.clone());
                            }
                        }
                        None => still_missing.push((key, *room_prefix, *structural_key, *hash)),
                    }
                }
            }

            for (key, room_prefix, structural_key, expected_hash) in still_missing {
                let node_bytes = store
                    .get_raw(&key)?
                    .ok_or_else(|| "Missing HAMT node".to_owned())?;
                let node =
                    decode_persisted_node_verified(&node_bytes, &structural_key, expected_hash)?;

                cache.lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, structural_key, expected_hash), node);
            }
        }

        type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
        let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
        for ((room_prefix, _, hash), node) in &node_map {
            nodes_by_prefix
                .entry(*room_prefix)
                .or_default()
                .insert(*hash, Arc::clone(node));
        }

        for (room_prefix, root_hash, structural_key, keys) in &queries {
            if let Some(prefix_nodes) = nodes_by_prefix.get(room_prefix) {
                if prefix_nodes.contains_key(root_hash) {
                    let (_entries, missing) =
                        lookup_from_node_map(root_hash, structural_key, keys, prefix_nodes)?;
                    for missing_hash in missing {
                        let child_loc = (*room_prefix, *structural_key, missing_hash);
                        if seen.insert(child_loc) {
                            to_fetch.insert(child_loc);
                        }
                    }
                }
            }
        }
    }

    type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
    let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
    for ((room_prefix, _, hash), node) in node_map {
        nodes_by_prefix
            .entry(room_prefix)
            .or_default()
            .insert(hash, node);
    }

    queries
        .into_iter()
        .map(|(room_prefix, root_hash, structural_key, keys)| {
            let prefix_nodes = nodes_by_prefix.get(&room_prefix).ok_or_else(|| {
                format!(
                    "Missing nodes for room prefix: {}",
                    hex::encode(room_prefix)
                )
            })?;
            let (entries, missing) =
                lookup_from_node_map(&root_hash, &structural_key, &keys, prefix_nodes)?;
            if !missing.is_empty() {
                return Err(format!(
                    "Unresolved missing nodes after fetch loop for root {:02x?}",
                    root_hash
                ));
            }
            Ok(entries)
        })
        .collect()
}
