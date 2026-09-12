use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use mtxdb::{DatabaseLayout, NodeData, NodeId, PackfileStorage, ShardType, StorageEngine};
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use sha2::{Digest, Sha256};

use crate::database::core::{NodeStore, ROOM_PREFIX_LEN};

struct MtxdbPools {
    state: Arc<dyn StorageEngine>,
    event_dag: Arc<dyn StorageEngine>,
    auth_chain: Arc<dyn StorageEngine>,
}

/// Base directory for the state_group -> room_prefix room-index files (see
/// `room_index` module below), set once by `open_client`.
static ROOM_INDEX_DIR: OnceCell<std::path::PathBuf> = OnceCell::new();

static DBS: OnceCell<MtxdbPools> = OnceCell::new();
/// Serialize all read-modify-write cycles through the embedded engine.
/// The mtxdb `StorageEngine` trait has no atomic increment or transaction API,
/// so we hold this across get→put_many for counters and auth-chain manifests.
/// Only one SQL transaction runs at a time via the DB pool, so contention is
/// negligible.
static RMW_LOCK: Mutex<()> = Mutex::new(());

fn pools() -> PyResult<&'static MtxdbPools> {
    DBS.get()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mtxdb not opened"))
}

fn state_db() -> PyResult<&'static Arc<dyn StorageEngine>> {
    Ok(&pools()?.state)
}

fn event_dag_db() -> PyResult<&'static Arc<dyn StorageEngine>> {
    Ok(&pools()?.event_dag)
}

fn auth_chain_db() -> PyResult<&'static Arc<dyn StorageEngine>> {
    Ok(&pools()?.auth_chain)
}

fn shard_type_for_key(key: &[u8]) -> ShardType {
    if key.starts_with(b"event_json:") || key.starts_with(b"prev_event_edges:") {
        ShardType::EventDag
    } else {
        ShardType::State
    }
}

fn db_for_shard_type(shard_type: ShardType) -> PyResult<&'static Arc<dyn StorageEngine>> {
    match shard_type {
        ShardType::State => state_db(),
        ShardType::EventDag => event_dag_db(),
        ShardType::AuthChain => auth_chain_db(),
    }
}

// -----------------------------------------------------------------------------
// HAMT Node Mapping
// -----------------------------------------------------------------------------

/// Extracts the room prefix and structural hash from a full node key.
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

/// Derive a HAMT root's `NodeId` within its room's own State collection --
/// a fixed `b"hamt:root:"` tag plus the namespace (still needed here: a
/// room's own collection is otherwise keyed only by the bare `state_group`
/// int, and different trial-test namespaces sharing one physical mtxdb
/// store can otherwise collide on the same state_group id -- see
/// tests/utils.py's default_config) plus the state_group, hashed and
/// truncated the same way kv_node_id/chain_node_id already derive node
/// ids from non-hash-shaped keys. Distinct from any real structural-hash
/// node id in that collection with overwhelming probability, the same
/// margin already relied on for kv_node_id's own use.
fn root_node_id(namespace: &str, state_group: i64) -> [u8; 16] {
    let mut buf = Vec::with_capacity(b"hamt:root:".len() + namespace.len() + 8);
    buf.extend_from_slice(b"hamt:root:");
    buf.extend_from_slice(namespace.as_bytes());
    buf.extend_from_slice(&state_group.to_be_bytes());
    let hash = Sha256::digest(&buf);
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn room_id_from_prefix(room_prefix: &[u8]) -> [u8; 16] {
    let mut room_id = [0u8; 16];
    let prefix_len = std::cmp::min(room_prefix.len(), 16);
    room_id[..prefix_len].copy_from_slice(&room_prefix[..prefix_len]);
    room_id
}

/// Store HAMT root records in their room's own State collection, rather
/// than the single global flat-KV collection every other caller shares
/// (`kv_room_id()`). A root write/read is naturally room-scoped -- every
/// caller either already has `room_prefix` in hand or can derive it
/// cheaply (`state_hamt.room_hamt_prefix`) -- and mtxdb clones a
/// collection's entire index on every write, so parking every room's
/// roots in one shared collection meant a single root write's clone cost
/// scaled with the *server's total* accumulated root count instead of one
/// room's, growing without bound as a trial run persists more rooms. This
/// mirrors `put_state_hamt_nodes`'s existing per-room routing exactly.
#[pyfunction]
pub fn put_state_hamt_roots(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    roots: Vec<(i64, Vec<u8>)>,
) -> PyResult<()> {
    let room_id = room_id_from_prefix(&room_prefix);
    let pairs: Vec<(NodeId, NodeData)> = roots
        .into_iter()
        .map(|(state_group, value)| {
            (
                root_node_id(&namespace, state_group),
                NodeData::new(bytes::Bytes::from(value)),
            )
        })
        .collect();
    py.detach(|| {
        let engine = state_db()?;
        engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })
    })
}

/// Tombstone HAMT root records in their room's own State collection --
/// the delete counterpart of `put_state_hamt_roots`, needed by
/// `purge_unreferenced_state_groups` and the root-deletion-queue drain.
/// mtxdb-core's `StorageEngine` trait exposes no per-key delete (only
/// `delete_collection`, a whole-collection range delete, which would drop
/// every other state_group's root in the room too) -- but `batch_delete`
/// on the old global collection was never anything more than `put_many`
/// with an empty `NodeData` value, the same "empty means absent" tombstone
/// convention `get_state_hamt_roots_for_room` and `batch_get` already
/// check for. Reuse that convention here instead of adding a new
/// mtxdb-core primitive. The corresponding room_index entry is left in
/// place: a tombstoned root reads back as a miss regardless, and
/// `state_group` ids are never reused (see the `room_index` module doc),
/// so the stale mapping is never looked up in a way that matters.
#[pyfunction]
pub fn delete_state_hamt_roots_for_room(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    state_groups: Vec<i64>,
) -> PyResult<()> {
    let room_id = room_id_from_prefix(&room_prefix);
    let pairs: Vec<(NodeId, NodeData)> = state_groups
        .into_iter()
        .map(|state_group| {
            (
                root_node_id(&namespace, state_group),
                NodeData::new(bytes::Bytes::new()),
            )
        })
        .collect();
    py.detach(|| {
        let engine = state_db()?;
        engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb delete error: {}", e))
        })
    })
}

/// Point/batch-read HAMT root records from their room's own State
/// collection -- the read counterpart of `put_state_hamt_roots`. Returns
/// the raw encoded root value (as written by `_encode_state_hamt_root`)
/// for each `state_group`, or `None` on a miss; decoding stays in Python
/// (`_decode_state_hamt_root`) rather than duplicating that format here.
#[pyfunction]
pub fn get_state_hamt_roots_for_room(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    state_groups: Vec<i64>,
) -> PyResult<Vec<Option<Vec<u8>>>> {
    let room_id = room_id_from_prefix(&room_prefix);
    let node_ids: Vec<NodeId> = state_groups
        .iter()
        .map(|&sg| root_node_id(&namespace, sg))
        .collect();
    py.detach(|| {
        let engine = state_db()?;
        let results = engine.get_many(&room_id, &node_ids).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {}", e))
        })?;
        Ok(results
            .into_iter()
            .map(|res| {
                res.and_then(|data| {
                    if data.bytes.is_empty() {
                        None
                    } else {
                        Some(data.bytes.to_vec())
                    }
                })
            })
            .collect())
    })
}

/// A flat, direct-offset `state_group -> room_prefix` index: entry N lives
/// at byte offset `N * 16` in a per-namespace file under
/// `<embedded_hamt_path>/room_index/<namespace_hash>.bin`. Exists so
/// `_fetch_hamt_roots_for_embedded_txn` (bg_updates.py) -- which only ever
/// has a bare `state_group` int, by design, and needs to resolve which
/// room's mtxdb collection to look a root up in -- doesn't have to touch
/// SQL or a shared PackfileStorage collection (whose per-write clone cost
/// scales with the server's *total* state-group count, the exact problem
/// this index exists to avoid). `state_group` is a small, dense, sequential
/// integer (not a content hash), so direct offset addressing needs no hash
/// table, no clone, and no rebuild -- write and read are both a single
/// syscall at a computed offset.
///
/// Three invariants this design depends on, stated explicitly:
///
/// 1. **Concurrent multi-worker writes need no locking.** Different
///    workers write disjoint `state_group` ids (ids are allocated once,
///    server-wide, by `_state_group_seq_gen`, never reused), so their
///    `pwrite`s land at disjoint, non-overlapping byte ranges. POSIX
///    requires no coordination between writers of non-overlapping regions
///    of the same regular file. Unlike `ShardPool`, this file needs no
///    `WriterLock`: two writers can never target the same offset.
/// 2. **All-zero is a valid "not (yet) written" sentinel, not ambiguous
///    with a real value.** A real `room_prefix` is derived from a hashed
///    `room_id` (`state_hamt.room_hamt_prefix`), so the chance of a
///    genuine value being 16 zero bytes is negligible (~2^-128) -- the
///    same margin already relied on elsewhere in this file (`kv_node_id`,
///    `chain_node_id`) for hash-derived ids. A reader that sees all-zero
///    (sparse-file default, or a `pwrite` mid-flight and not yet
///    reflected) treats it as a miss. `pwrite`/`pread` of one 16-byte
///    value, well within a single page, is applied atomically at the
///    page-cache level on Linux -- a concurrent reader observes either the
///    complete old or complete new value, never a torn mix.
/// 3. **This index has the same bounded durability window as the rest of
///    the embedded engine, not a weaker one.** There is no per-write
///    fsync here (matching the engine-wide move away from per-write
///    fsyncs -- see `_periodic_embedded_sync`): a crash can lose a very
///    recent mapping along with the root record it points to, since both
///    are written in the same uncommitted window. That's consistent, not
///    a new gap -- the root itself has no stronger guarantee in that same
///    window. A resulting miss for a group still inside the
///    `EMBEDDED_HAMT_MIGRATION_UPDATE_NAME` window falls through to
///    `_fetch_hamt_roots_for_embedded_txn`'s SQL fallback, same as a
///    genuine migration-window miss.
///
/// **Outside that window, a miss does not degrade gracefully.** A
/// missing or stale entry for an already-migrated, already-backfilled
/// `state_group` is not a slower read and does not self-heal on the next
/// read -- `_fetch_hamt_roots_for_embedded_txn` returns it as missing,
/// and `_get_state_groups_from_groups_txn` (store.py) then raises
/// `RuntimeError("State group(s) exist in SQL but have no HAMT root:
/// ...")`, since a group with a SQL `state_groups` row and no HAMT root
/// is treated as data corruption once both background updates have
/// finished. This fails loud, not silently, but it is a hard failure,
/// not a fallback. The only way an entry gets (re)written is a write
/// through `put_room_index` (called from
/// `_store_state_hamt_root_embedded_txn`) or a fresh run of the
/// `state_hamt_backfill_roots` background update -- never a plain read.
/// Anything that deletes or reinterprets this directory's files (e.g. a
/// change to `RECORD_LEN`/the on-disk record layout, applied in place
/// against files written under the old layout) hits this same failure
/// mode for every group it doesn't happen to already cover.
mod room_index {
    use std::collections::HashMap;
    use std::fs::{File, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::sync::{Arc, Mutex};

    use pyo3::PyResult;
    use sha2::{Digest, Sha256};

    use super::{ROOM_INDEX_DIR, ROOM_PREFIX_LEN};

    // A real `room_prefix` is always exactly `ROOM_PREFIX_LEN` (8) bytes --
    // `room_hamt_prefix_raw` truncates to that length unconditionally in
    // both its branches (MSC4291 hash-derived and legacy). This used to be
    // hardcoded to 16, silently zero-padding every stored value; `get_many`
    // returned that padding along with the real prefix, and every reader
    // (`lookup_state_hamts` et al.) enforces the true 8-byte length, so a
    // padded value failed downstream with "room_prefix must be 8 bytes".
    const RECORD_LEN: u64 = ROOM_PREFIX_LEN as u64;

    /// Cached file handles, one per namespace, so a batch of N `put`/`get`
    /// calls (e.g. one per state group in a persist loop) pays one `open()`
    /// for the whole batch rather than one per call -- the same overhead
    /// this index otherwise avoids by skipping mtxdb's clone-on-write
    /// entirely. `FileExt::write_at`/`read_at` (pread/pwrite) take an
    /// explicit offset per call, so a shared handle needs no seek and no
    /// per-call mutable state -- safe to hand out from behind a `Mutex`
    /// that's only ever held for the duration of a HashMap lookup/insert,
    /// never across the actual I/O.
    static HANDLES: Mutex<Option<HashMap<String, std::sync::Arc<File>>>> = Mutex::new(None);

    fn index_path(namespace: &str) -> PyResult<std::path::PathBuf> {
        let dir = ROOM_INDEX_DIR
            .get()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("mtxdb not opened"))?;
        std::fs::create_dir_all(dir).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to create room_index dir: {e}"
            ))
        })?;
        let namespace_hash = Sha256::digest(namespace.as_bytes());
        Ok(dir.join(format!("{}.bin", hex::encode(&namespace_hash[..16]))))
    }

    fn cached_handle(namespace: &str, create: bool) -> PyResult<Option<std::sync::Arc<File>>> {
        let mut guard = HANDLES
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?;
        let map = guard.get_or_insert_with(HashMap::new);
        if let Some(file) = map.get(namespace) {
            return Ok(Some(Arc::clone(file)));
        }
        let path = index_path(namespace)?;
        // `write(true)` unconditionally: the cached handle is shared for
        // the namespace's lifetime, so a `get_many`-first ordering must
        // not poison it read-only for every later `put` -- only whether a
        // *missing* file gets created (`create`) should depend on which
        // call warmed the cache.
        let opened = OpenOptions::new()
            .create(create)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path);
        let file = match opened {
            Ok(f) => f,
            Err(e) if !create && e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to open room_index file: {e}"
                )))
            }
        };
        let file = Arc::new(file);
        map.insert(namespace.to_string(), Arc::clone(&file));
        Ok(Some(file))
    }

    pub fn put(namespace: &str, entries: &[(i64, Vec<u8>)]) -> PyResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let file = cached_handle(namespace, true)?.expect("create=true never returns None");
        for (state_group, room_prefix) in entries {
            let mut record = [0u8; RECORD_LEN as usize];
            let n = std::cmp::min(room_prefix.len(), RECORD_LEN as usize);
            record[..n].copy_from_slice(&room_prefix[..n]);
            let offset = (*state_group as u64).saturating_mul(RECORD_LEN);
            file.write_at(&record, offset).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("room_index write failed: {e}"))
            })?;
        }
        Ok(())
    }

    pub fn get_many(namespace: &str, state_groups: &[i64]) -> PyResult<Vec<Option<Vec<u8>>>> {
        let Some(file) = cached_handle(namespace, false)? else {
            return Ok(vec![None; state_groups.len()]);
        };
        let mut out = Vec::with_capacity(state_groups.len());
        for &state_group in state_groups {
            let offset = (state_group as u64).saturating_mul(RECORD_LEN);
            let mut record = [0u8; RECORD_LEN as usize];
            let value = match file.read_exact_at(&mut record, offset) {
                Ok(()) if record.iter().any(|&b| b != 0) => Some(record.to_vec()),
                _ => None, // all-zero (sentinel) or short read past EOF: miss.
            };
            out.push(value);
        }
        Ok(out)
    }

    /// Flush all cached room-index handles to disk. Mirrors the bounded,
    /// periodic (not per-write) durability window the rest of the embedded
    /// engine uses -- see `_periodic_embedded_sync` on the Python side,
    /// which calls this via the top-level `sync()` pyfunction. Without
    /// this, room-index writes had no fsync point at all (unbounded loss
    /// window on crash, not just the same ~1s one as everything else); a
    /// miss still falls through to the SQL fallback in
    /// `_fetch_hamt_roots_for_embedded_txn`, so this narrows an existing
    /// gap rather than being the only thing standing between a crash and
    /// data loss.
    pub fn sync() -> PyResult<()> {
        let guard = HANDLES
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {e}")))?;
        let Some(map) = guard.as_ref() else {
            return Ok(());
        };
        for file in map.values() {
            file.sync_data().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("room_index sync failed: {e}"))
            })?;
        }
        Ok(())
    }
}

#[pyfunction]
pub fn put_room_index(namespace: String, entries: Vec<(i64, Vec<u8>)>) -> PyResult<()> {
    room_index::put(&namespace, &entries)
}

#[pyfunction]
pub fn get_room_index(namespace: String, state_groups: Vec<i64>) -> PyResult<Vec<Option<Vec<u8>>>> {
    room_index::get_many(&namespace, &state_groups)
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
            // Treat empty-byte tombstones (from batch_delete) as absent.
            Ok(result.and_then(|data| {
                if data.bytes.is_empty() {
                    None
                } else {
                    Some(data.bytes.to_vec())
                }
            }))
        } else {
            // Hamt roots live in the state pool's flat-KV namespace.
            let room_id = kv_room_id();
            let node_id = kv_node_id(key);
            let result = self
                .engine
                .get(&room_id, &node_id)
                .map_err(|e| e.to_string())?;
            match result {
                None => Ok(None),
                Some(data) if data.bytes.is_empty() => Ok(None),
                Some(data) => Ok(Some(data.bytes.to_vec())),
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Auth Chain Manifests
// -----------------------------------------------------------------------------

fn namespace_room_id(namespace: &str) -> [u8; 16] {
    let hash = Sha256::digest(namespace.as_bytes());
    let mut room_id = [0u8; 16];
    room_id.copy_from_slice(&hash[..16]);
    room_id
}

fn chain_node_id(chain_id: i64) -> [u8; 16] {
    let hash = Sha256::digest(chain_id.to_be_bytes());
    let mut node_id = [0u8; 16];
    node_id.copy_from_slice(&hash[..16]);
    node_id
}

fn serialize_manifest(edges: &[(i64, i64, i64)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + edges.len() * 24);
    buf.extend_from_slice(&(edges.len() as u32).to_be_bytes());
    for &(o_seq, t_chain, t_seq) in edges {
        buf.extend_from_slice(&o_seq.to_be_bytes());
        buf.extend_from_slice(&t_chain.to_be_bytes());
        buf.extend_from_slice(&t_seq.to_be_bytes());
    }
    buf
}

fn deserialize_manifest(bytes: &[u8]) -> Vec<(i64, i64, i64)> {
    if bytes.len() < 4 {
        return Vec::new();
    }
    let count = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut edges = Vec::with_capacity(count);
    let mut offset = 4;
    for _ in 0..count {
        if offset + 24 > bytes.len() {
            break;
        }
        let o_seq = i64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let t_chain = i64::from_be_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        let t_seq = i64::from_be_bytes(bytes[offset + 16..offset + 24].try_into().unwrap());
        edges.push((o_seq, t_chain, t_seq));
        offset += 24;
    }
    edges
}

// -----------------------------------------------------------------------------
// PyO3 Bindings
// -----------------------------------------------------------------------------

#[pyfunction]
pub fn open_client(py: Python<'_>, path: String) -> PyResult<()> {
    py.detach(|| {
        if DBS.get().is_some() {
            return Ok(());
        }
        let layout = DatabaseLayout::open(std::path::PathBuf::from(&path)).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to open mtxdb layout: {}", e))
        })?;
        let open_pool = |pool| {
            let path = layout.pool_dir(pool)?;
            PackfileStorage::open(path)
        };
        // State pool holds HAMT nodes, roots, and state-group sidecars --
        // dense structural hashes, not text. zstd never shrinks them (see
        // mtxdb's own compression bench), so every write there was still
        // paying the compressor's full match-finding pass for nothing.
        // open_with_compression(.., false) skips the attempt entirely; the
        // event-dag/auth-chain pools (JSON-ish payloads) keep compression on.
        let open_state_pool = || {
            let path = layout.pool_dir(ShardType::State)?;
            PackfileStorage::open_with_compression(path, false)
        };
        let state = Arc::new(open_state_pool().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to open mtxdb state pool: {}",
                e
            ))
        })?);
        let event_dag = Arc::new(open_pool(ShardType::EventDag).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to open mtxdb event-dag pool: {}",
                e
            ))
        })?);
        let auth_chain = Arc::new(open_pool(ShardType::AuthChain).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to open mtxdb auth-chain pool: {}",
                e
            ))
        })?);
        let _ = DBS.set(MtxdbPools {
            state,
            event_dag,
            auth_chain,
        });
        let _ = ROOM_INDEX_DIR.set(std::path::PathBuf::from(&path).join("room_index"));
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
    let room_id = room_id_from_prefix(&room_prefix);

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
        let engine = state_db()?;
        engine.put_many(&room_id, &pairs).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })
    })
}

pub type AuthChainLinksForChain = (i64, Vec<(i64, i64, i64)>);

#[pyfunction]
pub fn get_auth_chain_links_batch(
    py: Python<'_>,
    namespace: String,
    chain_ids: Vec<i64>,
) -> PyResult<Vec<AuthChainLinksForChain>> {
    py.detach(|| {
        let engine = auth_chain_db()?;
        let room_id = namespace_room_id(&namespace);
        let node_ids: Vec<NodeId> = chain_ids.iter().map(|&c| chain_node_id(c)).collect();

        let results = engine.get_many(&room_id, &node_ids).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {}", e))
        })?;

        let mut out = Vec::with_capacity(chain_ids.len());
        for (chain_id, res) in chain_ids.into_iter().zip(results) {
            if let Some(data) = res {
                let edges = deserialize_manifest(&data.bytes);
                if !edges.is_empty() {
                    out.push((chain_id, edges));
                }
            }
        }
        Ok(out)
    })
}

#[pyfunction]
pub fn put_auth_chain_links_batch(
    namespace: String,
    links: Vec<(i64, i64, i64, i64)>,
) -> PyResult<()> {
    let _guard = RMW_LOCK
        .lock()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e)))?;
    // No `py.detach` here — see increment_counters_batch's comment for why.
    let engine = auth_chain_db()?;
    let room_id = namespace_room_id(&namespace);

    let mut grouped: HashMap<i64, Vec<(i64, i64, i64)>> = HashMap::new();
    for (o_chain, o_seq, t_chain, t_seq) in links {
        grouped
            .entry(o_chain)
            .or_default()
            .push((o_seq, t_chain, t_seq));
    }

    let mut pairs_to_put = Vec::with_capacity(grouped.len());

    for (chain_id, new_edges) in grouped {
        let node_id = chain_node_id(chain_id);
        let mut edges = match engine.get(&room_id, &node_id) {
            Ok(Some(data)) => deserialize_manifest(&data.bytes),
            Ok(None) => Vec::new(),
            Err(e) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "mtxdb get error reading chain {}: {}",
                    chain_id, e
                )))
            }
        };
        // Dedup against what's already stored: a retried caller (e.g. a
        // retried persist_events transaction, or a re-run background
        // migration batch) recomputes the same edges and calls this again
        // -- these writes aren't part of the SQL transaction's rollback,
        // so a retry after a partial success would otherwise duplicate
        // edges here forever. Existing edges are deduped first so an edge
        // already present twice from before this fix existed collapses
        // down rather than being preserved.
        let mut seen: HashSet<(i64, i64, i64)> = HashSet::with_capacity(edges.len());
        edges.retain(|edge| seen.insert(*edge));
        for edge in new_edges {
            if seen.insert(edge) {
                edges.push(edge);
            }
        }
        let bytes = serialize_manifest(&edges);
        pairs_to_put.push((node_id, NodeData::new(bytes::Bytes::from(bytes))));
    }

    engine.put_many(&room_id, &pairs_to_put).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
    })?;
    Ok(())
}

#[pyfunction]
pub fn delete_auth_chain_links_batch(namespace: String, pairs: Vec<(i64, i64)>) -> PyResult<()> {
    let _guard = RMW_LOCK
        .lock()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e)))?;
    // No `py.detach` here — see increment_counters_batch's comment for why.
    let engine = auth_chain_db()?;
    let room_id = namespace_room_id(&namespace);

    let mut grouped: HashMap<i64, HashSet<i64>> = HashMap::new();
    for (o_chain, o_seq) in pairs {
        grouped.entry(o_chain).or_default().insert(o_seq);
    }

    let mut pairs_to_put = Vec::with_capacity(grouped.len());

    for (chain_id, seqs_to_delete) in grouped {
        let node_id = chain_node_id(chain_id);
        if let Ok(Some(data)) = engine.get(&room_id, &node_id) {
            let edges = deserialize_manifest(&data.bytes);
            let filtered: Vec<_> = edges
                .into_iter()
                .filter(|(seq, _, _)| !seqs_to_delete.contains(seq))
                .collect();

            let bytes = serialize_manifest(&filtered);
            pairs_to_put.push((node_id, NodeData::new(bytes::Bytes::from(bytes))));
        }
    }

    if !pairs_to_put.is_empty() {
        engine.put_many(&room_id, &pairs_to_put).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })?;
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Generic KV (Event JSON / Event-to-State-Group)
// -----------------------------------------------------------------------------

fn kv_room_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = 1; // Dedicated room_id for global flat KV data
    id
}

fn kv_node_id(key: &[u8]) -> [u8; 16] {
    let hash = Sha256::digest(key);
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

/// Fetch flat-KV records, routing each key to its shard type internally.
#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    py.detach(|| {
        let mut state_ids = Vec::new();
        let mut event_ids = Vec::new();
        for (position, key) in keys.iter().enumerate() {
            let entry = (position, kv_node_id(key));
            match shard_type_for_key(key) {
                ShardType::State => state_ids.push(entry),
                ShardType::EventDag => event_ids.push(entry),
                ShardType::AuthChain => unreachable!("flat KV never routes to auth-chain"),
            }
        }
        let mut values = vec![None; keys.len()];
        for (shard_type, ids) in [
            (ShardType::State, state_ids),
            (ShardType::EventDag, event_ids),
        ] {
            if ids.is_empty() {
                continue;
            }
            let node_ids: Vec<NodeId> = ids.iter().map(|(_, id)| *id).collect();
            let found = db_for_shard_type(shard_type)?
                .get_many(&kv_room_id(), &node_ids)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get_many error: {e}"))
                })?;
            for ((position, _), value) in ids.into_iter().zip(found) {
                values[position] = value;
            }
        }
        Ok(keys
            .into_iter()
            .zip(values)
            .filter_map(|(key, value)| {
                value
                    .filter(|data| !data.bytes.is_empty())
                    .map(|data| (key, data.bytes.to_vec()))
            })
            .collect())
    })
}

/// Store flat-KV records, routing each key to its shard type internally.
#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    py.detach(|| {
        let mut state_puts = Vec::new();
        let mut event_puts = Vec::new();
        for (key, value) in pairs {
            let entry = (kv_node_id(&key), NodeData::new(bytes::Bytes::from(value)));
            match shard_type_for_key(&key) {
                ShardType::State => state_puts.push(entry),
                ShardType::EventDag => event_puts.push(entry),
                ShardType::AuthChain => unreachable!("flat KV never routes to auth-chain"),
            }
        }
        for (shard_type, puts) in [
            (ShardType::State, state_puts),
            (ShardType::EventDag, event_puts),
        ] {
            if !puts.is_empty() {
                db_for_shard_type(shard_type)?
                    .put_many(&kv_room_id(), &puts)
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {e}"))
                    })?;
            }
        }
        Ok(())
    })
}

/// Tombstone flat-KV records in the shard type selected from each key.
#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    let pairs = keys.into_iter().map(|key| (key, Vec::new())).collect();
    batch_put(py, pairs)
}

// -----------------------------------------------------------------------------
// HAMT Materialize / Lookup Wrappers
// -----------------------------------------------------------------------------

use rezzy::hamt::StructuralHash;

use crate::database::core::{self, NodeCache, StateEntries};
use crate::state_hamt::room_structural_key_raw;

static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(core::new_node_cache)
}

#[pyfunction]
pub fn materialize_state_hamt(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    root_structural_hash: Vec<u8>,
    room_id: &str,
) -> PyResult<Option<StateEntries>> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {} bytes",
            ROOM_PREFIX_LEN
        ))
    })?;
    let root_structural_hash: StructuralHash = root_structural_hash.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
    })?;
    let structural_key = room_structural_key_raw(room_id);

    py.detach(|| {
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };

        core::materialize_state_hamt(
            &store,
            node_cache(),
            &namespace,
            &room_prefix,
            root_structural_hash,
            &structural_key,
        )
        .map(Some)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    })
}

#[pyfunction]
pub fn materialize_state_hamts(
    py: Python<'_>,
    namespace: String,
    roots: Vec<(Vec<u8>, Vec<u8>, String)>,
) -> PyResult<Vec<StateEntries>> {
    let roots = roots
        .into_iter()
        .map(|(room_prefix, root_hash, room_id)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {} bytes",
                    ROOM_PREFIX_LEN
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 32 bytes")
            })?;
            let structural_key = room_structural_key_raw(&room_id);
            Ok((room_prefix, structural_key, root_hash))
        })
        .collect::<PyResult<Vec<_>>>()?;

    py.detach(|| {
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };

        core::materialize_state_hamts(&store, node_cache(), &namespace, roots)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    })
}

pub type PySelectiveQuery = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<(String, String)>);

#[pyfunction]
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
                    "room_prefix must be {} bytes",
                    ROOM_PREFIX_LEN
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
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };

        core::lookup_state_hamts(&store, node_cache(), &namespace, parsed_queries)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    })
}

pub type PyRootRecord = (i64, Vec<u8>, Vec<u8>, String, Vec<u8>);

#[pyfunction]
pub fn batch_get_state_hamt_roots(
    py: Python<'_>,
    namespace: String,
    groups: Vec<i64>,
) -> PyResult<Vec<Option<PyRootRecord>>> {
    py.detach(|| {
        let engine = state_db()?;
        let store = MtxdbStore {
            engine: Arc::clone(engine),
        };
        let records = core::batch_get_state_hamt_roots(&store, &namespace, &groups)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(groups
            .into_iter()
            .zip(records)
            .map(|(group, rec)| {
                rec.map(|r| {
                    let structural_hash_vec = r.root_hash.as_slice().to_vec();
                    let room_prefix_vec = r.room_prefix.to_vec();
                    (
                        group,
                        room_prefix_vec,
                        structural_hash_vec,
                        r.room_id,
                        r.lattice,
                    )
                })
            })
            .collect())
    })
}

#[pyfunction]
pub fn increment_counters_batch(pairs: Vec<(Vec<u8>, i64)>) -> PyResult<Vec<i64>> {
    let _guard = RMW_LOCK
        .lock()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock poison: {}", e)))?;
    // No `py.detach` here: the RMW lock must be held across get→put_many,
    // and MutexGuard is !Send so it can't cross the Ungil boundary. The GIL
    // stays held for the duration (#[pyfunction] holds it by default), which
    // also serializes concurrent Python callers — so this is safe and fast
    // for local mmap I/O.
    let engine = state_db()?;
    let room_id = kv_room_id();
    let mut results = Vec::with_capacity(pairs.len());
    let mut puts = Vec::with_capacity(pairs.len());

    for (key, delta) in pairs {
        let node_id = kv_node_id(&key);
        let current = match engine.get(&room_id, &node_id) {
            Ok(Some(data)) => {
                if data.bytes.len() == 8 {
                    i64::from_be_bytes(data.bytes.as_ref().try_into().unwrap())
                } else {
                    0
                }
            }
            Ok(None) => 0,
            Err(e) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "mtxdb get error reading counter: {}",
                    e
                )))
            }
        };
        let new_value = current + delta;
        results.push(new_value);
        puts.push((
            node_id,
            NodeData::new(bytes::Bytes::copy_from_slice(&new_value.to_be_bytes())),
        ));
    }

    if !puts.is_empty() {
        engine.put_many(&room_id, &puts).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb put error: {}", e))
        })?;
    }

    Ok(results)
}

/// Batch-read HAMT nodes from the native mtxdb store using the same
/// `(room_prefix, structural_hash)` key encoding that `put_state_hamt_nodes`
/// uses.  Returns one `Option<Vec<u8>>` per requested hash (`None` for misses).
#[pyfunction]
pub fn get_state_hamt_nodes_batch(
    py: Python<'_>,
    _namespace: String,
    room_prefix: Vec<u8>,
    hashes: Vec<Vec<u8>>,
) -> PyResult<Vec<Option<Vec<u8>>>> {
    let mut room_id = [0u8; 16];
    let prefix_len = std::cmp::min(room_prefix.len(), 16);
    room_id[..prefix_len].copy_from_slice(&room_prefix[..prefix_len]);

    let node_ids: Vec<NodeId> = hashes
        .iter()
        .map(|h| {
            let mut node_id = [0u8; 16];
            let copy_len = std::cmp::min(h.len(), 16);
            node_id[..copy_len].copy_from_slice(&h[..copy_len]);
            node_id
        })
        .collect();

    py.detach(|| {
        let engine = state_db()?;
        let results = engine.get_many(&room_id, &node_ids).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("mtxdb get error: {}", e))
        })?;
        Ok(results
            .into_iter()
            .map(|opt| opt.map(|d| d.bytes.to_vec()))
            .collect())
    })
}

#[pyfunction]
pub fn sync(py: Python<'_>) -> PyResult<()> {
    py.detach(|| {
        for (name, engine) in [
            ("state", state_db()?),
            ("event-dag", event_dag_db()?),
            ("auth-chain", auth_chain_db()?),
        ] {
            engine.sync().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "mtxdb sync error for {name} pool: {e}"
                ))
            })?;
        }
        room_index::sync()?;
        Ok(())
    })
}

#[pyfunction]
pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_client, m)?)?;
    m.add_function(wrap_pyfunction!(put_state_hamt_nodes, m)?)?;
    m.add_function(wrap_pyfunction!(get_state_hamt_nodes_batch, m)?)?;
    m.add_function(wrap_pyfunction!(get_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(put_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(delete_auth_chain_links_batch, m)?)?;
    m.add_function(wrap_pyfunction!(batch_get, m)?)?;
    m.add_function(wrap_pyfunction!(batch_put, m)?)?;
    m.add_function(wrap_pyfunction!(batch_delete, m)?)?;
    m.add_function(wrap_pyfunction!(materialize_state_hamt, m)?)?;
    m.add_function(wrap_pyfunction!(materialize_state_hamts, m)?)?;
    m.add_function(wrap_pyfunction!(lookup_state_hamts, m)?)?;
    m.add_function(wrap_pyfunction!(batch_get_state_hamt_roots, m)?)?;
    m.add_function(wrap_pyfunction!(put_state_hamt_roots, m)?)?;
    m.add_function(wrap_pyfunction!(get_state_hamt_roots_for_room, m)?)?;
    m.add_function(wrap_pyfunction!(delete_state_hamt_roots_for_room, m)?)?;
    m.add_function(wrap_pyfunction!(put_room_index, m)?)?;
    m.add_function(wrap_pyfunction!(get_room_index, m)?)?;
    m.add_function(wrap_pyfunction!(increment_counters_batch, m)?)?;
    m.add_function(wrap_pyfunction!(sync, m)?)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.mtxdb_engine", m)?;
    Ok(())
}
