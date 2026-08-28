use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};

use lru::LruCache;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use rezzy::hamt::{HamtNode, StructuralHash};
use sha2::{Digest, Sha256};
use tikv_client::{RawClient, TransactionClient};
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout, Duration};

use crate::state_hamt::{decode_persisted_node, lookup_from_node_map, materialize_from_node_map};

static RUNTIME: OnceCell<Runtime> = OnceCell::new();
static CLIENT: OnceCell<RawClient> = OnceCell::new();
static TX_CLIENT: OnceCell<TransactionClient> = OnceCell::new();

/// Process-wide in-memory cache of decoded HAMT nodes, keyed by their full
/// (namespaced, room-prefixed) TiKV key. HAMT nodes are immutable and
/// content-addressed, so a cache hit is always correct -- there is no
/// invalidation to get wrong, only a possible network round-trip saved.
/// Sized generously (each node is small) since it's shared across every
/// room and state group this process serves.
const NODE_CACHE_CAPACITY: usize = 100_000;
type NodeCache = Mutex<LruCache<Vec<u8>, Arc<HamtNode<String, String>>>>;
static NODE_CACHE: OnceCell<NodeCache> = OnceCell::new();

fn node_cache() -> &'static NodeCache {
    NODE_CACHE.get_or_init(|| {
        Mutex::new(LruCache::new(
            NonZeroUsize::new(NODE_CACHE_CAPACITY).expect("cache capacity is nonzero"),
        ))
    })
}
const READINESS_PROBE_VALUE: &[u8] = b"ok";
static READINESS_PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const OPEN_CLIENT_ATTEMPTS: u32 = 5;
const OPEN_CLIENT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const OPEN_CLIENT_RETRY_DELAY: Duration = Duration::from_secs(1);
const TRANSACTION_WRITE_ATTEMPTS: u32 = 5;
const TRANSACTION_WRITE_RETRY_DELAY: Duration = Duration::from_millis(10);

fn is_retryable_key_error(k: &tikv_client::ProtoKeyError) -> bool {
    k.conflict.is_some() || !k.retryable.is_empty() || k.locked.is_some()
}

fn is_retryable_region_error(r: &tikv_client::ProtoRegionError) -> bool {
    r.not_leader.is_some()
        || r.epoch_not_match.is_some()
        || r.server_is_busy.is_some()
        || r.stale_command.is_some()
        || r.region_not_found.is_some()
}

fn is_retryable_txn_error(err: &tikv_client::Error) -> bool {
    match err {
        tikv_client::Error::KeyError(k) => is_retryable_key_error(k),
        tikv_client::Error::RegionError(r) => is_retryable_region_error(r),
        tikv_client::Error::ResolveLockError(_) => true,
        tikv_client::Error::PessimisticLockError { inner, .. } => is_retryable_txn_error(inner),
        // Transaction commits can aggregate per-key errors. Retry only when
        // every member is retryable, rather than turning a fatal error into a
        // delayed retry loop.
        tikv_client::Error::MultipleKeyErrors(errors) => {
            !errors.is_empty() && errors.iter().all(is_retryable_txn_error)
        }
        // Replaying this operation is safe: it only writes immutable,
        // content-addressed HAMT nodes and the same state-group root value.
        tikv_client::Error::UndeterminedError(_) => true,
        tikv_client::Error::LeaderNotFound { .. }
        | tikv_client::Error::RegionNotFoundInResponse { .. }
        | tikv_client::Error::EntryNotFoundInRegionCache => true,
        _ => false,
    }
}

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime")
    })
}

async fn check_raw_kv_ready(client: &RawClient) -> Result<(), String> {
    let probe_key = format!(
        "synapse:tikv:readiness-probe:{}:{}",
        std::process::id(),
        READINESS_PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
    .into_bytes();
    client
        .put(probe_key.clone(), READINESS_PROBE_VALUE.to_vec())
        .await
        .map_err(|e| e.to_string())?;

    let probe_result = async {
        let value = client
            .get(probe_key.clone())
            .await
            .map_err(|e| e.to_string())?;
        if value.as_deref() != Some(READINESS_PROBE_VALUE) {
            return Err("TiKV readiness probe returned an unexpected value".to_owned());
        }
        Ok(())
    }
    .await;

    // A successful write must be cleaned up even if the read failed. The
    // readiness key is process-unique, so this can never delete another
    // process's probe.
    let delete_result = client.delete(probe_key).await.map_err(|e| e.to_string());
    probe_result.and(delete_result)
}

async fn open_ready_client(pd_endpoints: Vec<String>) -> Result<RawClient, String> {
    let mut last_error = String::new();

    for attempt in 1..=OPEN_CLIENT_ATTEMPTS {
        match timeout(OPEN_CLIENT_ATTEMPT_TIMEOUT, async {
            let client = RawClient::new(pd_endpoints.clone())
                .await
                .map_err(|e| e.to_string())?;
            check_raw_kv_ready(&client).await?;
            Ok::<RawClient, String>(client)
        })
        .await
        {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(e)) => last_error = e,
            Err(_) => {
                last_error = format!(
                    "timed out after {} seconds",
                    OPEN_CLIENT_ATTEMPT_TIMEOUT.as_secs()
                );
            }
        }

        if attempt < OPEN_CLIENT_ATTEMPTS {
            sleep(OPEN_CLIENT_RETRY_DELAY).await;
        }
    }

    Err(format!(
        "TiKV cluster did not become ready for raw KV operations after {} attempts: {}",
        OPEN_CLIENT_ATTEMPTS, last_error
    ))
}

/// How long a failed `open_client` attempt is remembered before the next
/// caller is allowed to retry against the network again.
///
/// `open_client` is called once per `StateStore` construction -- i.e. once
/// per test-case `HomeServer` in a trial run, or once per worker restart in
/// production. Without this, a genuinely unreachable TiKV cluster meant
/// every single caller paid `open_ready_client`'s full multi-attempt retry
/// budget (up to ~14s) from scratch, compounding into what looks like an
/// indefinite hang across a large test suite. This is a short-TTL negative
/// cache, not a permanent one, so a cluster that comes up mid-run is still
/// picked up promptly by the next caller after the TTL lapses.
const OPEN_CLIENT_NEGATIVE_CACHE_TTL: StdDuration = StdDuration::from_secs(5);
type OpenClientFailure = Mutex<Option<(Vec<String>, Instant, String)>>;
static OPEN_CLIENT_FAILURE: OnceCell<OpenClientFailure> = OnceCell::new();

fn open_client_failure_cache() -> &'static OpenClientFailure {
    OPEN_CLIENT_FAILURE.get_or_init(|| Mutex::new(None))
}

#[pyfunction]
pub fn open_client(py: Python<'_>, pd_endpoints: Vec<String>) -> PyResult<()> {
    if CLIENT.get().is_some() && TX_CLIENT.get().is_some() {
        return Ok(());
    }
    if CLIENT.get().is_some() || TX_CLIENT.get().is_some() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is only partially initialized",
        ));
    }

    if let Some((cached_endpoints, failed_at, err)) =
        open_client_failure_cache().lock().unwrap().as_ref()
    {
        if cached_endpoints == &pd_endpoints && failed_at.elapsed() < OPEN_CLIENT_NEGATIVE_CACHE_TTL
        {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "TiKV cluster at {:?} failed to connect {:.1}s ago; not retrying yet \
                 (retries again after {:.0}s): {}",
                pd_endpoints,
                failed_at.elapsed().as_secs_f64(),
                OPEN_CLIENT_NEGATIVE_CACHE_TTL.as_secs_f64(),
                err
            )));
        }
    }

    let record_failure = |err: &str| {
        *open_client_failure_cache().lock().unwrap() =
            Some((pd_endpoints.clone(), Instant::now(), err.to_owned()));
    };

    let rt = get_runtime();
    let client =
        match py.detach(|| rt.block_on(async { open_ready_client(pd_endpoints.clone()).await })) {
            Ok(client) => client,
            Err(e) => {
                record_failure(&e);
                return Err(pyo3::exceptions::PyRuntimeError::new_err(e));
            }
        };

    let tx_client = match py.detach(|| {
        get_runtime()
            .block_on(TransactionClient::new(pd_endpoints.clone()))
            .map_err(|e| e.to_string())
    }) {
        Ok(tx_client) => tx_client,
        Err(e) => {
            record_failure(&e);
            return Err(pyo3::exceptions::PyRuntimeError::new_err(e));
        }
    };
    // Publish both clients only after both connections have been established;
    // a failed transaction-client connection must not leave open_client()
    // appearing successful on subsequent calls.
    if TX_CLIENT.set(tx_client).is_err() && TX_CLIENT.get().is_none() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Failed to set TiKV transaction client",
        ));
    }
    if CLIENT.set(client).is_err() && CLIENT.get().is_none() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Failed to set TiKV Client instance",
        ));
    }
    Ok(())
}

#[pyfunction]
pub fn put(py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.put(key, value).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

#[pyfunction]
pub fn get(py: Python<'_>, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    let val = py
        .detach(|| {
            rt.block_on(async { client.get(key).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(val)
}

#[pyfunction]
pub fn batch_get(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    let pairs = py
        .detach(|| {
            rt.block_on(async { client.batch_get(keys).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    let results: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .into_iter()
        .map(|pair| {
            let (k, v): (tikv_client::Key, tikv_client::Value) = pair.into();
            (k.into(), v)
        })
        .collect();
    Ok(results)
}

#[pyfunction]
pub fn batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.batch_put(pairs).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

/// Atomically publish a set of immutable HAMT nodes and its root/index
/// record. RawClient::batch_put is not transactional; using it for a tree
/// plus its root can expose a root whose children are not committed yet.
#[pyfunction]
pub fn transactional_batch_put(py: Python<'_>, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
    let client = TX_CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV transaction client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async {
            let mut last_error: Option<tikv_client::Error> = None;

            for attempt in 1..=TRANSACTION_WRITE_ATTEMPTS {
                let result: Result<(), tikv_client::Error> = async {
                    let mut txn = client.begin_optimistic().await?;
                    for (key, value) in &pairs {
                        txn.put(key.clone(), value.clone()).await?;
                    }
                    txn.commit().await?;
                    Ok(())
                }
                .await;

                match result {
                    Ok(_) => return Ok(()),
                    Err(error) => {
                        if !is_retryable_txn_error(&error) {
                            return Err(error.to_string());
                        }
                        last_error = Some(error);
                    }
                }

                if attempt < TRANSACTION_WRITE_ATTEMPTS {
                    sleep(TRANSACTION_WRITE_RETRY_DELAY * attempt).await;
                }
            }

            Err(last_error
                .expect("transaction write loop always records an error")
                .to_string())
        })
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

#[pyfunction]
pub fn delete(py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.delete(key).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

#[pyfunction]
pub fn batch_delete(py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<()> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(async { client.batch_delete(keys).await })
            .map_err(|e| e.to_string())
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(())
}

/// Computes the exclusive upper bound of a byte-string prefix range: `prefix`
/// incremented as a big-endian number. Strips any trailing 0xFF bytes (they
/// can't be incremented in place) and increments the first byte, from the
/// end, that isn't 0xFF -- e.g. `[0x01, 0xFF] -> [0x02]`, not `[0x01]` (which
/// would sort *before* `prefix`, producing an inverted, empty scan range).
///
/// Returns an empty `Vec` if every byte in `prefix` is 0xFF (or `prefix` is
/// itself empty): there is no finite successor in that case, so the caller
/// must scan with no upper bound and filter results by prefix explicitly.
fn prefix_scan_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    loop {
        match end.pop() {
            None => break,
            Some(0xFF) => continue,
            Some(b) => {
                end.push(b + 1);
                break;
            }
        }
    }
    end
}

#[pyfunction]
pub fn scan_prefix(
    py: Python<'_>,
    prefix: Vec<u8>,
    limit: u32,
) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let client = CLIENT.get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "TiKV client is not open. Call open_client first.",
        )
    })?;
    let rt = get_runtime();

    let start = prefix.clone();
    let end = prefix_scan_upper_bound(&prefix);

    let pairs = if end.is_empty() {
        py.detach(|| {
            rt.block_on(async { client.scan(start.., limit).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?
    } else {
        py.detach(|| {
            rt.block_on(async { client.scan(start..end, limit).await })
                .map_err(|e| e.to_string())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?
    };

    let results: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .into_iter()
        .map(|pair| {
            let (k, v): (tikv_client::Key, tikv_client::Value) = pair.into();
            let key_bytes: Vec<u8> = k.into();
            (key_bytes, v)
        })
        // Only matters for the unbounded (all-0xFF-prefix) branch above --
        // an incremented finite end bound already scopes the range exactly.
        .filter(|(k, _): &(Vec<u8>, Vec<u8>)| k.starts_with(&prefix))
        .collect();
    Ok(results)
}

/// Batch size for TiKV `batch_get` calls while walking the HAMT. Matches the
/// batching Python previously did in `_get_state_groups_from_hamt_txn`.
const NODE_FETCH_BATCH_SIZE: usize = 100;

/// Fixed width of the room-scoped TiKV key prefix. See `room_tikv_prefix_raw`
/// for why 8 bytes is enough: it's a locality hint, not an identity -- a
/// prefix collision between two rooms just means their nodes interleave in
/// the same key range, not a correctness issue (the full key is always
/// `prefix || structural_hash`, which stays unique regardless). At 8 bytes,
/// the birthday bound puts a 50% chance of *any* collision existing at
/// around 4 billion rooms -- far beyond any realistic homeserver.
const ROOM_PREFIX_LEN: usize = 8;

/// Namespaced, room-prefixed HAMT node key:
/// `hamt:node:<namespace_hash>:<room_prefix_hex>:<structural_hash_hex>`.
/// The room prefix gives nodes belonging to the same room contiguous byte
/// ranges in TiKV's sorted keyspace, for locality -- see `room_tikv_prefix_raw`.
fn node_tikv_key(
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

async fn get_client() -> Result<&'static RawClient, String> {
    CLIENT
        .get()
        .ok_or_else(|| "TiKV client is not open. Call open_client first.".to_owned())
}

/// Fetch a state group's HAMT from TiKV, entirely in Rust: BFS-fetch the
/// reachable nodes (batched), decode each one, and materialize
/// `(event_type, state_key, event_id)` triples -- without crossing back into
/// Python per node.
///
/// The caller supplies the TiKV namespace, room prefix, and root hash. Node
/// keys are namespaced because content-addressing is only safe to share when
/// every deployment uses the same HAMT secret.
async fn materialize_state_hamt_async(
    namespace: &str,
    room_prefix: &[u8; ROOM_PREFIX_LEN],
    root_structural_hash: StructuralHash,
) -> Result<Vec<(String, String, String)>, String> {
    let client = get_client().await?;

    let mut node_map: HashMap<StructuralHash, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);
    let mut to_fetch: HashSet<StructuralHash> = HashSet::from([root_structural_hash]);

    while !to_fetch.is_empty() {
        let current_batch: Vec<StructuralHash> = to_fetch.drain().collect();

        // Serve whatever we can from the in-memory node cache first. A hit
        // here is always correct (see `NODE_CACHE`) and saves a TiKV
        // round-trip entirely; children of a cached node are queued for the
        // next round, where they'll again be checked against the cache
        // before any network access.
        let mut still_missing = Vec::with_capacity(current_batch.len());
        {
            let mut cache = node_cache().lock().unwrap();
            for hash in current_batch {
                let key = node_tikv_key(namespace, room_prefix, &hash);
                match cache.get(&key) {
                    Some(node) => {
                        let node = node.clone();
                        for child in &node.children {
                            let child_hash = child.structural_hash();
                            if seen.insert(child_hash) {
                                to_fetch.insert(child_hash);
                            }
                        }
                        node_map.insert(hash, node);
                    }
                    None => still_missing.push(hash),
                }
            }
        }

        for chunk in still_missing.chunks(NODE_FETCH_BATCH_SIZE) {
            let keys: Vec<Vec<u8>> = chunk
                .iter()
                .map(|hash| node_tikv_key(namespace, room_prefix, hash))
                .collect();
            let rows = client.batch_get(keys).await.map_err(|e| e.to_string())?;

            if rows.len() != chunk.len() {
                return Err(format!(
                    "Missing HAMT node(s): expected {}, got {}",
                    chunk.len(),
                    rows.len()
                ));
            }

            for pair in rows {
                let (key, node_bytes): (tikv_client::Key, tikv_client::Value) = pair.into();
                let key: Vec<u8> = key.into();
                // We fetched by exact key per hash, so re-derive which hash this
                // row belongs to by decoding the node itself rather than trusting
                // key-order (TiKV batch_get does not guarantee response order).
                let node = decode_persisted_node(&node_bytes)?;
                let hash = node.structural_hash;

                for child in &node.children {
                    let child_hash = child.structural_hash();
                    if seen.insert(child_hash) {
                        to_fetch.insert(child_hash);
                    }
                }

                node_cache().lock().unwrap().put(key, node.clone());
                node_map.insert(hash, node);
            }
        }
    }

    materialize_from_node_map(&root_structural_hash, &node_map)
}

/// Materialize several HAMT roots in one TiKV traversal.
///
/// Each BFS layer is fetched across every requested root at once. Nodes are
/// keyed by both their room prefix and structural hash: structural hashes are
/// only meaningful inside the room-scoped TiKV keyspace, while roots for the
/// same room can share fetched subtrees.
async fn materialize_state_hamts_async(
    namespace: &str,
    roots: Vec<([u8; ROOM_PREFIX_LEN], StructuralHash)>,
) -> Result<Vec<Vec<(String, String, String)>>, String> {
    let client = get_client().await?;
    type NodeLocation = ([u8; ROOM_PREFIX_LEN], StructuralHash);

    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = roots.iter().copied().collect();
    let mut to_fetch = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        // See the comment in `materialize_state_hamt_async`: serve cached
        // nodes first, only hitting TiKV for what's genuinely missing.
        let mut still_missing = Vec::with_capacity(current_batch.len());
        {
            let mut cache = node_cache().lock().unwrap();
            for (room_prefix, hash) in current_batch {
                let key = node_tikv_key(namespace, &room_prefix, &hash);
                match cache.get(&key) {
                    Some(node) => {
                        let node = node.clone();
                        for child in &node.children {
                            let child_location = (room_prefix, child.structural_hash());
                            if seen.insert(child_location) {
                                to_fetch.insert(child_location);
                            }
                        }
                        node_map.insert((room_prefix, hash), node);
                    }
                    None => still_missing.push((room_prefix, hash)),
                }
            }
        }

        for chunk in still_missing.chunks(NODE_FETCH_BATCH_SIZE) {
            let key_to_location: HashMap<Vec<u8>, NodeLocation> = chunk
                .iter()
                .map(|(room_prefix, hash)| {
                    (
                        node_tikv_key(namespace, room_prefix, hash),
                        (*room_prefix, *hash),
                    )
                })
                .collect();
            let keys: Vec<Vec<u8>> = key_to_location.keys().cloned().collect();
            let rows = client.batch_get(keys).await.map_err(|e| e.to_string())?;

            if rows.len() != chunk.len() {
                return Err(format!(
                    "Missing HAMT node(s): expected {}, got {}",
                    chunk.len(),
                    rows.len()
                ));
            }

            for pair in rows {
                let (key, node_bytes): (tikv_client::Key, tikv_client::Value) = pair.into();
                let key: Vec<u8> = key.into();
                let (room_prefix, expected_hash) = key_to_location
                    .get(&key)
                    .copied()
                    .ok_or_else(|| "TiKV returned an unexpected HAMT node key".to_owned())?;
                let node = decode_persisted_node(&node_bytes)?;
                if node.structural_hash != expected_hash {
                    return Err("HAMT node hash does not match its TiKV key".to_owned());
                }

                for child in &node.children {
                    let child_location = (room_prefix, child.structural_hash());
                    if seen.insert(child_location) {
                        to_fetch.insert(child_location);
                    }
                }

                node_cache().lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, expected_hash), node);
            }
        }
    }

    type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
    let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
    for ((room_prefix, hash), node) in node_map {
        nodes_by_prefix
            .entry(room_prefix)
            .or_default()
            .insert(hash, node);
    }

    roots
        .into_iter()
        .map(|(room_prefix, root_hash)| {
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

/// Materialize a state group's full state map directly from TiKV, in pure
/// Rust. `room_prefix` and `root_structural_hash` come from the TiKV root
/// record, so this needs no room_id or room-version lookup.
#[pyfunction]
#[pyo3(text_signature = "(namespace, room_prefix, root_structural_hash, /)")]
pub fn materialize_state_hamt(
    py: Python<'_>,
    namespace: String,
    room_prefix: Vec<u8>,
    root_structural_hash: Vec<u8>,
) -> PyResult<Option<Vec<(String, String, String)>>> {
    let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "room_prefix must be {ROOM_PREFIX_LEN} bytes"
        ))
    })?;
    let root_structural_hash: StructuralHash = root_structural_hash.try_into().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 16 bytes")
    })?;
    let rt = get_runtime();
    py.detach(|| {
        rt.block_on(materialize_state_hamt_async(
            &namespace,
            &room_prefix,
            root_structural_hash,
        ))
    })
    .map(Some)
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

/// Materialize several state groups' full state maps directly from TiKV.
///
/// The input and output order is preserved. Callers should use this only for
/// multi-root reads; the single-root function avoids the small setup cost for
/// the common case.
#[pyfunction]
#[pyo3(text_signature = "(namespace, roots, /)")]
pub fn materialize_state_hamts(
    py: Python<'_>,
    namespace: String,
    roots: Vec<(Vec<u8>, Vec<u8>)>,
) -> PyResult<Vec<Vec<(String, String, String)>>> {
    let roots = roots
        .into_iter()
        .map(|(room_prefix, root_hash)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {ROOM_PREFIX_LEN} bytes"
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 16 bytes")
            })?;
            Ok((room_prefix, root_hash))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let rt = get_runtime();
    py.detach(|| rt.block_on(materialize_state_hamts_async(&namespace, roots)))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

type SelectiveQuery = (
    [u8; ROOM_PREFIX_LEN],
    StructuralHash,
    [u8; 32],
    Vec<(String, String)>,
);
type PySelectiveQuery = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<(String, String)>);

/// Look up specific state keys across several HAMT roots in one TiKV traversal.
///
/// Traverses each BFS layer across every requested root at once, querying the
/// in-memory node cache first and batch-fetching missing child nodes across all
/// requested trees simultaneously.
async fn lookup_state_hamts_async(
    namespace: &str,
    queries: Vec<SelectiveQuery>,
) -> Result<Vec<Vec<(String, String, String)>>, String> {
    let client = get_client().await?;
    type NodeLocation = ([u8; ROOM_PREFIX_LEN], StructuralHash);

    let mut node_map: HashMap<NodeLocation, Arc<HamtNode<String, String>>> = HashMap::new();
    let mut seen: HashSet<NodeLocation> = queries.iter().map(|(p, h, _, _)| (*p, *h)).collect();
    let mut to_fetch: HashSet<NodeLocation> = seen.clone();

    while !to_fetch.is_empty() {
        let current_batch: Vec<NodeLocation> = to_fetch.drain().collect();

        let mut still_missing = Vec::with_capacity(current_batch.len());
        {
            let mut cache = node_cache().lock().unwrap();
            for (room_prefix, hash) in current_batch {
                let key = node_tikv_key(namespace, &room_prefix, &hash);
                match cache.get(&key) {
                    Some(node) => {
                        let node = node.clone();
                        node_map.insert((room_prefix, hash), node);
                    }
                    None => still_missing.push((room_prefix, hash)),
                }
            }
        }

        for chunk in still_missing.chunks(NODE_FETCH_BATCH_SIZE) {
            let key_to_location: HashMap<Vec<u8>, NodeLocation> = chunk
                .iter()
                .map(|(room_prefix, hash)| {
                    (
                        node_tikv_key(namespace, room_prefix, hash),
                        (*room_prefix, *hash),
                    )
                })
                .collect();
            let keys: Vec<Vec<u8>> = key_to_location.keys().cloned().collect();
            let rows = client.batch_get(keys).await.map_err(|e| e.to_string())?;

            if rows.len() != chunk.len() {
                return Err(format!(
                    "Missing HAMT node(s): expected {}, got {}",
                    chunk.len(),
                    rows.len()
                ));
            }

            for pair in rows {
                let (key, node_bytes): (tikv_client::Key, tikv_client::Value) = pair.into();
                let key: Vec<u8> = key.into();
                let (room_prefix, expected_hash) = key_to_location
                    .get(&key)
                    .copied()
                    .ok_or_else(|| "TiKV returned an unexpected HAMT node key".to_owned())?;
                let node = decode_persisted_node(&node_bytes)?;
                if node.structural_hash != expected_hash {
                    return Err("HAMT node hash does not match its TiKV key".to_owned());
                }

                node_cache().lock().unwrap().put(key, node.clone());
                node_map.insert((room_prefix, expected_hash), node);
            }
        }

        // Group loaded nodes by room prefix so we can query each room's tree
        type PrefixNodeMap = HashMap<StructuralHash, Arc<HamtNode<String, String>>>;
        let mut nodes_by_prefix: HashMap<[u8; ROOM_PREFIX_LEN], PrefixNodeMap> = HashMap::new();
        for ((room_prefix, hash), node) in &node_map {
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
                        let child_loc = (*room_prefix, missing_hash);
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
    for ((room_prefix, hash), node) in node_map {
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

/// Look up specific state keys across several state groups directly from TiKV.
///
/// Each query is a tuple of `(room_prefix, root_structural_hash, structural_key, keys)`.
#[pyfunction]
#[pyo3(text_signature = "(namespace, queries, /)")]
pub fn lookup_state_hamts(
    py: Python<'_>,
    namespace: String,
    queries: Vec<PySelectiveQuery>,
) -> PyResult<Vec<Vec<(String, String, String)>>> {
    let parsed_queries = queries
        .into_iter()
        .map(|(room_prefix, root_hash, structural_key, keys)| {
            let room_prefix: [u8; ROOM_PREFIX_LEN] = room_prefix.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "room_prefix must be {ROOM_PREFIX_LEN} bytes"
                ))
            })?;
            let root_hash: StructuralHash = root_hash.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("root_structural_hash must be 16 bytes")
            })?;
            let structural_key: [u8; 32] = structural_key.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("structural_key must be 32 bytes")
            })?;
            Ok((room_prefix, root_hash, structural_key, keys))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let rt = get_runtime();
    py.detach(|| rt.block_on(lookup_state_hamts_async(&namespace, parsed_queries)))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let child_module = PyModule::new(py, "tikv_engine")?;
    child_module.add_function(wrap_pyfunction!(open_client, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_get, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(transactional_batch_put, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(batch_delete, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(scan_prefix, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_hamt, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(materialize_state_hamts, &child_module)?)?;
    child_module.add_function(wrap_pyfunction!(lookup_state_hamts, &child_module)?)?;

    m.add_submodule(&child_module)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse.synapse_rust.tikv_engine", &child_module)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_scan_upper_bound_increments_last_non_ff_byte() {
        assert_eq!(prefix_scan_upper_bound(&[0x01]), vec![0x02]);
        assert_eq!(prefix_scan_upper_bound(&[0x01, 0x02]), vec![0x01, 0x03]);
    }

    #[test]
    fn prefix_scan_upper_bound_carries_past_trailing_ff_bytes() {
        // The bug this guards against: naively popping a single trailing
        // 0xFF byte gives [0x01], which sorts *before* [0x01, 0xFF] --
        // an inverted, empty range. The correct successor carries into
        // the preceding byte instead.
        let end = prefix_scan_upper_bound(&[0x01, 0xFF]);
        assert_eq!(end, vec![0x02]);
        assert!(end.as_slice() > [0x01, 0xFF].as_slice());

        assert_eq!(prefix_scan_upper_bound(&[0x01, 0xFF, 0xFF]), vec![0x02]);
    }

    #[test]
    fn prefix_scan_upper_bound_has_no_successor_for_all_ff_or_empty() {
        assert_eq!(prefix_scan_upper_bound(&[0xFF, 0xFF]), Vec::<u8>::new());
        assert_eq!(prefix_scan_upper_bound(&[]), Vec::<u8>::new());
    }

    #[test]
    fn open_client_negative_cache_records_and_matches_by_endpoint() {
        // Exercises the real process-wide cache via `open_client_failure_cache()`.
        // Uses an endpoint string unique to this test so it can't collide
        // with another test's entry if tests run in parallel within this
        // process.
        let endpoints = vec!["unit-test-only-negative-cache:2379".to_owned()];
        let other_endpoints = vec!["unit-test-only-negative-cache-2:2379".to_owned()];

        {
            let mut cache = open_client_failure_cache().lock().unwrap();
            assert!(
                cache.is_none() || cache.as_ref().unwrap().0 != endpoints,
                "test endpoint should not already be cached"
            );
            *cache = Some((
                endpoints.clone(),
                Instant::now(),
                "connection refused".to_owned(),
            ));
        }

        // Same endpoints, fresh failure: still within the negative-cache TTL.
        {
            let cache = open_client_failure_cache().lock().unwrap();
            let (cached_endpoints, failed_at, _err) = cache.as_ref().unwrap();
            assert_eq!(cached_endpoints, &endpoints);
            assert!(failed_at.elapsed() < OPEN_CLIENT_NEGATIVE_CACHE_TTL);
        }

        // A different endpoint list must never be treated as a cache hit for
        // this one -- open_client's lookup compares endpoints, not just
        // "is anything cached".
        {
            let cache = open_client_failure_cache().lock().unwrap();
            let (cached_endpoints, _, _) = cache.as_ref().unwrap();
            assert_ne!(cached_endpoints, &other_endpoints);
        }
    }

    #[test]
    fn node_cache_roundtrips_by_key() {
        // Doesn't touch the process-wide static cache (that would race with
        // other tests running in parallel); exercises the same LruCache type
        // and key shape directly.
        let mut cache: LruCache<Vec<u8>, Arc<HamtNode<String, String>>> =
            LruCache::new(NonZeroUsize::new(4).unwrap());
        // (matches the `NodeCache` value type used by the process-wide `node_cache()`)

        let key = node_tikv_key("test-namespace", &[0u8; ROOM_PREFIX_LEN], &[0u8; 16]);
        assert!(cache.get(&key).is_none());

        let node = Arc::new(HamtNode {
            datamap: 0,
            nodemap: 0,
            leaves: Vec::new(),
            children: Vec::new(),
            structural_hash: [0u8; 16],
        });
        cache.put(key.clone(), node.clone());

        let cached = cache.get(&key).expect("node should be cached after put");
        assert_eq!(cached.structural_hash, node.structural_hash);
    }

    #[test]
    fn retryable_key_error_does_not_retry_an_abort() {
        let key_err = tikv_client::ProtoKeyError::default();
        assert!(!is_retryable_key_error(&key_err));

        let retryable_err = tikv_client::ProtoKeyError {
            retryable: "retryable lock".to_string(),
            ..Default::default()
        };
        assert!(is_retryable_key_error(&retryable_err));

        let abort_err = tikv_client::ProtoKeyError {
            abort: "txn aborted".to_string(),
            ..Default::default()
        };
        assert!(!is_retryable_key_error(&abort_err));
    }

    #[test]
    fn retryable_region_error_detects_flags() {
        let region_err = tikv_client::ProtoRegionError::default();
        assert!(!is_retryable_region_error(&region_err));
    }

    #[test]
    fn fatal_errors_fail_fast_without_retrying() {
        assert!(!is_retryable_txn_error(&tikv_client::Error::Unimplemented));
        assert!(!is_retryable_txn_error(
            &tikv_client::Error::UnsupportedMode
        ));
        assert!(!is_retryable_txn_error(
            &tikv_client::Error::ColumnFamilyError("default".to_string())
        ));
        assert!(!is_retryable_txn_error(&tikv_client::Error::StringError(
            "generic string error".to_string()
        )));
    }

    #[test]
    fn wrapped_key_conflicts_are_retryable() {
        let conflict = tikv_client::Error::KeyError(Box::new(tikv_client::ProtoKeyError {
            retryable: "write conflict".to_owned(),
            ..Default::default()
        }));
        assert!(is_retryable_txn_error(
            &tikv_client::Error::MultipleKeyErrors(vec![conflict])
        ));

        assert!(!is_retryable_txn_error(
            &tikv_client::Error::MultipleKeyErrors(vec![tikv_client::Error::Unimplemented])
        ));
    }

    #[test]
    fn node_cache_with_selective_lookup() {
        use crate::state_hamt::{
            build_root_handle_and_nodes, decode_persisted_node, lookup_from_node_map,
            room_structural_key_raw,
        };

        let secret = [42u8; 32];
        let room_id = "!test_room:example.com";
        let structural_key = room_structural_key_raw(&secret, room_id);
        let entries = vec![
            (
                "m.room.name".to_string(),
                "".to_string(),
                "$event1".to_string(),
            ),
            (
                "m.room.topic".to_string(),
                "".to_string(),
                "$event2".to_string(),
            ),
        ];
        let ((root_hash, _sg), nodes) =
            build_root_handle_and_nodes(&secret, room_id, entries).unwrap();
        let mut node_map = HashMap::new();
        for (h, node_bytes) in nodes {
            let node = decode_persisted_node(&node_bytes).unwrap();
            node_map.insert(h, node);
        }

        let keys = vec![("m.room.name".to_string(), "".to_string())];
        let (found, missing) =
            lookup_from_node_map(&root_hash, &structural_key, &keys, &node_map).unwrap();
        assert!(missing.is_empty());
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, "m.room.name");
        assert_eq!(found[0].2, "$event1");
    }
}
