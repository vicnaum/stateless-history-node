use alloy_primitives::B256;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use reth_chainspec::MAINNET;
use reth_eth_wire::EthNetworkPrimitives;
use reth_eth_wire_types::{
    BlockHashOrNumber, EthVersion, GetBlockHeaders, GetReceipts, GetReceipts70, HeadersDirection,
    Receipts69, Receipts70,
};
use reth_network::config::{rng_secret_key, NetworkConfigBuilder};
use reth_network::import::ProofOfStakeBlockImport;
use reth_network_api::{
    test_utils::{PeersHandle, PeersHandleProvider},
    DiscoveredEvent, DiscoveryEvent, NetworkEvent, NetworkEventListenerProvider, PeerId,
    PeerRequest, PeerRequestSender,
};
use reth_network_api::events::PeerEvent;
use reth_network::PeersConfig;
use reth_primitives_traits::{Header, SealedHeader};
use serde_json::json;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CliConfig::parse();
    let default_level = if config.quiet {
        "error"
    } else if config.verbosity >= Verbosity::V3 {
        "info"
    } else if config.verbosity >= Verbosity::V2 {
        "warn"
    } else {
        "error"
    };
    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new(default_level)
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let log_flush_lines = config.log_flush_lines;
    let log_flush_ms = config.log_flush_ms;
    let run_start = Instant::now();

    let out_dir = PathBuf::from("output");
    std::fs::create_dir_all(&out_dir)?;
    let known_path = out_dir.join("known_blocks.jsonl");
    let known_blocks = load_known_blocks(&known_path);
    let targets = build_anchor_targets(config.anchor_window);
    let total_targets = targets.len();
    let pending_blocks: Vec<u64> = targets
        .into_iter()
        .filter(|block| !known_blocks.contains(block))
        .collect();
    let queued_blocks: HashSet<u64> = pending_blocks.iter().copied().collect();
    let pending_targets: BinaryHeap<Reverse<u64>> =
        pending_blocks.into_iter().map(Reverse).collect();

    let secret_key = rng_secret_key();
    let mut peers_config = PeersConfig::default()
        .with_max_outbound(config.max_outbound)
        .with_max_concurrent_dials(config.max_concurrent_dials)
        .with_refill_slots_interval(Duration::from_millis(config.refill_interval_ms));
    if let Some(max_inbound) = config.max_inbound {
        peers_config = peers_config.with_max_inbound(max_inbound);
    }

    let net_config = NetworkConfigBuilder::<EthNetworkPrimitives>::new(secret_key)
        .mainnet_boot_nodes()
        .with_unused_ports()
        .peer_config(peers_config)
        .disable_tx_gossip(true)
        .block_import(Box::new(ProofOfStakeBlockImport::default()))
        .build_with_noop_provider(MAINNET.clone());

    let handle = net_config.start_network().await?;
    let peers_handle = handle.peers_handle().clone();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    let context = Arc::new(RunContext {
        stats: Mutex::new(Stats::default()),
        queue: Mutex::new(pending_targets),
        known_blocks: Mutex::new(known_blocks),
        failed_blocks: Mutex::new(HashSet::new()),
        escalation_queue: Mutex::new(VecDeque::new()),
        escalation_queued: Mutex::new(HashSet::new()),
        escalation_attempts: Mutex::new(HashMap::new()),
        escalation_active: AtomicBool::new(false),
        queued_blocks: Mutex::new(queued_blocks),
        attempts: Mutex::new(HashMap::new()),
        peer_health: Mutex::new(HashMap::new()),
        active_peers: Mutex::new(HashSet::new()),
        peers_with_jobs: Mutex::new(HashSet::new()),
        window: Mutex::new(WindowStats::default()),
        config,
        peers_handle,
        logger: Logger::new(&out_dir, log_flush_lines)?,
        request_id: AtomicU64::new(1),
        total_targets,
        discovered: AtomicU64::new(0),
        sessions: AtomicU64::new(0),
        jobs_assigned: AtomicU64::new(0),
        in_flight: AtomicU64::new(0),
        run_start,
        shutdown_tx,
    });
    if log_flush_ms > 0 {
        context
            .logger
            .spawn_flush_tasks(Duration::from_millis(log_flush_ms));
    }

    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<PeerHandle>();

    let scheduler_context = Arc::clone(&context);
    let scheduler_ready_tx = ready_tx.clone();
    tokio::spawn(async move {
        let mut warmed_up = false;
        while let Some(peer) = ready_rx.recv().await {
            if !warmed_up {
                if WARMUP_SECS > 0 {
                    tokio::time::sleep(Duration::from_secs(WARMUP_SECS)).await;
                }
                warmed_up = true;
            }

            if let Some(remaining) = scheduler_context.peer_ban_remaining(&peer.peer_key).await {
                let ready_tx = scheduler_ready_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(remaining).await;
                    let _ = ready_tx.send(peer);
                });
                continue;
            }

            let batch = scheduler_context
                .next_batch_for_peer(&peer.peer_key, peer.head_number)
                .await;
            if batch.blocks.is_empty() {
                continue;
            }
            scheduler_context.record_peer_job(&peer.peer_key).await;
            scheduler_context.inc_in_flight();

            let context = Arc::clone(&scheduler_context);
            let ready_tx = scheduler_ready_tx.clone();
            tokio::spawn(async move {
                let peer_for_probe = peer.clone();
                let should_requeue = probe_block_batch(
                    peer_for_probe.peer_id,
                    peer_for_probe.peer_key,
                    peer_for_probe.messages,
                    peer_for_probe.eth_version,
                    batch.blocks,
                    batch.mode,
                    Arc::clone(&context),
                )
                .await;
                context.dec_in_flight();
                if should_requeue {
                    let _ = ready_tx.send(peer);
                }
            });
        }
    });

    info!(peer_id = ?handle.peer_id(), "network started");
    println!("receipt-harness: network started");

    let mut discovery_events = handle.discovery_listener();
    let context_for_discovery = Arc::clone(&context);
    tokio::spawn(async move {
        let mut discovered = 0usize;
        while let Some(event) = discovery_events.next().await {
            match event {
                DiscoveryEvent::NewNode(DiscoveredEvent::EventQueued {
                    peer_id,
                    addr,
                    fork_id,
                }) => {
                    discovered += 1;
                    context_for_discovery
                        .discovered
                        .fetch_add(1, Ordering::SeqCst);
                    info!(
                        peer_id = ?peer_id,
                        addr = ?addr,
                        fork_id = ?fork_id,
                        discovered,
                        "peer discovered"
                    );
                }
                DiscoveryEvent::EnrForkId(peer_id, fork_id) => {
                    info!(
                        peer_id = ?peer_id,
                        fork_id = ?fork_id,
                        "peer forkid from ENR"
                    );
                }
            }
        }
    });

    let mut event_listener = handle.event_listener();
    let context_for_events = Arc::clone(&context);
    let ready_tx_for_events = ready_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = event_listener.next().await {
            match event {
                NetworkEvent::ActivePeerSession { info, messages } => {
                    let matches_genesis = info.status.genesis == MAINNET.genesis_hash();
                    if !matches_genesis {
                        warn!(
                            peer_id = ?info.peer_id,
                            genesis = ?info.status.genesis,
                            "peer genesis mismatch"
                        );
                        continue;
                    }

                    info!(
                        peer_id = ?info.peer_id,
                        client_version = %info.client_version,
                        remote_addr = ?info.remote_addr,
                        eth_version = ?info.version,
                        chain = ?info.status.chain,
                        head_hash = ?info.status.blockhash,
                        "peer session established"
                    );

                    context_for_events
                        .stats
                        .lock()
                        .await
                        .record_peer(info.client_version.as_ref());

                    let peer_id = info.peer_id;
                    let peer_key = format!("{:?}", peer_id);
                    let head_hash = info.status.blockhash;
                    let messages = messages.clone();
                    let context = Arc::clone(&context_for_events);
                    let ready_tx = ready_tx_for_events.clone();

                    tokio::spawn(async move {
                        let head_start = Instant::now();
                        let head_number = match request_head_number(
                            peer_id,
                            info.version,
                            BlockHashOrNumber::Hash(head_hash),
                            &messages,
                            context.as_ref(),
                        )
                        .await
                        {
                            Ok(number) => {
                                info!(
                                    peer_id = ?peer_id,
                                    head_number = number,
                                    head_ms = head_start.elapsed().as_millis(),
                                    "peer head resolved"
                                );
                                number
                            }
                            Err(err) => {
                                let _ = context
                                    .handle_peer_failure(peer_id, &peer_key, "head")
                                    .await;
                                warn!(peer_id = ?peer_id, error = %err, "head header request failed");
                                return;
                            }
                        };

                        context.record_active_peer(peer_key.clone()).await;
                        let _ = ready_tx.send(PeerHandle {
                            peer_id,
                            peer_key,
                            eth_version: info.version,
                            head_number,
                            messages,
                        });
                    });
                }
                NetworkEvent::Peer(peer_event) => match peer_event {
                    PeerEvent::SessionClosed { peer_id, .. } | PeerEvent::PeerRemoved(peer_id) => {
                        let peer_key = format!("{:?}", peer_id);
                        context_for_events.record_peer_disconnect(&peer_key).await;
                    }
                    _ => {}
                },
            }
        }
    });

    let progress_rx = shutdown_rx.clone();
    spawn_progress_bar(Arc::clone(&context), progress_rx);
    spawn_window_stats(Arc::clone(&context));

    if let Some(seconds) = context.config.run_secs {
        info!(run_secs = seconds, "auto-shutdown enabled");
        tokio::select! {
            _ = wait_for_shutdown_signal() => {},
            _ = tokio::time::sleep(Duration::from_secs(seconds)) => {
                info!("auto-shutdown reached");
            },
            _ = shutdown_rx.changed() => {},
        }
    } else {
        tokio::select! {
            _ = wait_for_shutdown_signal() => {},
            _ = shutdown_rx.changed() => {},
        }
    }
    let _ = context.shutdown_tx.send(true);

    let completed_blocks = context.completed_blocks().await;
    let summary =
        summarize_stats(&context.stats, run_start.elapsed(), context.total_targets, completed_blocks)
            .await;
    context.logger.log_stats(summary).await;
    context.logger.flush_all().await;
    let missing_blocks = {
        let known = context.known_blocks.lock().await;
        match write_missing_blocks(&out_dir, context.config.anchor_window, &known) {
            Ok(count) => count,
            Err(err) => {
                warn!(error = ?err, "failed to write missing blocks");
                0
            }
        }
    };
    if !context.config.quiet {
        let (elapsed_secs, fetched_blocks, avg_speed, avg_window, max_speed, failed_blocks) = {
            let stats = context.stats.lock().await;
            let elapsed_secs = run_start.elapsed().as_secs_f64().max(0.001);
            let fetched_blocks = stats.receipts_ok_total;
            let avg_speed = fetched_blocks as f64 / elapsed_secs;
            let avg_window = if stats.window_count > 0 {
                stats.window_blocks_per_sec_sum / stats.window_count as f64
            } else {
                0.0
            };
            (
                elapsed_secs,
                fetched_blocks,
                avg_speed,
                avg_window,
                stats.window_blocks_per_sec_max,
                stats.failed_blocks_total,
            )
        };
        if failed_blocks > 0 {
            println!(
                "All done! Fetched {} blocks in {:.1}s ({} failed). Avg {:.2} blocks/s, window avg {:.2}, max {:.2}.",
                fetched_blocks,
                elapsed_secs,
                failed_blocks,
                avg_speed,
                avg_window,
                max_speed
            );
        } else {
            println!(
                "All done! Fetched {} blocks in {:.1}s. Avg {:.2} blocks/s, window avg {:.2}, max {:.2}.",
                fetched_blocks,
                elapsed_secs,
                avg_speed,
                avg_window,
                max_speed
            );
        }
        if missing_blocks > 0 {
            println!(
                "Missing blocks: {} (saved to output/missing_blocks.jsonl).",
                missing_blocks
            );
        }
    }
    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Verbosity {
    Minimal,
    V1,
    V2,
    V3,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ProbeMode {
    Normal,
    Escalation,
}

struct WorkBatch {
    blocks: Vec<u64>,
    mode: ProbeMode,
}

impl Verbosity {
    fn from_flag(flag: &str) -> Option<Self> {
        match flag {
            "-v" => Some(Self::V1),
            "-vv" => Some(Self::V2),
            "-vvv" => Some(Self::V3),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct CliConfig {
    run_secs: Option<u64>,
    max_outbound: usize,
    max_concurrent_dials: usize,
    refill_interval_ms: u64,
    max_inbound: Option<usize>,
    anchor_window: u64,
    blocks_per_assignment: usize,
    receipts_per_request: usize,
    request_timeout_ms: u64,
    log_flush_ms: u64,
    log_flush_lines: usize,
    verbosity: Verbosity,
    quiet: bool,
    peer_failure_threshold: u32,
    peer_ban_secs: u64,
    max_attempts: u32,
    stats_interval_secs: u64,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            run_secs: None,
            max_outbound: 400,
            max_concurrent_dials: 100,
            refill_interval_ms: 500,
            max_inbound: None,
            anchor_window: 10_000,
            blocks_per_assignment: 32,
            receipts_per_request: 16,
            request_timeout_ms: 4000,
            log_flush_ms: 1000,
            log_flush_lines: 200,
            verbosity: Verbosity::Minimal,
            quiet: false,
            peer_failure_threshold: 5,
            peer_ban_secs: 120,
            max_attempts: 3,
            stats_interval_secs: 10,
        }
    }
}

impl CliConfig {
    fn parse() -> Self {
        let mut config = Self::default();
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if let Some(level) = Verbosity::from_flag(&arg) {
                if level > config.verbosity {
                    config.verbosity = level;
                }
                continue;
            }
            match arg.as_str() {
                "--run-secs" => config.run_secs = parse_u64_arg(&mut args),
                "--max-outbound" => config.max_outbound = parse_usize_arg(&mut args, config.max_outbound),
                "--max-concurrent-dials" => {
                    config.max_concurrent_dials = parse_usize_arg(&mut args, config.max_concurrent_dials)
                }
                "--refill-interval-ms" => {
                    config.refill_interval_ms = parse_u64_arg(&mut args).unwrap_or(config.refill_interval_ms)
                }
                "--max-inbound" => config.max_inbound = parse_optional_usize_arg(&mut args),
                "--anchor-window" => {
                    config.anchor_window = parse_u64_arg(&mut args).unwrap_or(config.anchor_window)
                }
                "--blocks-per-assignment" => {
                    config.blocks_per_assignment = parse_usize_arg(&mut args, config.blocks_per_assignment)
                }
                "--receipts-per-request" => {
                    config.receipts_per_request = parse_usize_arg(&mut args, config.receipts_per_request)
                }
                "--request-timeout-ms" => {
                    config.request_timeout_ms = parse_u64_arg(&mut args).unwrap_or(config.request_timeout_ms)
                }
                "--log-flush-ms" => {
                    config.log_flush_ms = parse_u64_arg(&mut args).unwrap_or(config.log_flush_ms)
                }
                "--log-flush-lines" => {
                    config.log_flush_lines = parse_usize_arg(&mut args, config.log_flush_lines)
                }
                "--peer-failure-threshold" => {
                    config.peer_failure_threshold = parse_u32_arg(&mut args, config.peer_failure_threshold)
                }
                "--peer-ban-secs" => {
                    config.peer_ban_secs = parse_u64_arg(&mut args).unwrap_or(config.peer_ban_secs)
                }
                "--max-attempts" => config.max_attempts = parse_u32_arg(&mut args, config.max_attempts),
                "--stats-interval-secs" => {
                    config.stats_interval_secs = parse_u64_arg(&mut args).unwrap_or(config.stats_interval_secs)
                }
                "--quiet" => config.quiet = true,
                _ => {
                    if let Some(value) = arg.strip_prefix("--run-secs=") {
                        config.run_secs = value.parse().ok();
                    } else if let Some(value) = arg.strip_prefix("--max-outbound=") {
                        config.max_outbound = value.parse().unwrap_or(config.max_outbound);
                    } else if let Some(value) = arg.strip_prefix("--max-concurrent-dials=") {
                        config.max_concurrent_dials = value.parse().unwrap_or(config.max_concurrent_dials);
                    } else if let Some(value) = arg.strip_prefix("--refill-interval-ms=") {
                        config.refill_interval_ms = value.parse().unwrap_or(config.refill_interval_ms);
                    } else if let Some(value) = arg.strip_prefix("--max-inbound=") {
                        config.max_inbound = value.parse().ok();
                    } else if let Some(value) = arg.strip_prefix("--anchor-window=") {
                        config.anchor_window = value.parse().unwrap_or(config.anchor_window);
                    } else if let Some(value) = arg.strip_prefix("--blocks-per-assignment=") {
                        config.blocks_per_assignment = value.parse().unwrap_or(config.blocks_per_assignment);
                    } else if let Some(value) = arg.strip_prefix("--receipts-per-request=") {
                        config.receipts_per_request = value.parse().unwrap_or(config.receipts_per_request);
                    } else if let Some(value) = arg.strip_prefix("--request-timeout-ms=") {
                        config.request_timeout_ms = value.parse().unwrap_or(config.request_timeout_ms);
                    } else if let Some(value) = arg.strip_prefix("--log-flush-ms=") {
                        config.log_flush_ms = value.parse().unwrap_or(config.log_flush_ms);
                    } else if let Some(value) = arg.strip_prefix("--log-flush-lines=") {
                        config.log_flush_lines = value.parse().unwrap_or(config.log_flush_lines);
                    } else if let Some(value) = arg.strip_prefix("--peer-failure-threshold=") {
                        config.peer_failure_threshold =
                            value.parse().unwrap_or(config.peer_failure_threshold);
                    } else if let Some(value) = arg.strip_prefix("--peer-ban-secs=") {
                        config.peer_ban_secs = value.parse().unwrap_or(config.peer_ban_secs);
                    } else if let Some(value) = arg.strip_prefix("--max-attempts=") {
                        config.max_attempts = value.parse().unwrap_or(config.max_attempts);
                    } else if let Some(value) = arg.strip_prefix("--stats-interval-secs=") {
                        config.stats_interval_secs = value.parse().unwrap_or(config.stats_interval_secs);
                    }
                }
            }
        }
        config
    }
}

fn parse_u64_arg<I: Iterator<Item = String>>(args: &mut I) -> Option<u64> {
    args.next().and_then(|value| value.parse().ok())
}

fn parse_usize_arg<I: Iterator<Item = String>>(args: &mut I, fallback: usize) -> usize {
    args.next().and_then(|value| value.parse().ok()).unwrap_or(fallback)
}

fn parse_u32_arg<I: Iterator<Item = String>>(args: &mut I, fallback: u32) -> u32 {
    args.next().and_then(|value| value.parse().ok()).unwrap_or(fallback)
}

fn parse_optional_usize_arg<I: Iterator<Item = String>>(args: &mut I) -> Option<usize> {
    args.next().and_then(|value| value.parse().ok())
}

async fn request_head_number(
    peer_id: PeerId,
    eth_version: EthVersion,
    start_block: BlockHashOrNumber,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    context: &RunContext,
) -> Result<u64, String> {
    let request_id = context.next_request_id();
    let sent_at = now_ms();
    context
        .log_request(json!({
            "event": "request_sent",
            "request_id": request_id,
            "peer_id": format!("{:?}", peer_id),
            "block": format!("{start_block:?}"),
            "kind": "head_header",
            "eth_version": format!("{eth_version}"),
            "sent_at_ms": sent_at,
        }))
        .await;

    let request = GetBlockHeaders {
        start_block,
        limit: 1,
        skip: 0,
        direction: HeadersDirection::Rising,
    };
    let (tx, rx) = oneshot::channel();
    messages
        .try_send(PeerRequest::GetBlockHeaders { request, response: tx })
        .map_err(|err| format!("send error: {err:?}"))?;

    let response =
        match tokio::time::timeout(Duration::from_millis(context.config.request_timeout_ms), rx)
            .await
        {
            Ok(inner) => inner.map_err(|err| format!("response dropped: {err:?}"))?,
            Err(_) => {
                let received_at = now_ms();
                let duration_ms = received_at.saturating_sub(sent_at);
                context
                    .log_request(json!({
                        "event": "response_err",
                        "request_id": request_id,
                        "peer_id": format!("{:?}", peer_id),
                        "kind": "head_header",
                        "eth_version": format!("{eth_version}"),
                        "received_at_ms": received_at,
                        "duration_ms": duration_ms,
                        "error": "timeout",
                    }))
                    .await;
                return Err("timeout".to_string());
            }
        };

    let received_at = now_ms();
    let duration_ms = received_at.saturating_sub(sent_at);

    match response {
        Ok(headers) => {
            let head_number = headers
                .0
                .first()
                .map(|header| header.number)
                .ok_or_else(|| "empty head header response".to_string())?;
            context
                .log_request(json!({
                    "event": "response_ok",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "kind": "head_header",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "items": headers.0.len(),
                    "head_number": head_number,
                }))
                .await;
            Ok(head_number)
        }
        Err(err) => {
            context
                .log_request(json!({
                    "event": "response_err",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "kind": "head_header",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "error": format!("{err:?}"),
                }))
                .await;
            Err(format!("{err:?}"))
        }
    }
}

fn build_anchor_targets(anchor_window: u64) -> Vec<u64> {
    let mut targets = Vec::new();
    for anchor in ANCHORS {
        for offset in 0..anchor_window {
            targets.push(anchor + offset);
        }
    }

    targets.sort_unstable();
    targets.dedup();
    targets
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigint =
            signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn write_missing_blocks(
    out_dir: &PathBuf,
    anchor_window: u64,
    known_blocks: &HashSet<u64>,
) -> std::io::Result<usize> {
    let path = out_dir.join("missing_blocks.jsonl");
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);
    let mut missing = 0usize;
    for anchor in ANCHORS {
        for offset in 0..anchor_window {
            let block = anchor + offset;
            if !known_blocks.contains(&block) {
                writeln!(writer, "{}", json!({ "block": block }))?;
                missing += 1;
            }
        }
    }
    writer.flush()?;
    if missing == 0 {
        drop(writer);
        let _ = std::fs::remove_file(&path);
    }
    Ok(missing)
}

async fn probe_block_batch(
    peer_id: PeerId,
    peer_key: String,
    messages: PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    eth_version: EthVersion,
    blocks: Vec<u64>,
    mode: ProbeMode,
    context: Arc<RunContext>,
) -> bool {
    if blocks.is_empty() {
        return true;
    }
    let is_escalation = mode == ProbeMode::Escalation;
    let header_start = Instant::now();
    let header_result = request_headers_batch(
        peer_id,
        eth_version,
        &messages,
        blocks[0],
        blocks.len(),
        context.as_ref(),
    )
    .await;
    let header_ms = header_start.elapsed().as_millis();

    let headers = match header_result {
        Ok(headers) => {
            context.record_peer_success(&peer_key).await;
            headers
        }
        Err(err) => {
            let peer_banned = context
                .handle_peer_failure(peer_id, &peer_key, "header")
                .await;
            warn!(
                peer_id = ?peer_id,
                block_start = blocks[0],
                block_count = blocks.len(),
                header_ms,
                error = %err,
                "probe header batch failed"
            );
            for block in &blocks {
                let (attempt, retry) = if is_escalation {
                    let failure_reason = if err == "timeout" {
                        "escalation_header_timeout"
                    } else {
                        "escalation_header_batch"
                    };
                    context.record_failure_reason(failure_reason).await;
                    context.requeue_escalation_block(*block).await;
                    (None, false)
                } else {
                    let attempt = context.record_attempt(*block).await;
                    let retry = attempt <= context.config.max_attempts;
                    if retry {
                        context.requeue_blocks(&[*block]).await;
                        context.record_retry().await;
                    } else {
                        context.mark_failed_block(*block).await;
                    }
                    let failure_reason = if err == "timeout" {
                        "header_timeout"
                    } else {
                        "header_batch"
                    };
                    context.record_failure_reason(failure_reason).await;
                    (Some(attempt), retry)
                };
                context
                    .stats
                    .lock()
                    .await
                    .record_probe(&peer_key, *block, false, false, header_ms, None);
                context
                    .update_window(&peer_key, header_ms, None, false)
                    .await;
                let payload = json!({
                    "event": "probe",
                    "peer_id": format!("{:?}", peer_id),
                    "block": block,
                    "eth_version": format!("{eth_version}"),
                    "header_ok": false,
                    "receipts_ok": false,
                    "header_ms": header_ms,
                    "attempt": attempt,
                    "will_retry": retry,
                    "mode": if is_escalation { "escalation" } else { "normal" },
                    "error": err,
                });
                if !context.config.quiet && context.config.verbosity >= Verbosity::V3 {
                    println!("{}", payload);
                }
                context.log_probe(payload).await;
            }
            return !peer_banned;
        }
    };

    let mut headers_by_number = HashMap::new();
    for header in headers {
        headers_by_number.insert(header.number, header);
    }

    let mut hashes = Vec::new();
    let mut missing = Vec::new();
    for block in &blocks {
        match headers_by_number.remove(block) {
            Some(header) => {
                if header.number != *block {
                    warn!(
                        peer_id = ?peer_id,
                        requested = block,
                        received = header.number,
                        "probe header mismatch"
                    );
                }
                let hash = SealedHeader::seal_slow(header).hash();
                hashes.push((*block, hash));
            }
            None => missing.push(*block),
        }
    }

    if !missing.is_empty() {
        warn!(
            peer_id = ?peer_id,
            missing = missing.len(),
            "probe header missing in batch"
        );
        for block in &missing {
            let (attempt, retry) = if is_escalation {
                context.record_failure_reason("escalation_missing_header").await;
                context.requeue_escalation_block(*block).await;
                (None, false)
            } else {
                let attempt = context.record_attempt(*block).await;
                let retry = attempt <= context.config.max_attempts;
                if retry {
                    context.requeue_blocks(&[*block]).await;
                    context.record_retry().await;
                } else {
                    context.mark_failed_block(*block).await;
                }
                context.record_failure_reason("missing_header").await;
                (Some(attempt), retry)
            };
            context
                .stats
                .lock()
                .await
                .record_probe(&peer_key, *block, false, false, header_ms, None);
            context
                .update_window(&peer_key, header_ms, None, false)
                .await;
            context
                .update_window(&peer_key, header_ms, None, false)
                .await;
            let payload = json!({
                "event": "probe",
                "peer_id": format!("{:?}", peer_id),
                "block": block,
                "eth_version": format!("{eth_version}"),
                "header_ok": false,
                "receipts_ok": false,
                "header_ms": header_ms,
                "attempt": attempt,
                "will_retry": retry,
                "mode": if is_escalation { "escalation" } else { "normal" },
                "error": "missing_header",
            });
            if !context.config.quiet && context.config.verbosity >= Verbosity::V3 {
                println!("{}", payload);
            }
            context.log_probe(payload).await;
        }
    }

    for chunk in hashes.chunks(context.config.receipts_per_request.max(1)) {
        let chunk_blocks: Vec<u64> = chunk.iter().map(|(block, _)| *block).collect();
        let chunk_hashes: Vec<B256> = chunk.iter().map(|(_, hash)| *hash).collect();
        let receipts_start = Instant::now();
        let receipts_result = match eth_version {
            EthVersion::Eth69 => {
                request_receipts69(
                    peer_id,
                    eth_version,
                    &messages,
                    &chunk_blocks,
                    &chunk_hashes,
                    context.as_ref(),
                )
                .await
            }
            EthVersion::Eth70 => {
                request_receipts70(
                    peer_id,
                    eth_version,
                    &messages,
                    &chunk_blocks,
                    &chunk_hashes,
                    context.as_ref(),
                )
                .await
            }
            _ => {
                request_receipts_legacy(
                    peer_id,
                    eth_version,
                    &messages,
                    &chunk_blocks,
                    &chunk_hashes,
                    context.as_ref(),
                )
                .await
            }
        };
        let receipts_ms = receipts_start.elapsed().as_millis();

        match receipts_result {
            Ok(counts) => {
                context.record_peer_success(&peer_key).await;
                for (idx, block) in chunk_blocks.iter().enumerate() {
                    if let Some(receipt_count) = counts.get(idx).copied() {
                    context
                        .stats
                        .lock()
                        .await
                        .record_probe(&peer_key, *block, true, true, header_ms, Some(receipts_ms));
                        context
                            .update_window(&peer_key, header_ms, Some(receipts_ms), true)
                            .await;
                        context.mark_known_block(*block).await;
                        let payload = json!({
                            "event": "probe",
                            "peer_id": format!("{:?}", peer_id),
                            "block": block,
                            "eth_version": format!("{eth_version}"),
                            "header_ok": true,
                            "receipts_ok": true,
                            "header_ms": header_ms,
                            "receipts_ms": receipts_ms,
                            "receipts": receipt_count,
                            "mode": if is_escalation { "escalation" } else { "normal" },
                        });
                        if !context.config.quiet && context.config.verbosity >= Verbosity::V3 {
                            println!("{}", payload);
                        }
                        context.log_probe(payload).await;
                    } else {
                        let (attempt, retry) = if is_escalation {
                            context
                                .record_failure_reason("escalation_receipt_count_missing")
                                .await;
                            context.requeue_escalation_block(*block).await;
                            (None, false)
                        } else {
                            let attempt = context.record_attempt(*block).await;
                            let retry = attempt <= context.config.max_attempts;
                            if retry {
                                context.requeue_blocks(&[*block]).await;
                                context.record_retry().await;
                            } else {
                                context.mark_failed_block(*block).await;
                            }
                            context.record_failure_reason("receipt_count_missing").await;
                            (Some(attempt), retry)
                        };
                        context
                            .stats
                            .lock()
                            .await
                            .record_probe(&peer_key, *block, true, false, header_ms, Some(receipts_ms));
                        context
                            .update_window(&peer_key, header_ms, Some(receipts_ms), false)
                            .await;
                        let payload = json!({
                            "event": "probe",
                            "peer_id": format!("{:?}", peer_id),
                            "block": block,
                            "eth_version": format!("{eth_version}"),
                            "header_ok": true,
                            "receipts_ok": false,
                            "header_ms": header_ms,
                            "receipts_ms": receipts_ms,
                            "attempt": attempt,
                            "will_retry": retry,
                            "mode": if is_escalation { "escalation" } else { "normal" },
                            "error": "receipt_count_missing",
                        });
                        if !context.config.quiet && context.config.verbosity >= Verbosity::V3 {
                            println!("{}", payload);
                        }
                        context.log_probe(payload).await;
                    }
                }
            }
            Err(err) => {
                let peer_banned = context
                    .handle_peer_failure(peer_id, &peer_key, "receipts")
                    .await;
                warn!(
                    peer_id = ?peer_id,
                    blocks = chunk_blocks.len(),
                    header_ms,
                    receipts_ms,
                    error = %err,
                    "receipts batch failed"
                );
                for block in &chunk_blocks {
                    let (attempt, retry) = if is_escalation {
                        let failure_reason = if err == "timeout" {
                            "escalation_receipts_timeout"
                        } else {
                            "escalation_receipts_batch"
                        };
                        context.record_failure_reason(failure_reason).await;
                        context.requeue_escalation_block(*block).await;
                        (None, false)
                    } else {
                        let attempt = context.record_attempt(*block).await;
                        let retry = attempt <= context.config.max_attempts;
                        if retry {
                            context.requeue_blocks(&[*block]).await;
                            context.record_retry().await;
                        } else {
                            context.mark_failed_block(*block).await;
                        }
                        let failure_reason = if err == "timeout" {
                            "receipts_timeout"
                        } else {
                            "receipts_batch"
                        };
                        context.record_failure_reason(failure_reason).await;
                        (Some(attempt), retry)
                    };
                    context
                        .stats
                        .lock()
                        .await
                        .record_probe(&peer_key, *block, true, false, header_ms, Some(receipts_ms));
                    context
                        .update_window(&peer_key, header_ms, Some(receipts_ms), false)
                        .await;
                    let payload = json!({
                        "event": "probe",
                        "peer_id": format!("{:?}", peer_id),
                        "block": block,
                        "eth_version": format!("{eth_version}"),
                        "header_ok": true,
                        "receipts_ok": false,
                        "header_ms": header_ms,
                        "receipts_ms": receipts_ms,
                        "attempt": attempt,
                        "will_retry": retry,
                        "mode": if is_escalation { "escalation" } else { "normal" },
                        "error": err,
                    });
                    if !context.config.quiet && context.config.verbosity >= Verbosity::V3 {
                        println!("{}", payload);
                    }
                    context.log_probe(payload).await;
                }
                if peer_banned {
                    return false;
                }
            }
        }
    }
    true
}

async fn request_headers_batch(
    peer_id: PeerId,
    eth_version: EthVersion,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    start_block: u64,
    limit: usize,
    context: &RunContext,
) -> Result<Vec<Header>, String> {
    let request_id = context.next_request_id();
    let sent_at = now_ms();
    context
        .log_request(json!({
            "event": "request_sent",
            "request_id": request_id,
            "peer_id": format!("{:?}", peer_id),
            "block_start": start_block,
            "block_count": limit,
            "kind": "headers",
            "eth_version": format!("{eth_version}"),
            "sent_at_ms": sent_at,
        }))
        .await;

    let request = GetBlockHeaders {
        start_block: BlockHashOrNumber::Number(start_block),
        limit: limit as u64,
        skip: 0,
        direction: HeadersDirection::Rising,
    };
    let (tx, rx) = oneshot::channel();
    messages
        .try_send(PeerRequest::GetBlockHeaders { request, response: tx })
        .map_err(|err| format!("send error: {err:?}"))?;

    let response =
        match tokio::time::timeout(Duration::from_millis(context.config.request_timeout_ms), rx)
            .await
        {
            Ok(inner) => inner.map_err(|err| format!("response dropped: {err:?}"))?,
            Err(_) => {
                let received_at = now_ms();
                let duration_ms = received_at.saturating_sub(sent_at);
                context
                    .log_request(json!({
                        "event": "response_err",
                        "request_id": request_id,
                        "peer_id": format!("{:?}", peer_id),
                        "block_start": start_block,
                        "block_count": limit,
                        "kind": "headers",
                        "eth_version": format!("{eth_version}"),
                        "received_at_ms": received_at,
                        "duration_ms": duration_ms,
                        "error": "timeout",
                    }))
                    .await;
                return Err("timeout".to_string());
            }
        };

    let received_at = now_ms();
    let duration_ms = received_at.saturating_sub(sent_at);

    match response {
        Ok(headers) => {
            context
                .log_request(json!({
                    "event": "response_ok",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": start_block,
                    "block_count": limit,
                    "kind": "headers",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "items": headers.0.len(),
                }))
                .await;
            Ok(headers.0)
        }
        Err(err) => {
            context
                .log_request(json!({
                    "event": "response_err",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": start_block,
                    "block_count": limit,
                    "kind": "headers",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "error": format!("{err:?}"),
                }))
                .await;
            Err(format!("{err:?}"))
        }
    }
}

struct LogWriter {
    writer: BufWriter<File>,
    lines_since_flush: usize,
}

type SharedLogWriter = Arc<Mutex<LogWriter>>;

struct Logger {
    requests: SharedLogWriter,
    probes: SharedLogWriter,
    known: SharedLogWriter,
    stats: SharedLogWriter,
    flush_every_lines: usize,
}

impl Logger {
    fn new(out_dir: &PathBuf, flush_every_lines: usize) -> std::io::Result<Self> {
        let requests = open_log(out_dir.join("requests.jsonl"))?;
        let probes = open_log(out_dir.join("probes.jsonl"))?;
        let known = open_log(out_dir.join("known_blocks.jsonl"))?;
        let stats = open_log(out_dir.join("stats.jsonl"))?;
        Ok(Self {
            requests,
            probes,
            known,
            stats,
            flush_every_lines: flush_every_lines.max(1),
        })
    }

    fn spawn_flush_tasks(&self, interval: Duration) {
        spawn_flush_task(self.requests.clone(), interval);
        spawn_flush_task(self.probes.clone(), interval);
        spawn_flush_task(self.known.clone(), interval);
        spawn_flush_task(self.stats.clone(), interval);
    }

    async fn flush_all(&self) {
        flush_writer(&self.requests).await;
        flush_writer(&self.probes).await;
        flush_writer(&self.known).await;
        flush_writer(&self.stats).await;
    }

    async fn log_request(&self, value: serde_json::Value) {
        write_json_line(&self.requests, self.flush_every_lines, value).await;
    }

    async fn log_probe(&self, value: serde_json::Value) {
        write_json_line(&self.probes, self.flush_every_lines, value).await;
    }

    async fn log_known(&self, block: u64) {
        write_json_line(
            &self.known,
            self.flush_every_lines,
            json!({
                "block": block,
                "at_ms": now_ms(),
            }),
        )
        .await;
    }

    async fn log_stats(&self, value: serde_json::Value) {
        write_json_line(&self.stats, self.flush_every_lines, value).await;
    }
}

struct RunContext {
    stats: Mutex<Stats>,
    queue: Mutex<BinaryHeap<Reverse<u64>>>,
    queued_blocks: Mutex<HashSet<u64>>,
    known_blocks: Mutex<HashSet<u64>>,
    failed_blocks: Mutex<HashSet<u64>>,
    escalation_queue: Mutex<VecDeque<u64>>,
    escalation_queued: Mutex<HashSet<u64>>,
    escalation_attempts: Mutex<HashMap<u64, HashSet<String>>>,
    escalation_active: AtomicBool,
    attempts: Mutex<HashMap<u64, u32>>,
    peer_health: Mutex<HashMap<String, PeerHealth>>,
    active_peers: Mutex<HashSet<String>>,
    peers_with_jobs: Mutex<HashSet<String>>,
    window: Mutex<WindowStats>,
    config: CliConfig,
    peers_handle: PeersHandle,
    logger: Logger,
    request_id: AtomicU64,
    total_targets: usize,
    discovered: AtomicU64,
    sessions: AtomicU64,
    jobs_assigned: AtomicU64,
    in_flight: AtomicU64,
    run_start: Instant,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl RunContext {
    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    async fn pop_next_batch_for_head(&self, head_number: u64) -> Vec<u64> {
        let mut queued = self.queued_blocks.lock().await;
        let mut queue = self.queue.lock().await;
        let mut batch = Vec::new();
        let mut last = None;
        while let Some(Reverse(next)) = queue.peek().copied() {
            if next > head_number {
                break;
            }
            if batch.len() >= self.config.blocks_per_assignment {
                break;
            }
            if let Some(prev) = last {
                if next != prev + 1 {
                    break;
                }
            }
            queue.pop();
            queued.remove(&next);
            batch.push(next);
            last = Some(next);
        }
        batch
    }

    async fn next_batch_for_peer(&self, peer_key: &str, head_number: u64) -> WorkBatch {
        let batch = self.pop_next_batch_for_head(head_number).await;
        if !batch.is_empty() {
            return WorkBatch {
                blocks: batch,
                mode: ProbeMode::Normal,
            };
        }
        self.maybe_start_escalation().await;
        let escalation = self.pop_escalation_for_peer(peer_key).await;
        WorkBatch {
            blocks: escalation,
            mode: ProbeMode::Escalation,
        }
    }

    async fn maybe_start_escalation(&self) {
        if self.escalation_active.load(Ordering::SeqCst) {
            return;
        }
        if self.queue_len().await > 0 {
            return;
        }
        let failed_blocks: Vec<u64> = {
            let failed = self.failed_blocks.lock().await;
            failed.iter().copied().collect()
        };
        if failed_blocks.is_empty() {
            return;
        }
        let mut queue = self.escalation_queue.lock().await;
        let mut queued = self.escalation_queued.lock().await;
        for block in failed_blocks {
            if queued.insert(block) {
                queue.push_back(block);
            }
        }
        self.escalation_active.store(true, Ordering::SeqCst);
    }

    async fn pop_escalation_for_peer(&self, peer_key: &str) -> Vec<u64> {
        let active_peers = self.active_peers_count().await;
        let mut exhausted_count = 0usize;
        let result = {
            let mut queue = self.escalation_queue.lock().await;
            let mut queued = self.escalation_queued.lock().await;
            let known = self.known_blocks.lock().await;
            let mut attempts = self.escalation_attempts.lock().await;
            let mut iterations = queue.len();
            while iterations > 0 {
                iterations -= 1;
                if let Some(block) = queue.pop_front() {
                    queued.remove(&block);
                    if known.contains(&block) {
                        continue;
                    }
                    let entry = attempts.entry(block).or_default();
                    if entry.contains(peer_key) {
                        queue.push_back(block);
                        queued.insert(block);
                        continue;
                    }
                    if active_peers > 0 && entry.len() >= active_peers {
                        exhausted_count += 1;
                        continue;
                    }
                    entry.insert(peer_key.to_string());
                    return vec![block];
                }
            }
            Vec::new()
        };
        if exhausted_count > 0 {
            for _ in 0..exhausted_count {
                self.record_failure_reason("escalation_exhausted").await;
            }
        }
        result
    }

    async fn requeue_escalation_block(&self, block: u64) {
        let active_peers = self.active_peers_count().await;
        let tried = {
            let attempts = self.escalation_attempts.lock().await;
            attempts.get(&block).map(|s| s.len()).unwrap_or(0)
        };
        if active_peers == 0 {
            self.record_failure_reason("escalation_no_peers").await;
            return;
        }
        if tried >= active_peers {
            self.record_failure_reason("escalation_exhausted").await;
            return;
        }
        let known = self.known_blocks.lock().await;
        if known.contains(&block) {
            return;
        }
        let mut queue = self.escalation_queue.lock().await;
        let mut queued = self.escalation_queued.lock().await;
        if queued.insert(block) {
            queue.push_back(block);
        }
    }

    async fn escalation_len(&self) -> usize {
        self.escalation_queue.lock().await.len()
    }

    async fn queue_len(&self) -> usize {
        self.queue.lock().await.len()
    }

    async fn log_request(&self, value: serde_json::Value) {
        if !self.config.quiet && self.config.verbosity >= Verbosity::V3 {
            println!("{value}");
        }
        self.logger.log_request(value).await;
    }

    async fn log_probe(&self, value: serde_json::Value) {
        self.logger.log_probe(value).await;
    }

    async fn mark_known_block(&self, block: u64) {
        let inserted = {
            let mut known = self.known_blocks.lock().await;
            known.insert(block)
        };
        if inserted {
            self.logger.log_known(block).await;
        }
        self.clear_failed_block(block).await;
        let mut attempts = self.attempts.lock().await;
        attempts.remove(&block);
    }

    async fn record_active_peer(&self, peer_key: String) {
        let mut active = self.active_peers.lock().await;
        if active.insert(peer_key) {
            self.sessions.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn record_peer_disconnect(&self, peer_key: &str) {
        let mut active = self.active_peers.lock().await;
        if active.remove(peer_key) {
            let current = self.sessions.load(Ordering::SeqCst);
            if current > 0 {
                self.sessions.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    async fn active_peers_count(&self) -> usize {
        self.active_peers.lock().await.len()
    }

    async fn escalation_attempted_blocks(&self) -> usize {
        self.escalation_attempts.lock().await.len()
    }

    async fn record_peer_job(&self, peer_key: &str) {
        self.jobs_assigned.fetch_add(1, Ordering::SeqCst);
        let mut peers = self.peers_with_jobs.lock().await;
        peers.insert(peer_key.to_string());
    }

    fn inc_in_flight(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
    }

    fn dec_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    async fn completed_blocks(&self) -> usize {
        self.known_blocks.lock().await.len()
    }

    async fn update_window(
        &self,
        peer_key: &str,
        header_ms: u128,
        receipts_ms: Option<u128>,
        receipts_ok: bool,
    ) {
        let mut window = self.window.lock().await;
        window.record(peer_key, header_ms, receipts_ms, receipts_ok);
    }

    async fn peer_ban_remaining(&self, peer_key: &str) -> Option<Duration> {
        let mut health = self.peer_health.lock().await;
        let entry = health.get_mut(peer_key)?;
        if let Some(until) = entry.ban_until {
            let remaining = until.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                entry.ban_until = None;
                None
            } else {
                Some(remaining)
            }
        } else {
            None
        }
    }

    async fn record_peer_failure(&self, peer_key: &str) -> Option<PeerBan> {
        let mut health = self.peer_health.lock().await;
        let entry = health.entry(peer_key.to_string()).or_default();
        entry.consecutive_failures += 1;
        if entry.consecutive_failures >= self.config.peer_failure_threshold {
            let failures = entry.consecutive_failures;
            entry.consecutive_failures = 0;
            entry.ban_until = Some(Instant::now() + Duration::from_secs(self.config.peer_ban_secs));
            return Some(PeerBan {
                ban_secs: self.config.peer_ban_secs,
                failures,
            });
        }
        None
    }

    async fn record_peer_success(&self, peer_key: &str) {
        let mut health = self.peer_health.lock().await;
        if let Some(entry) = health.get_mut(peer_key) {
            entry.consecutive_failures = 0;
            entry.ban_until = None;
        }
    }

    async fn record_failure_reason(&self, reason: &str) {
        self.stats
            .lock()
            .await
            .record_failure_reason(reason);
    }

    async fn record_retry(&self) {
        self.stats.lock().await.record_retry();
    }

    async fn failed_blocks_total(&self) -> u64 {
        self.failed_blocks.lock().await.len() as u64
    }

    async fn mark_failed_block(&self, block: u64) {
        let mut failed = self.failed_blocks.lock().await;
        if failed.insert(block) {
            self.stats.lock().await.record_failed_block();
        }
    }

    async fn clear_failed_block(&self, block: u64) {
        let mut failed = self.failed_blocks.lock().await;
        if failed.remove(&block) {
            self.stats.lock().await.record_failed_block_recovered();
        }
        let mut attempts = self.escalation_attempts.lock().await;
        attempts.remove(&block);
    }

    async fn record_window(&self, blocks_per_sec: f64) {
        self.stats.lock().await.record_window(blocks_per_sec);
    }

    async fn record_drain(&self, secs: f64) {
        self.stats.lock().await.record_drain(secs);
    }

    async fn handle_peer_failure(&self, peer_id: PeerId, peer_key: &str, reason: &str) -> bool {
        if let Some(ban) = self.record_peer_failure(peer_key).await {
            self.log_request(json!({
                "event": "peer_ban",
                "peer_id": peer_key,
                "reason": reason,
                "ban_secs": ban.ban_secs,
                "failures": ban.failures,
                "at_ms": now_ms(),
            }))
            .await;
            self.peers_handle.remove_peer(peer_id);
            warn!(
                peer_id = ?peer_id,
                ban_secs = ban.ban_secs,
                failures = ban.failures,
                reason = reason,
                "peer temporarily banned"
            );
            return true;
        }
        false
    }

    async fn record_attempt(&self, block: u64) -> u32 {
        let mut attempts = self.attempts.lock().await;
        let entry = attempts.entry(block).or_insert(0);
        *entry += 1;
        *entry
    }

    async fn requeue_blocks(&self, blocks: &[u64]) {
        let known = self.known_blocks.lock().await;
        let mut queued = self.queued_blocks.lock().await;
        let mut queue = self.queue.lock().await;
        for block in blocks {
            if !known.contains(block) && queued.insert(*block) {
                queue.push(Reverse(*block));
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

fn open_log(path: PathBuf) -> std::io::Result<SharedLogWriter> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(Arc::new(Mutex::new(LogWriter {
        writer: BufWriter::new(file),
        lines_since_flush: 0,
    })))
}

async fn write_json_line(
    writer: &SharedLogWriter,
    flush_every_lines: usize,
    value: serde_json::Value,
) {
    let mut guard = writer.lock().await;
    if let Err(err) = writeln!(guard.writer, "{value}") {
        warn!(error = ?err, "failed to write log line");
    }
    guard.lines_since_flush += 1;
    if guard.lines_since_flush >= flush_every_lines {
        if let Err(err) = guard.writer.flush() {
            warn!(error = ?err, "failed to flush log");
        }
        guard.lines_since_flush = 0;
    }
}

async fn flush_writer(writer: &SharedLogWriter) {
    let mut guard = writer.lock().await;
    if let Err(err) = guard.writer.flush() {
        warn!(error = ?err, "failed to flush log");
    }
    guard.lines_since_flush = 0;
}

fn spawn_flush_task(writer: SharedLogWriter, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            flush_writer(&writer).await;
        }
    });
}

fn load_known_blocks(path: &PathBuf) -> HashSet<u64> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return HashSet::new(),
    };
    let reader = BufReader::new(file);
    let mut blocks = HashSet::new();
    for line in reader.lines().flatten() {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
            if let Some(block) = value.get("block").and_then(|v| v.as_u64()) {
                blocks.insert(block);
            }
        }
    }
    blocks
}

#[derive(Clone)]
struct PeerHandle {
    peer_id: PeerId,
    peer_key: String,
    eth_version: EthVersion,
    head_number: u64,
    messages: PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
}

const ANCHORS: [u64; 5] = [10_000_835, 11_362_579, 12_369_621, 16_291_127, 21_688_329];
const WARMUP_SECS: u64 = 3;

#[derive(Default)]
struct PeerHealth {
    consecutive_failures: u32,
    ban_until: Option<Instant>,
}

struct PeerBan {
    ban_secs: u64,
    failures: u32,
}

#[derive(Default)]
struct WindowStats {
    blocks_total: u64,
    blocks_ok: u64,
    header_ms: Vec<u128>,
    receipts_ms: Vec<u128>,
    peers: HashSet<String>,
}

impl WindowStats {
    fn record(
        &mut self,
        peer_key: &str,
        header_ms: u128,
        receipts_ms: Option<u128>,
        receipts_ok: bool,
    ) {
        self.blocks_total += 1;
        if receipts_ok {
            self.blocks_ok += 1;
        }
        self.header_ms.push(header_ms);
        if let Some(ms) = receipts_ms {
            self.receipts_ms.push(ms);
        }
        self.peers.insert(peer_key.to_string());
    }

    fn take_snapshot(&mut self) -> WindowSnapshot {
        let snapshot = WindowSnapshot {
            blocks_total: self.blocks_total,
            blocks_ok: self.blocks_ok,
            header_ms: std::mem::take(&mut self.header_ms),
            receipts_ms: std::mem::take(&mut self.receipts_ms),
            peers: std::mem::take(&mut self.peers),
        };
        self.blocks_total = 0;
        self.blocks_ok = 0;
        snapshot
    }
}

struct WindowSnapshot {
    blocks_total: u64,
    blocks_ok: u64,
    header_ms: Vec<u128>,
    receipts_ms: Vec<u128>,
    peers: HashSet<String>,
}

#[derive(Default)]
struct BlockStats {
    total: u64,
    header_ok: u64,
    receipts_ok: u64,
    header_ms_sum: u128,
    receipts_ms_sum: u128,
}

#[derive(Default)]
struct PeerStats {
    blocks_total: u64,
    blocks_ok: u64,
    header_ok: u64,
    receipts_ok: u64,
    header_ms_sum: u128,
    receipts_ms_sum: u128,
}

#[derive(Default)]
struct Stats {
    by_block: HashMap<u64, BlockStats>,
    clients: HashMap<String, u64>,
    peer_stats: HashMap<String, PeerStats>,
    failure_reasons: HashMap<String, u64>,
    total_probes: u64,
    receipts_ok_total: u64,
    retries_total: u64,
    failed_blocks_total: u64,
    header_ms_all: Vec<u128>,
    receipts_ms_all: Vec<u128>,
    window_blocks_per_sec_sum: f64,
    window_blocks_per_sec_max: f64,
    window_count: u64,
    drain_time_secs: Option<f64>,
}

impl Stats {
    fn record_peer(&mut self, client_version: &str) {
        *self.clients.entry(client_version.to_string()).or_insert(0) += 1;
    }

    fn record_probe(
        &mut self,
        peer_key: &str,
        block: u64,
        header_ok: bool,
        receipts_ok: bool,
        header_ms: u128,
        receipts_ms: Option<u128>,
    ) {
        let stats = self.by_block.entry(block).or_default();
        stats.total += 1;
        self.total_probes += 1;
        let peer = self.peer_stats.entry(peer_key.to_string()).or_default();
        peer.blocks_total += 1;
        if header_ok {
            stats.header_ok += 1;
            stats.header_ms_sum += header_ms;
            peer.header_ok += 1;
            peer.header_ms_sum += header_ms;
            self.header_ms_all.push(header_ms);
        }
        if receipts_ok {
            stats.receipts_ok += 1;
            self.receipts_ok_total += 1;
            peer.blocks_ok += 1;
            peer.receipts_ok += 1;
            if let Some(ms) = receipts_ms {
                stats.receipts_ms_sum += ms;
                peer.receipts_ms_sum += ms;
                self.receipts_ms_all.push(ms);
            }
        }
    }

    fn record_retry(&mut self) {
        self.retries_total += 1;
    }

    fn record_failed_block(&mut self) {
        self.failed_blocks_total += 1;
    }

    fn record_failed_block_recovered(&mut self) {
        if self.failed_blocks_total > 0 {
            self.failed_blocks_total -= 1;
        }
    }

    fn record_failure_reason(&mut self, reason: &str) {
        *self.failure_reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    fn record_window(&mut self, blocks_per_sec: f64) {
        self.window_blocks_per_sec_sum += blocks_per_sec;
        if blocks_per_sec > self.window_blocks_per_sec_max {
            self.window_blocks_per_sec_max = blocks_per_sec;
        }
        self.window_count += 1;
    }

    fn record_drain(&mut self, secs: f64) {
        if self.drain_time_secs.is_none() {
            self.drain_time_secs = Some(secs);
        }
    }
}

async fn summarize_stats(
    stats: &Mutex<Stats>,
    elapsed: Duration,
    total_targets: usize,
    completed_blocks: usize,
) -> serde_json::Value {
    let stats = stats.lock().await;
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let throughput_blocks = stats.receipts_ok_total as f64 / elapsed_secs;
    let coverage = if total_targets > 0 {
        completed_blocks as f64 / total_targets as f64
    } else {
        0.0
    };
    let avg_window_blocks_per_sec = if stats.window_count > 0 {
        stats.window_blocks_per_sec_sum / stats.window_count as f64
    } else {
        0.0
    };
    let mut blocks: Vec<_> = stats.by_block.iter().map(|(block, stats)| {
        let header_rate = if stats.total > 0 {
            stats.header_ok as f64 / stats.total as f64
        } else {
            0.0
        };
        let receipts_rate = if stats.total > 0 {
            stats.receipts_ok as f64 / stats.total as f64
        } else {
            0.0
        };
        let avg_header_ms = if stats.header_ok > 0 {
            stats.header_ms_sum as f64 / stats.header_ok as f64
        } else {
            0.0
        };
        let avg_receipts_ms = if stats.receipts_ok > 0 {
            stats.receipts_ms_sum as f64 / stats.receipts_ok as f64
        } else {
            0.0
        };
        json!({
            "block": block,
            "total": stats.total,
            "header_ok": stats.header_ok,
            "receipts_ok": stats.receipts_ok,
            "header_rate": header_rate,
            "receipts_rate": receipts_rate,
            "avg_header_ms": avg_header_ms,
            "avg_receipts_ms": avg_receipts_ms,
        })
    }).collect();

    blocks.sort_by_key(|entry| entry["block"].as_u64().unwrap_or_default());

    let header_p50 = percentile_from(&stats.header_ms_all, 0.50);
    let header_p95 = percentile_from(&stats.header_ms_all, 0.95);
    let header_p99 = percentile_from(&stats.header_ms_all, 0.99);
    let receipts_p50 = percentile_from(&stats.receipts_ms_all, 0.50);
    let receipts_p95 = percentile_from(&stats.receipts_ms_all, 0.95);
    let receipts_p99 = percentile_from(&stats.receipts_ms_all, 0.99);

    let mut peer_rows: Vec<_> = stats
        .peer_stats
        .iter()
        .map(|(peer, stats)| {
            let avg_header_ms = if stats.header_ok > 0 {
                stats.header_ms_sum as f64 / stats.header_ok as f64
            } else {
                0.0
            };
            let avg_receipts_ms = if stats.receipts_ok > 0 {
                stats.receipts_ms_sum as f64 / stats.receipts_ok as f64
            } else {
                0.0
            };
            json!({
                "peer_id": peer,
                "blocks_total": stats.blocks_total,
                "blocks_ok": stats.blocks_ok,
                "avg_header_ms": avg_header_ms,
                "avg_receipts_ms": avg_receipts_ms,
            })
        })
        .collect();
    peer_rows.sort_by(|a, b| {
        b["blocks_ok"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&a["blocks_ok"].as_u64().unwrap_or(0))
    });
    if peer_rows.len() > 10 {
        peer_rows.truncate(10);
    }

    json!({
        "event": "summary",
        "duration_secs": elapsed_secs,
        "total_probes": stats.total_probes,
        "receipts_ok_total": stats.receipts_ok_total,
        "blocks_ok_total": stats.receipts_ok_total,
        "throughput_blocks_per_sec": throughput_blocks,
        "avg_window_blocks_per_sec": avg_window_blocks_per_sec,
        "max_window_blocks_per_sec": stats.window_blocks_per_sec_max,
        "total_targets": total_targets,
        "completed_blocks": completed_blocks,
        "coverage": coverage,
        "failed_blocks_total": stats.failed_blocks_total,
        "retries_total": stats.retries_total,
        "failure_reasons": stats.failure_reasons,
        "header_p50_ms": header_p50,
        "header_p95_ms": header_p95,
        "header_p99_ms": header_p99,
        "receipts_p50_ms": receipts_p50,
        "receipts_p95_ms": receipts_p95,
        "receipts_p99_ms": receipts_p99,
        "peer_leaderboard": peer_rows,
        "drain_time_secs": stats.drain_time_secs,
        "peers": stats.clients,
        "by_block": blocks,
    })
}

fn spawn_window_stats(context: Arc<RunContext>) {
    if context.config.stats_interval_secs == 0 {
        return;
    }
    let interval = Duration::from_secs(context.config.stats_interval_secs);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        let mut drain_streak = 0u8;
        loop {
            ticker.tick().await;
            let queue_len = context.queue_len().await;
            let escalation_len = context.escalation_len().await;
            let in_flight = context.in_flight.load(Ordering::SeqCst);
            let mut window = context.window.lock().await;
            let snapshot = window.take_snapshot();
            drop(window);
            let mut header_ms = snapshot.header_ms;
            let mut receipts_ms = snapshot.receipts_ms;
            let header_p50 = percentile(&mut header_ms, 0.50);
            let header_p95 = percentile(&mut header_ms, 0.95);
            let receipts_p50 = percentile(&mut receipts_ms, 0.50);
            let receipts_p95 = percentile(&mut receipts_ms, 0.95);
            let window_secs = interval.as_secs_f64().max(0.001);
            let probes_per_sec = snapshot.blocks_total as f64 / window_secs;
            let blocks_per_sec = snapshot.blocks_ok as f64 / window_secs;
            context.record_window(blocks_per_sec).await;
            let payload = json!({
                "event": "window_stats",
                "interval_secs": interval.as_secs(),
                "queue_len": queue_len,
                "active_peers_total": context.active_peers.lock().await.len(),
                "active_peers_window": snapshot.peers.len(),
                "probes_per_sec": probes_per_sec,
                "blocks_per_sec": blocks_per_sec,
                "header_p50_ms": header_p50,
                "header_p95_ms": header_p95,
                "receipts_p50_ms": receipts_p50,
                "receipts_p95_ms": receipts_p95,
                "at_ms": now_ms(),
            });
            context.logger.log_stats(payload).await;

            if queue_len == 0
                && escalation_len == 0
                && in_flight == 0
                && context.completed_blocks().await + context.failed_blocks_total().await as usize
                    >= context.total_targets
            {
                drain_streak = drain_streak.saturating_add(1);
                context
                    .record_drain(context.run_start.elapsed().as_secs_f64())
                    .await;
                if drain_streak >= 2 {
                    let _ = context.shutdown_tx.send(true);
                }
            } else {
                drain_streak = 0;
            }
        }
    });
}

fn spawn_progress_bar(
    context: Arc<RunContext>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    if context.config.quiet {
        return;
    }
    let total = context.total_targets as u64;
    let pb = ProgressBar::with_draw_target(
        Some(total),
        ProgressDrawTarget::stderr_with_hz(10),
    );
    let style = ProgressStyle::with_template(
        "{bar:40.cyan/blue} {percent:>3}% {pos}/{len} | {elapsed_precise} | {msg}",
    )
    .unwrap()
    .progress_chars("=>-");
    pb.set_style(style);
    pb.set_message("status looking_for_peers | peers 0/0 | queue 0 | inflight 0 | Failed 0 | speed 0.0/s | eta --");
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        let mut sent_shutdown = false;
        let mut main_finished = false;
        let mut escalation_total = 0u64;
        let mut escalation_pb: Option<ProgressBar> = None;
        let mut main_window: VecDeque<(Instant, u64)> = VecDeque::new();
        let mut escalation_window: VecDeque<(Instant, u64)> = VecDeque::new();
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let completed = context.completed_blocks().await;
                    let failed = context.failed_blocks_total().await as usize;
                    let processed = completed + failed;
                    let active = context.active_peers.lock().await.len();
                    let sessions = context.sessions.load(Ordering::SeqCst);
                    let queue_len = context.queue_len().await;
                    let in_flight = context.in_flight.load(Ordering::SeqCst);
                    let escalation_active = context.escalation_active.load(Ordering::SeqCst);
                    let escalation_len = context.escalation_len().await;
                    let failed_u64 = failed as u64;
                    if failed_u64 > escalation_total {
                        escalation_total = failed_u64;
                        if let Some(ref pb) = escalation_pb {
                            pb.set_length(escalation_total);
                        }
                    }
                    let status = if sessions == 0 {
                        "looking_for_peers"
                    } else if escalation_active && escalation_len > 0 {
                        "escalating"
                    } else if processed >= context.total_targets && queue_len == 0 && in_flight == 0 {
                        "finalizing"
                    } else {
                        "fetching"
                    };
                    if !main_finished {
                        let now = Instant::now();
                        main_window.push_back((now, processed as u64));
                        while let Some((t, _)) = main_window.front() {
                            if now.duration_since(*t) > Duration::from_secs(1) && main_window.len() > 1 {
                                main_window.pop_front();
                            } else {
                                break;
                            }
                        }
                        let speed = if let (Some((t0, v0)), Some((t1, v1))) =
                            (main_window.front(), main_window.back())
                        {
                            let dt = t1.duration_since(*t0).as_secs_f64();
                            if dt > 0.0 && v1 >= v0 {
                                (v1 - v0) as f64 / dt
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        };
                        let remaining = context.total_targets.saturating_sub(processed) as f64;
                        let eta = if speed > 0.0 {
                            format!("{:.0}s", remaining / speed)
                        } else {
                            "--".to_string()
                        };
                        let msg = format!(
                            "status {} | peers {}/{} | queue {} | inflight {} | Failed {} | speed {:.1}/s | eta {}",
                            status, active, sessions, queue_len, in_flight, failed, speed, eta
                        );
                        pb.set_message(msg);
                        pb.set_position(processed.min(context.total_targets) as u64);
                    }

                    if !main_finished && processed >= context.total_targets && queue_len == 0 {
                        main_finished = true;
                        pb.finish_and_clear();
                    }

                    if main_finished {
                        if escalation_total == 0 && failed_u64 > 0 {
                            escalation_total = failed_u64;
                        }
                        if escalation_total > 0 {
                            if escalation_pb.is_none() {
                                let esc_pb = ProgressBar::with_draw_target(
                                    Some(escalation_total),
                                    ProgressDrawTarget::stderr_with_hz(10),
                                );
                                let esc_style = ProgressStyle::with_template(
                                        "{bar:40.red/black} {percent:>3}% {pos}/{len} | {elapsed_precise} | {msg}",
                                )
                                .unwrap()
                                .progress_chars("=>-");
                                esc_pb.set_style(esc_style);
                                esc_pb.set_message("status escalating_failed | failed 0/0 | tried 0/0 | peers 0/0 | inflight 0 | queue 0");
                                escalation_pb = Some(esc_pb);
                            }
                            let remaining = failed_u64;
                            let done = escalation_total.saturating_sub(remaining);
                                let now = Instant::now();
                                escalation_window.push_back((now, done));
                                while let Some((t, _)) = escalation_window.front() {
                                    if now.duration_since(*t) > Duration::from_secs(1)
                                        && escalation_window.len() > 1
                                    {
                                        escalation_window.pop_front();
                                    } else {
                                        break;
                                    }
                                }
                                let esc_speed =
                                    if let (Some((t0, v0)), Some((t1, v1))) = (
                                        escalation_window.front(),
                                        escalation_window.back(),
                                    ) {
                                        let dt = t1.duration_since(*t0).as_secs_f64();
                                        if dt > 0.0 && v1 >= v0 {
                                            (v1 - v0) as f64 / dt
                                        } else {
                                            0.0
                                        }
                                    } else {
                                        0.0
                                    };
                                let esc_eta = if esc_speed > 0.0 {
                                    format!("{:.0}s", remaining as f64 / esc_speed)
                                } else {
                                    "--".to_string()
                                };
                            if let Some(ref pb) = escalation_pb {
                                let attempted = context.escalation_attempted_blocks().await;
                                let msg = format!(
                                        "failed {remaining}/{escalation_total} | tried {attempted}/{escalation_total} | peers {active}/{sessions} | inflight {in_flight} | queue {escalation_len} | speed {:.1}/s | eta {}",
                                        esc_speed,
                                        esc_eta
                                );
                                pb.set_message(msg);
                                pb.set_position(done);
                            }
                        }
                    }
                    if !sent_shutdown
                        && processed >= context.total_targets
                        && queue_len == 0
                        && escalation_len == 0
                        && in_flight == 0
                    {
                        let _ = context.shutdown_tx.send(true);
                        sent_shutdown = true;
                    }
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }
        if !main_finished {
            pb.finish_and_clear();
        }
        if let Some(pb) = escalation_pb {
            pb.finish_and_clear();
        }
    });
}

fn percentile(values: &mut Vec<u128>, percentile: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[idx]
}

fn percentile_from(values: &[u128], pct: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut copy = values.to_vec();
    percentile(&mut copy, pct)
}

async fn request_receipts_legacy(
    peer_id: PeerId,
    eth_version: EthVersion,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    block_numbers: &[u64],
    block_hashes: &[B256],
    context: &RunContext,
) -> Result<Vec<usize>, String> {
    let request_id = context.next_request_id();
    let sent_at = now_ms();
    context
        .log_request(json!({
            "event": "request_sent",
            "request_id": request_id,
            "peer_id": format!("{:?}", peer_id),
            "block_start": block_numbers.first().copied().unwrap_or_default(),
            "block_count": block_numbers.len(),
            "kind": "receipts",
            "variant": "legacy",
            "eth_version": format!("{eth_version}"),
            "sent_at_ms": sent_at,
        }))
        .await;

    let request = GetReceipts(block_hashes.to_vec());
    let (tx, rx) = oneshot::channel();
    messages
        .try_send(PeerRequest::GetReceipts { request, response: tx })
        .map_err(|err| format!("send error: {err:?}"))?;

    let response =
        match tokio::time::timeout(Duration::from_millis(context.config.request_timeout_ms), rx)
            .await
        {
            Ok(inner) => inner.map_err(|err| format!("response dropped: {err:?}"))?,
            Err(_) => {
                let received_at = now_ms();
                let duration_ms = received_at.saturating_sub(sent_at);
                context
                    .log_request(json!({
                        "event": "response_err",
                        "request_id": request_id,
                        "peer_id": format!("{:?}", peer_id),
                        "block_start": block_numbers.first().copied().unwrap_or_default(),
                        "block_count": block_numbers.len(),
                        "kind": "receipts",
                        "variant": "legacy",
                        "eth_version": format!("{eth_version}"),
                        "received_at_ms": received_at,
                        "duration_ms": duration_ms,
                        "error": "timeout",
                    }))
                    .await;
                return Err("timeout".to_string());
            }
        };

    let received_at = now_ms();
    let duration_ms = received_at.saturating_sub(sent_at);

    match response {
        Ok(receipts) => {
            let counts: Vec<usize> = receipts.0.iter().map(|r| r.len()).collect();
            context
                .log_request(json!({
                    "event": "response_ok",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": block_numbers.first().copied().unwrap_or_default(),
                    "block_count": block_numbers.len(),
                    "kind": "receipts",
                    "variant": "legacy",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "items": counts.len(),
                }))
                .await;
            Ok(counts)
        }
        Err(err) => {
            context
                .log_request(json!({
                    "event": "response_err",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": block_numbers.first().copied().unwrap_or_default(),
                    "block_count": block_numbers.len(),
                    "kind": "receipts",
                    "variant": "legacy",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "error": format!("{err:?}"),
                }))
                .await;
            Err(format!("{err:?}"))
        }
    }
}

async fn request_receipts69(
    peer_id: PeerId,
    eth_version: EthVersion,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    block_numbers: &[u64],
    block_hashes: &[B256],
    context: &RunContext,
) -> Result<Vec<usize>, String> {
    let request_id = context.next_request_id();
    let sent_at = now_ms();
    context
        .log_request(json!({
            "event": "request_sent",
            "request_id": request_id,
            "peer_id": format!("{:?}", peer_id),
            "block_start": block_numbers.first().copied().unwrap_or_default(),
            "block_count": block_numbers.len(),
            "kind": "receipts",
            "variant": "eth69",
            "eth_version": format!("{eth_version}"),
            "sent_at_ms": sent_at,
        }))
        .await;

    let request = GetReceipts(block_hashes.to_vec());
    let (tx, rx) = oneshot::channel();
    messages
        .try_send(PeerRequest::GetReceipts69 { request, response: tx })
        .map_err(|err| format!("send error: {err:?}"))?;

    let response =
        match tokio::time::timeout(Duration::from_millis(context.config.request_timeout_ms), rx)
            .await
        {
            Ok(inner) => inner.map_err(|err| format!("response dropped: {err:?}"))?,
            Err(_) => {
                let received_at = now_ms();
                let duration_ms = received_at.saturating_sub(sent_at);
                context
                    .log_request(json!({
                        "event": "response_err",
                        "request_id": request_id,
                        "peer_id": format!("{:?}", peer_id),
                        "block_start": block_numbers.first().copied().unwrap_or_default(),
                        "block_count": block_numbers.len(),
                        "kind": "receipts",
                        "variant": "eth69",
                        "eth_version": format!("{eth_version}"),
                        "received_at_ms": received_at,
                        "duration_ms": duration_ms,
                        "error": "timeout",
                    }))
                    .await;
                return Err("timeout".to_string());
            }
        };

    let received_at = now_ms();
    let duration_ms = received_at.saturating_sub(sent_at);

    match response {
        Ok(Receipts69(receipts)) => {
            let counts: Vec<usize> = receipts.iter().map(|r| r.len()).collect();
            context
                .log_request(json!({
                    "event": "response_ok",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": block_numbers.first().copied().unwrap_or_default(),
                    "block_count": block_numbers.len(),
                    "kind": "receipts",
                    "variant": "eth69",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "items": counts.len(),
                }))
                .await;
            Ok(counts)
        }
        Err(err) => {
            context
                .log_request(json!({
                    "event": "response_err",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": block_numbers.first().copied().unwrap_or_default(),
                    "block_count": block_numbers.len(),
                    "kind": "receipts",
                    "variant": "eth69",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "error": format!("{err:?}"),
                }))
                .await;
            Err(format!("{err:?}"))
        }
    }
}

async fn request_receipts70(
    peer_id: PeerId,
    eth_version: EthVersion,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    block_numbers: &[u64],
    block_hashes: &[B256],
    context: &RunContext,
) -> Result<Vec<usize>, String> {
    let request_id = context.next_request_id();
    let sent_at = now_ms();
    context
        .log_request(json!({
            "event": "request_sent",
            "request_id": request_id,
            "peer_id": format!("{:?}", peer_id),
            "block_start": block_numbers.first().copied().unwrap_or_default(),
            "block_count": block_numbers.len(),
            "kind": "receipts",
            "variant": "eth70",
            "eth_version": format!("{eth_version}"),
            "sent_at_ms": sent_at,
        }))
        .await;

    let request = GetReceipts70 {
        first_block_receipt_index: 0,
        block_hashes: block_hashes.to_vec(),
    };
    let (tx, rx) = oneshot::channel();
    messages
        .try_send(PeerRequest::GetReceipts70 { request, response: tx })
        .map_err(|err| format!("send error: {err:?}"))?;

    let response =
        match tokio::time::timeout(Duration::from_millis(context.config.request_timeout_ms), rx)
            .await
        {
            Ok(inner) => inner.map_err(|err| format!("response dropped: {err:?}"))?,
            Err(_) => {
                let received_at = now_ms();
                let duration_ms = received_at.saturating_sub(sent_at);
                context
                    .log_request(json!({
                        "event": "response_err",
                        "request_id": request_id,
                        "peer_id": format!("{:?}", peer_id),
                        "block_start": block_numbers.first().copied().unwrap_or_default(),
                        "block_count": block_numbers.len(),
                        "kind": "receipts",
                        "variant": "eth70",
                        "eth_version": format!("{eth_version}"),
                        "received_at_ms": received_at,
                        "duration_ms": duration_ms,
                        "error": "timeout",
                    }))
                    .await;
                return Err("timeout".to_string());
            }
        };

    let received_at = now_ms();
    let duration_ms = received_at.saturating_sub(sent_at);

    match response {
        Ok(Receipts70 { receipts, last_block_incomplete }) => {
            let counts: Vec<usize> = receipts.iter().map(|r| r.len()).collect();
            context
                .log_request(json!({
                    "event": "response_ok",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": block_numbers.first().copied().unwrap_or_default(),
                    "block_count": block_numbers.len(),
                    "kind": "receipts",
                    "variant": "eth70",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "items": counts.len(),
                    "last_block_incomplete": last_block_incomplete,
                }))
                .await;
            Ok(counts)
        }
        Err(err) => {
            context
                .log_request(json!({
                    "event": "response_err",
                    "request_id": request_id,
                    "peer_id": format!("{:?}", peer_id),
                    "block_start": block_numbers.first().copied().unwrap_or_default(),
                    "block_count": block_numbers.len(),
                    "kind": "receipts",
                    "variant": "eth70",
                    "eth_version": format!("{eth_version}"),
                    "received_at_ms": received_at,
                    "duration_ms": duration_ms,
                    "error": format!("{err:?}"),
                }))
                .await;
            Err(format!("{err:?}"))
        }
    }
}

