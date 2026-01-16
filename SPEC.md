# Receipt Availability Harness Spec (Updated)

## Goal
Measure the availability and latency of Ethereum L1 block headers and receipts
served over devp2p `eth` without executing transactions or maintaining state.
Persist only what is needed for receipts availability analysis and indexing.

## Non-goals
- Full execution / state verification.
- Archive-style state or EVM traces.
- Receipts root validation (explicitly skipped in v1).

## High-level architecture
1. **P2P network layer (Reth as library)**
   - Discovery, dialing, and `eth` handshake.
   - `PeerRequestSender` for headers/receipts.
2. **Scheduler**
   - Shared work queue of target blocks.
   - Per-peer assignment and batching.
   - Soft ban on peers with repeated failures.
3. **Probing pipeline**
   - Header batch → receipt batch per block hash.
   - Eth protocol version-aware receipts requests (`eth/69` vs `eth/70`).
4. **Persistence**
   - JSONL logs for requests, probes, stats, and known blocks.

## Target selection
Targets are constructed from a fixed set of DeFi deployment anchors plus a
contiguous window:
- Uniswap V2: 10000835
- Aave V2: 11362579
- Uniswap V3: 12369621
- Aave V3: 16291127
- Uniswap V4: 21688329

The default window is 10,000 blocks per anchor. Set with `--anchor-window`.

## Data stored
Minimal data to enable downstream analysis:
- **Headers:** only for probing (not persisted as canonical chain state).
- **Receipts:** only counts and timing in probe logs.
- **JSONL output:**
  - `requests.jsonl`: request events + timing.
  - `probes.jsonl`: per-block probe results.
  - `known_blocks.jsonl`: blocks proven available.
  - `stats.jsonl`: window stats + summary.

## Retry and error handling
Two-tier retry strategy:
1. **Normal retries**
   - Failed blocks are requeued up to `--max-attempts`.
2. **Escalation pass**
   - Once the main queue is drained, blocks that failed all retries are tried
     again across distinct peers (one attempt per peer).
   - If every known peer fails, the block is marked unavailable.

All failures are recorded with reason tags (`header_timeout`,
`receipts_batch`, `escalation_*`, etc).

## Peer handling
- Track peer health and ban temporarily after consecutive failures.
- Ban duration and failure threshold are configurable.
- Sessions are counted; peer assignment is based on availability and queue.

## Completion criteria
The harness exits when:
1. All targets are completed or failed, and
2. Both normal and escalation queues are empty, and
3. No in-flight work remains.

`--run-secs` provides a hard time limit override.

## CLI behavior
Console output is a real-time progress bar (10Hz) showing:
- % complete and processed/total
- elapsed time, ETA
- speed (blocks/sec)
- peer status

Verbose levels:
- `-v`: progress + periodic stats
- `-vv`: adds per-peer summaries
- `-vvv`: includes per-request and per-probe JSON logs

## Known limitations
- No receipts root validation.
- Network-only data availability is impacted by EIP-4444 pruning.
- No L2 support yet (Base/Optimism planned).
