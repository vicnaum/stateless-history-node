# Stateless History Receipt Availability Harness

This harness measures how often Ethereum mainnet peers can serve block headers
and transaction receipts over the devp2p `eth` protocol. It uses Reth as a
library, does not execute transactions, and stores only the minimum history
needed for receipts availability analysis.

## What it does
- Discovers and connects to Ethereum mainnet peers via devp2p.
- Fetches canonical headers and receipts for target blocks.
- Records per-request, per-block probe results, and windowed stats.
- Runs with a shared work queue and peer soft-bans.
- Retries failed blocks, then escalates by trying them on many peers.

## Quick start
```bash
cd /Users/vicnaum/github/stateless-history-node/harness
/Users/vicnaum/.cargo/bin/cargo run -- --anchor-window 100 -v
```

### Examples
- 1000 blocks per anchor, progress bar + window stats:
```bash
/Users/vicnaum/.cargo/bin/cargo run -- --anchor-window 1000 -v
```

- Time-bounded run (5 minutes):
```bash
/Users/vicnaum/.cargo/bin/cargo run -- --anchor-window 1000 -v --run-secs 300
```

## Output files
All output is written under `harness/output/`:
- `requests.jsonl`: request/response timing + peer events.
- `probes.jsonl`: per-block probe outcome (header/receipts, latencies).
- `known_blocks.jsonl`: blocks confirmed as available.
- `stats.jsonl`: windowed stats + final summary.

## CLI flags
Core:
- `--anchor-window <u64>`: blocks per anchor (default: 10000).
- `--run-secs <u64>`: stop after N seconds.
- `--max-attempts <u32>`: retries before a block is marked failed (default: 3).
- `--quiet`: suppress console output (still writes JSONL files).
- `-v`/`-vv`/`-vvv`: verbosity levels (progress bar, stats, per-request logs).

Network:
- `--max-outbound <usize>`
- `--max-concurrent-dials <usize>`
- `--refill-interval-ms <u64>`
- `--max-inbound <usize>`

Batching / performance:
- `--blocks-per-assignment <usize>`
- `--receipts-per-request <usize>`
- `--request-timeout-ms <u64>`

Logging and stats:
- `--log-flush-ms <u64>`
- `--log-flush-lines <usize>`
- `--stats-interval-secs <u64>`
- `--peer-failure-threshold <u32>`
- `--peer-ban-secs <u64>`

## Retry model
- **Normal retries:** failed blocks requeue up to `--max-attempts`.
- **Escalation pass:** once the main queue is drained, failed blocks are tried
  again on as many distinct peers as possible (one attempt per peer).

## Notes
- This harness targets Ethereum mainnet only (for now).
- Receipt root validation is intentionally skipped in v1.
- Re-running reuses `known_blocks.jsonl` to avoid duplicate work.
