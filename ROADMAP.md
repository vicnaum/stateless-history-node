# Roadmap

## Done
- [x] Scaffold harness using Reth as a library (no execution/state).
- [x] P2P discovery, dialing, and `eth` handshake.
- [x] Header + receipts probing with eth/69 and eth/70 handling.
- [x] Batch requests and shared work queue scheduling.
- [x] DeFi anchor block targeting with adjustable window.
- [x] Known-blocks cache to avoid re-fetching.
- [x] Soft ban for peers with repeated failures.
- [x] Request/probe/stats JSONL output.
- [x] Windowed stats and end-of-run summary.
- [x] Real-time progress bar with status and ETA.
- [x] Auto-exit when all work is complete.
- [x] Two-tier retry strategy (normal retries + escalation pass).

## Next
- [ ] Add receipts root validation (optional flag).
- [ ] OP Stack L2 support (Base, Optimism) using op-geth/op-reth peers.
- [ ] Configurable anchors from file/CLI.
- [ ] Minimal tx extraction (from/to/value) alongside receipts.
- [ ] Add a Portal/era1 fallback for pruned history (optional).
- [ ] Export metrics (Prometheus/OTel) for long-running runs.
- [ ] Peer scoring / selection strategies for better coverage.
- [ ] Tests for retry + escalation logic.
