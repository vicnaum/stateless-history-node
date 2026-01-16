#!/usr/bin/env python3
import argparse
import json
import time
from collections import defaultdict
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize receipt-harness stats.jsonl + probes.jsonl."
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Harness output directory (default: output).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Top N peers to display (default: 10).",
    )
    parser.add_argument(
        "--since-ms",
        type=int,
        default=None,
        help="Only include window stats after this unix ms timestamp.",
    )
    parser.add_argument(
        "--since-mins",
        type=float,
        default=None,
        help="Only include window stats from the last N minutes.",
    )
    return parser.parse_args()


def load_jsonl(path: Path):
    if not path.exists():
        return
    with path.open() as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def percentile(values, pct):
    if not values:
        return 0
    values = sorted(values)
    idx = int(round((len(values) - 1) * pct))
    return values[idx]


def summarize_windows(stats_path: Path, since_ms: int | None):
    windows = []
    for entry in load_jsonl(stats_path):
        if entry.get("event") != "window_stats":
            continue
        at_ms = entry.get("at_ms")
        if since_ms is not None and isinstance(at_ms, (int, float)) and at_ms < since_ms:
            continue
        windows.append(entry)

    if not windows:
        return {}

    blocks_rates = [w.get("blocks_per_sec", w.get("receipts_per_sec", 0.0)) for w in windows]
    probes_rates = [w.get("probes_per_sec", 0.0) for w in windows]
    header_p50 = [w.get("header_p50_ms", 0) for w in windows]
    header_p95 = [w.get("header_p95_ms", 0) for w in windows]
    receipts_p50 = [w.get("receipts_p50_ms", 0) for w in windows]
    receipts_p95 = [w.get("receipts_p95_ms", 0) for w in windows]
    active_peers = [w.get("active_peers_window", 0) for w in windows]

    latest = windows[-1]
    return {
        "windows": len(windows),
        "avg_blocks_per_sec": sum(blocks_rates) / len(blocks_rates),
        "max_blocks_per_sec": max(blocks_rates),
        "avg_probes_per_sec": sum(probes_rates) / len(probes_rates),
        "avg_header_p50_ms": sum(header_p50) / len(header_p50),
        "avg_header_p95_ms": sum(header_p95) / len(header_p95),
        "avg_receipts_p50_ms": sum(receipts_p50) / len(receipts_p50),
        "avg_receipts_p95_ms": sum(receipts_p95) / len(receipts_p95),
        "avg_active_peers_window": sum(active_peers) / len(active_peers),
        "latest": latest,
    }


def summarize_probes(probes_path: Path):
    per_peer = defaultdict(
        lambda: {
            "probes": 0,
            "header_ok": 0,
            "receipts_ok": 0,
            "header_ms_sum": 0,
            "receipts_ms_sum": 0,
        }
    )
    total_probes = 0
    total_receipts_ok = 0

    for entry in load_jsonl(probes_path):
        if entry.get("event") != "probe":
            continue
        peer = entry.get("peer_id")
        if not peer:
            continue
        stats = per_peer[peer]
        total_probes += 1
        stats["probes"] += 1
        if entry.get("header_ok"):
            stats["header_ok"] += 1
            stats["header_ms_sum"] += int(entry.get("header_ms") or 0)
        if entry.get("receipts_ok"):
            stats["receipts_ok"] += 1
            total_receipts_ok += 1
            stats["receipts_ms_sum"] += int(entry.get("receipts_ms") or 0)

    return total_probes, total_receipts_ok, per_peer


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    if args.since_ms is None and args.since_mins is not None:
        args.since_ms = int(time.time() * 1000 - args.since_mins * 60 * 1000)

    windows = summarize_windows(output_dir / "stats.jsonl", args.since_ms)
    total_probes, total_receipts_ok, per_peer = summarize_probes(
        output_dir / "probes.jsonl"
    )

    print("=== Window stats ===")
    if windows:
        print(f"windows: {windows['windows']}")
        print(f"avg_blocks_per_sec: {windows['avg_blocks_per_sec']:.2f}")
        print(f"max_blocks_per_sec: {windows['max_blocks_per_sec']:.2f}")
        print(f"avg_probes_per_sec: {windows['avg_probes_per_sec']:.2f}")
        print(
            "avg_header_p50_ms / p95_ms:",
            f"{windows['avg_header_p50_ms']:.1f} / {windows['avg_header_p95_ms']:.1f}",
        )
        print(
            "avg_receipts_p50_ms / p95_ms:",
            f"{windows['avg_receipts_p50_ms']:.1f} / {windows['avg_receipts_p95_ms']:.1f}",
        )
        print(f"avg_active_peers_window: {windows['avg_active_peers_window']:.2f}")
        latest = windows["latest"]
        print(
            "latest_window:",
            f"blocks_per_sec={latest.get('blocks_per_sec', latest.get('receipts_per_sec', 0)):.2f}",
            f"probes_per_sec={latest.get('probes_per_sec', 0):.2f}",
            f"active_peers_window={latest.get('active_peers_window', 0)}",
            f"queue_len={latest.get('queue_len', 0)}",
        )
    else:
        print("no window_stats found")

    print("\n=== Probe totals ===")
    print(f"total_probes: {total_probes}")
    print(f"blocks_ok_total: {total_receipts_ok}")
    print(f"unique_peers: {len(per_peer)}")

    rows = []
    for peer, stats in per_peer.items():
        receipts_ok = stats["receipts_ok"]
        probes = stats["probes"]
        header_ok = stats["header_ok"]
        avg_header_ms = stats["header_ms_sum"] / header_ok if header_ok else 0
        avg_receipts_ms = stats["receipts_ms_sum"] / receipts_ok if receipts_ok else 0
        rows.append(
            (
                receipts_ok,
                peer,
                probes,
                header_ok,
                avg_header_ms,
                avg_receipts_ms,
            )
        )
    rows.sort(reverse=True)

    print("\n=== Top peers ===")
    print(
        "peer_id\tprobes\theader_ok\tblocks_ok\tavg_header_ms\tavg_receipts_ms"
    )
    for receipts_ok, peer, probes, header_ok, avg_header_ms, avg_receipts_ms in rows[
        : args.top
    ]:
        print(
            f"{peer}\t{probes}\t{header_ok}\t{receipts_ok}"
            f"\t{avg_header_ms:.1f}\t{avg_receipts_ms:.1f}"
        )


if __name__ == "__main__":
    main()
