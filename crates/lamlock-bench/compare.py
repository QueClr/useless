#!/usr/bin/env python3
"""Compare lamlock vs std-mutex Criterion benchmark results."""

import json
from pathlib import Path

CRITERION_DIR = Path(__file__).resolve().parent.parent.parent / "target" / "criterion"
WORKLOADS = ["stack", "pqueue", "ringbuf", "slab"]
THREADS = [64, 128, 256, 512]


def read_estimate(workload, impl_name, threads):
    path = CRITERION_DIR / workload / impl_name / str(threads) / "new" / "estimates.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)["mean"]["point_estimate"]  # nanoseconds


def fmt_time(ns):
    if ns >= 1e9:
        return f"{ns / 1e9:.3f}  s"
    if ns >= 1e6:
        return f"{ns / 1e6:.3f} ms"
    if ns >= 1e3:
        return f"{ns / 1e3:.3f} µs"
    return f"{ns:.3f} ns"


def main():
    rows = []
    for w in WORKLOADS:
        for t in THREADS:
            lam = read_estimate(w, "lamlock", t)
            std = read_estimate(w, "std-mutex", t)
            if lam is None or std is None:
                continue
            delta = (lam - std) / std * 100
            rows.append((w, t, lam, std, delta))

    if not rows:
        print("No benchmark results found in", CRITERION_DIR)
        return

    hdr = f"{'Workload':<14} {'Threads':>7} {'lamlock':>12} {'std-mutex':>12} {'Δ%':>8}"
    sep = "-" * len(hdr)
    print(sep)
    print(hdr)
    print(sep)
    for w, t, lam, std, delta in rows:
        print(f"{w:<14} {t:>7} {fmt_time(lam):>12} {fmt_time(std):>12} {delta:>+7.1f}%")
    print(sep)


if __name__ == "__main__":
    main()
