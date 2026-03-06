#!/usr/bin/env python3
"""Compare lamlock variants vs std-mutex Criterion benchmark results."""

import json
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

CRITERION_DIR = Path(__file__).resolve().parent.parent.parent / "target" / "criterion"
WORKLOADS = ["stack", "pqueue", "ringbuf", "slab", "hashtable", "btree"]
THREADS = [2, 4, 8, 16, 32, 64, 128, 256, 512]
VARIANTS = ["lamlock", "lamlock-no-panic", "lamlock-spin", "lamlock-spin-no-panic", "std-mutex"]

# Short display names to keep the table narrow.
SHORT = {
    "lamlock": "ResiLock",
    "lamlock-no-panic": "ResiLock-np",
    "lamlock-spin": "ResiLock-spin",
    "lamlock-spin-no-panic": "ResiLock-spin-np",
    "std-mutex": "std-mutex",
}


def read_estimate(workload, impl_name, threads):
    path = CRITERION_DIR / workload / impl_name / str(threads) / "new" / "estimates.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)["mean"]["point_estimate"]  # nanoseconds


def fmt_time(ns):
    if ns >= 1e9:
        return f"{ns / 1e9:.2f}s"
    if ns >= 1e6:
        return f"{ns / 1e6:.2f}ms"
    if ns >= 1e3:
        return f"{ns / 1e3:.2f}µs"
    return f"{ns:.0f}ns"


def print_table(title, rows, present):
    """Print one table: absolute times, then a separate delta table."""
    W = 10  # column width for times
    D = 8   # column width for deltas

    # ── Absolute times ──
    hdr = f"{'work':<8} {'thr':>3}"
    for v in present:
        hdr += f" {SHORT[v]:>{W}}"
    sep = "─" * len(hdr)

    print()
    print(f"  {title}")
    print(f"  {sep}")
    print(f"  {hdr}")
    print(f"  {sep}")
    for w, t, est in rows:
        line = f"{w:<8} {t:>3}"
        for v in present:
            if v in est:
                line += f" {fmt_time(est[v]):>{W}}"
            else:
                line += f" {'—':>{W}}"
        print(f"  {line}")
    print(f"  {sep}")

    # ── Delta % vs std-mutex ──
    others = [v for v in present if v != "std-mutex"]
    if "std-mutex" not in present or not others:
        return

    dhdr = f"{'work':<8} {'thr':>3}"
    for v in others:
        dhdr += f" {SHORT[v]:>{D}}"
    dsep = "─" * len(dhdr)

    print()
    print("  Δ% vs std-mutex (negative = faster)")
    print(f"  {dsep}")
    print(f"  {dhdr}")
    print(f"  {dsep}")
    for w, t, est in rows:
        if "std-mutex" not in est:
            continue
        std = est["std-mutex"]
        line = f"{w:<8} {t:>3}"
        for v in others:
            if v in est:
                delta = (est[v] - std) / std * 100
                line += f" {delta:>+{D-1}.1f}%"
            else:
                line += f" {'—':>{D}}"
        print(f"  {line}")
    print(f"  {dsep}")


def main():
    rows = []
    for w in WORKLOADS:
        for t in THREADS:
            estimates = {}
            for v in VARIANTS:
                est = read_estimate(w, v, t)
                if est is not None:
                    estimates[v] = est
            if not estimates:
                continue
            rows.append((w, t, estimates))

    if not rows:
        print("No benchmark results found in", CRITERION_DIR)
        return

    # Determine which variants actually have data
    present = [v for v in VARIANTS if any(v in est for _, _, est in rows)]

    print_table("Benchmark Results", rows, present)
    plot_graphs(rows, present)


def plot_graphs(rows, present):
    plt.rcParams.update({
        "font.size": 14,
        "axes.titlesize": 18,
        "axes.labelsize": 16,
        "xtick.labelsize": 13,
        "ytick.labelsize": 13,
        "legend.fontsize": 13,
    })

    out_dir = Path(__file__).resolve().parent / "plots"
    out_dir.mkdir(exist_ok=True)

    others = [v for v in present if v != "std-mutex"]

    # Group rows by workload
    by_workload = {}
    for w, t, est in rows:
        by_workload.setdefault(w, []).append((t, est))

    for w, entries in by_workload.items():
        entries.sort(key=lambda x: x[0])

        # Only plot points where std-mutex baseline exists
        baseline = {t: est["std-mutex"] for t, est in entries if "std-mutex" in est}
        if not baseline:
            continue

        fig, ax = plt.subplots(figsize=(8, 5))

        # Draw std-mutex as a flat 1.0 reference line
        ax.axhline(1.0, color="black", linestyle="--", linewidth=1.5, label=SHORT["std-mutex"])

        for v in others:
            threads = [t for t, est in entries if v in est and t in baseline]
            ratios = [est[v] / baseline[t] for t, est in entries if v in est and t in baseline]
            if threads:
                ax.plot(threads, ratios, marker="o", markersize=7, linewidth=2.5, label=SHORT[v])

        ax.set_xscale("log", base=2)
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax.xaxis.set_major_locator(ticker.FixedLocator(THREADS))
        ax.set_xlabel("Threads")
        ax.set_ylabel("Time (normalized to std-mutex)")
        ax.set_title(f"{w}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()

        dest = out_dir / f"{w}.png"
        fig.savefig(dest, dpi=300)
        plt.close(fig)
        print(f"  saved {dest}")


if __name__ == "__main__":
    main()
