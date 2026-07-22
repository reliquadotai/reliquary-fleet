#!/usr/bin/env python3
"""Query Reliquary R2: list windows, fetch slot data, analyze your hotkeys.

Standalone helper — runnable on its own without launching the web
dashboard. Pulls the OUR_SS58 mapping from config.yaml (or the
environment variable RELIQUARY_OUR_SS58 as a comma-separated
hotkey=label list) so it can run on a fresh clone without editing.
"""

import gzip
import json
import os
import sys
from collections import Counter
from pathlib import Path

import boto3


def _load_our() -> dict[str, str]:
    """Resolve hotkey -> label from config.yaml or env, in that order."""
    # Try repo-local config.yaml first.
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    if os.path.exists(cfg_path):
        try:
            import yaml

            with Path(cfg_path).open(encoding="utf-8") as handle:
                data = yaml.safe_load(handle) or {}
            out: dict[str, str] = {}
            for b in data.get("fleet") or []:
                hk = (b.get("hotkey") or "").strip()
                if hk:
                    out[hk] = (b.get("label") or hk[:10]).strip()
            for hk in data.get("starred_hotkeys") or []:
                hk = str(hk).strip()
                if hk and hk not in out:
                    out[hk] = hk[:10]
            if out:
                return out
        except Exception:
            pass
    # Fall back to env var (handy for one-off `python3 r2_query.py` runs).
    raw = os.environ.get("RELIQUARY_OUR_SS58", "")
    if raw:
        out = {}
        for pair in raw.split(","):
            if "=" in pair:
                hk, lbl = pair.split("=", 1)
                out[hk.strip()] = lbl.strip()
        return out
    return {}


OUR = _load_our()


def c():
    return boto3.client(
        "s3", endpoint_url=os.environ["R2_ENDPOINT"], region_name="auto"
    )


def list_windows(cli, n=20):
    r = cli.list_objects_v2(
        Bucket=os.environ["R2_BUCKET"], Prefix="reliquary/dataset/window-", MaxKeys=1000
    )
    keys = sorted(
        [o["Key"] for o in r.get("Contents", [])],
        key=lambda k: int(k.split("window-")[1].split(".")[0]) if "window-" in k else 0,
        reverse=True,
    )
    return keys[:n]


def fetch(cli, key):
    raw = cli.get_object(Bucket=os.environ["R2_BUCKET"], Key=key)["Body"].read()
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    cli = c()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "summary"

    if cmd == "list":
        for k in list_windows(cli, 30):
            print(k)
        return

    if cmd == "window":
        wn = sys.argv[2]
        keys = [k for k in list_windows(cli, 100) if f"window-{wn}." in k]
        for k in keys[:1]:
            d = fetch(cli, k)
            print(f"--- {k} ---")
            print("validator:", d.get("validator_hotkey", "")[:12])
            print("randomness:", d.get("randomness", "")[:16])
            batch = d.get("batch", [])
            print(f"BATCH ({len(batch)}/8):")
            for i, b in enumerate(batch):
                hk = b.get("hotkey", "")
                tag = OUR.get(hk, "")
                prompt = b.get("prompt_idx")
                sigma = b.get("sigma", 0)
                print(f"  [{i}] hk={hk[:12]} {tag} prompt={prompt} sigma={sigma:.3f}")
            ru = d.get("runners_up", [])
            print(f"RUNNERS UP ({len(ru)}):")
            for r in ru[:5]:
                hk = r.get("hotkey", "")
                tag = OUR.get(hk, "")
                prompt = r.get("prompt_idx")
                sigma = r.get("sigma", 0)
                print(f"  hk={hk[:12]} {tag} prompt={prompt} sigma={sigma:.3f}")
            rs = d.get("reject_summary", {})
            print(f"REJECT SUMMARY: {dict(rs)}")
            rej = d.get("rejected", [])
            print(f"REJECTED ({len(rej)} entries) — first 10:")
            for r in rej[:10]:
                hk = r.get("hotkey", "")
                tag = OUR.get(hk, "")
                reason = r.get("reason")
                prompt = r.get("prompt_idx")
                print(f"  hk={hk[:12]} {tag} reason={reason} prompt={prompt}")
        return

    # summary mode
    n = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 12
    keys = list_windows(cli, n)
    print(f"=== last {len(keys)} windows ===")
    our_batch = Counter()
    our_runners = Counter()
    our_rejects = Counter()  # (name, reason)
    other_batch = 0
    for k in keys:
        try:
            d = fetch(cli, k)
            wn = k.split("window-")[1].split(".")[0]
            batch = d.get("batch", [])
            ru = d.get("runners_up", [])
            rej = d.get("rejected", [])
            our_in_batch = []
            for b in batch:
                hk = b.get("hotkey", "")
                if hk in OUR:
                    our_batch[OUR[hk]] += 1
                    our_in_batch.append(OUR[hk])
                else:
                    other_batch += 1
            for r in ru:
                hk = r.get("hotkey", "")
                if hk in OUR:
                    our_runners[OUR[hk]] += 1
            for r in rej:
                hk = r.get("hotkey", "")
                if hk in OUR:
                    our_rejects[(OUR[hk], r.get("reason", "?"))] += 1
            runner_count = sum(r.get("hotkey") in OUR for r in ru)
            rejected_count = sum(r.get("hotkey") in OUR for r in rej)
            print(
                f"window-{wn}: batch=[{','.join(our_in_batch) or '-'}] "
                f"ours={len(our_in_batch)}/{len(batch)} "
                f"runners_up={runner_count} rejected={rejected_count}"
            )
        except Exception as e:
            print(f"window-{k}: ERR {e}")
    print(f"\n=== TOTALS across {n} windows ===")
    batch_total = sum(our_batch.values())
    runner_total = sum(our_runners.values())
    reject_total = sum(our_rejects.values())
    print(f"OUR slots in batch (= scoring): {dict(our_batch)} TOTAL={batch_total}")
    print(
        "OUR runners-up (validated but lost FIFO): "
        f"{dict(our_runners)} TOTAL={runner_total}"
    )
    print("OUR rejects by (name,reason):")
    for (name, reason), cnt in sorted(our_rejects.items(), key=lambda x: -x[1]):
        print(f"  {name:25} {reason:25} {cnt}")
    print(f"OTHER miners' batch slots: {other_batch}")
    total_our_attempts = batch_total + runner_total + reject_total
    if total_our_attempts:
        batch_rate = batch_total / total_our_attempts * 100
        reject_rate = reject_total / total_our_attempts * 100
        print(f"\nOur batch-win rate: {batch_rate:.1f}%")
        print(f"Our reject rate:    {reject_rate:.1f}%")


if __name__ == "__main__":
    main()
