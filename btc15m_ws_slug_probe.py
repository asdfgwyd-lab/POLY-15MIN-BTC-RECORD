#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Probe Polymarket BTC 15m slugs via Gamma + WS, and print/write samples.
Usage:
  python btc15m_ws_slug_probe.py
  python btc15m_ws_slug_probe.py --slugs btc-updown-15m-1770798600,btc-updown-15m-1770799500
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
import websockets

GAMMA_BASE = "https://gamma-api.polymarket.com"
WSS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DEFAULT_SLUGS: List[str] = []

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "btc15m-ws-slug-probe/1.0"})


def utc_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_to_ts(iso_str: Optional[str]) -> Optional[int]:
    if not iso_str:
        return None
    s = iso_str.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None


def floor_to_grid(ts: int, step: int) -> int:
    return (ts // step) * step


def parse_list_field(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v) for v in x]
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [str(vv) for vv in v]
        except Exception:
            pass
        return [p.strip().strip('"').strip("'") for p in s.strip("[]").split(",") if p.strip()]
    return []


def safe_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def best_price(levels: List[dict], side: str) -> Optional[float]:
    prices = [safe_float(l.get("price")) for l in levels if l.get("price") is not None]
    prices = [p for p in prices if p is not None]
    if not prices:
        return None
    return max(prices) if side == "bid" else min(prices)


def gamma_get_market_by_slug(slug: str, timeout_read: int = 20) -> Optional[dict]:
    try:
        r = SESSION.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=(10, timeout_read))
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else None
    except Exception as e:
        print(f"[gamma] slug fetch failed slug={slug}: {repr(e)}", flush=True)
        return None


def extract_yes_no_tokens(m: dict) -> Optional[Tuple[str, str]]:
    outcomes = parse_list_field(m.get("outcomes"))
    token_ids = parse_list_field(m.get("clobTokenIds"))
    if len(outcomes) < 2 or len(token_ids) < 2:
        return None

    if "Yes" in outcomes and "No" in outcomes:
        yi = outcomes.index("Yes")
        ni = outcomes.index("No")
        if yi < len(token_ids) and ni < len(token_ids):
            return token_ids[yi], token_ids[ni]

    return token_ids[0], token_ids[1]


def build_slug_window(back_hours: int, forward_hours: int) -> List[str]:
    step = 15 * 60
    now_ts = int(time.time())
    start_ts = floor_to_grid(now_ts - back_hours * 3600, step)
    end_ts = floor_to_grid(now_ts + forward_hours * 3600, step)
    slugs: List[str] = []
    for ts in range(start_ts, end_ts + 1, step):
        slugs.append(f"btc-updown-15m-{ts}")
    return slugs


class Probe:
    def __init__(
        self,
        slugs: List[str],
        outdir: Optional[str],
        sample_sec: int,
        refresh_sec: int,
        back_hours: int,
        forward_hours: int,
    ):
        self.slugs = slugs
        self.outdir = outdir
        self.sample_sec = sample_sec
        self.refresh_sec = refresh_sec
        self.back_hours = back_hours
        self.forward_hours = forward_hours
        self.token_meta: Dict[str, Tuple[str, str]] = {}
        self.book: Dict[str, Tuple[Optional[float], Optional[float], Optional[int]]] = {}

    def _csv_path_for_ts(self, ts: int) -> Optional[str]:
        if not self.outdir:
            return None
        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        return os.path.join(self.outdir, f"btc15m_probe_{day}.csv")

    def build_tokens(self) -> List[str]:
        tokens: List[str] = []
        now_ts = int(time.time())
        kept = 0
        skipped = 0
        self.token_meta = {}
        for slug in self.slugs:
            m = gamma_get_market_by_slug(slug)
            if not m:
                continue
            end_iso = m.get("endDate") or m.get("endDateIso")
            start_iso = m.get("startDate") or m.get("startDateIso")
            end_ts = iso_to_ts(end_iso)
            start_ts = iso_to_ts(start_iso)
            if end_ts is None and start_ts is not None:
                end_ts = start_ts + 15 * 60

            # Only keep markets in the last 15 minutes window.
            if end_ts is None or not (end_ts - 15 * 60 <= now_ts <= end_ts):
                skipped += 1
                continue

            print(f"[gamma] slug={slug} startDate={start_iso} endDate={end_iso}", flush=True)
            toks = extract_yes_no_tokens(m)
            if not toks:
                print(f"[gamma] missing tokens slug={slug}", flush=True)
                continue
            self.token_meta[toks[0]] = (slug, "YES")
            self.token_meta[toks[1]] = (slug, "NO")
            tokens.extend([toks[0], toks[1]])
            kept += 1
        # dedupe
        seen = set()
        out = []
        for t in tokens:
            if t not in seen:
                seen.add(t)
                out.append(t)
        print(f"[probe] slugs={len(self.slugs)} kept={kept} skipped={skipped} tokens={len(out)}", flush=True)
        return out

    def handle_ws_message(self, data: dict) -> None:
        et = data.get("event_type")
        if et == "book":
            token_id = str(data.get("asset_id", ""))
            if token_id not in self.token_meta:
                return
            ts_ms = data.get("timestamp")
            last_u = None
            try:
                if ts_ms is not None:
                    last_u = int(int(ts_ms) / 1000)
            except Exception:
                last_u = None
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            bid = best_price(bids, "bid")
            ask = best_price(asks, "ask")
            self.book[token_id] = (bid, ask, last_u)
        elif et == "price_change":
            ts_ms = data.get("timestamp")
            last_u = None
            try:
                if ts_ms is not None:
                    last_u = int(int(ts_ms) / 1000)
            except Exception:
                last_u = None
            pcs = data.get("price_changes") or []
            for pc in pcs:
                token_id = str(pc.get("asset_id", ""))
                if token_id not in self.token_meta:
                    continue
                bid = safe_float(pc.get("best_bid"))
                ask = safe_float(pc.get("best_ask"))
                old = self.book.get(token_id, (None, None, None))
                self.book[token_id] = (bid if bid is not None else old[0], ask if ask is not None else old[1], last_u)

    def write_rows(self, ts: int) -> int:
        path = self._csv_path_for_ts(ts)
        if not path:
            return 0
        need_header = not os.path.exists(path) or os.path.getsize(path) == 0
        rows = []
        for token_id, (slug, side) in self.token_meta.items():
            bid, ask, last_u = self.book.get(token_id, (None, None, None))
            mid = None
            spread = None
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
                spread = ask - bid
            rows.append(
                [
                    utc_iso(ts),
                    slug,
                    side,
                    token_id,
                    f"{bid:.6f}" if bid is not None else "",
                    f"{ask:.6f}" if ask is not None else "",
                    f"{mid:.6f}" if mid is not None else "",
                    f"{spread:.6f}" if spread is not None else "",
                    utc_iso(last_u) if last_u is not None else "",
                ]
            )
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if need_header:
                w.writerow(["sample_ts_utc", "slug", "side", "token_id", "bid", "ask", "mid", "spread", "last_book_update_ts"])
            w.writerows(rows)
        return len(rows)

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(WSS_MARKET, ping_interval=20, ping_timeout=20) as ws:
                    backoff = 1.0
                    token_ids = self.build_tokens()
                    if not token_ids:
                        print("[probe] no tokens to subscribe", flush=True)
                    else:
                        await ws.send(json.dumps({"type": "market", "assets_ids": token_ids}))
                        print(f"[ws] subscribed tokens={len(token_ids)}", flush=True)

                    last_sample = 0
                    last_refresh = int(time.time())
                    last_bad_json_ts = 0
                    bad_json_count = 0

                    while True:
                        now = int(time.time())

                        # refresh slugs/tokens every N seconds
                        if self.refresh_sec > 0 and (now - last_refresh) >= self.refresh_sec:
                            # rebuild slug window each refresh
                            self.slugs = build_slug_window(back_hours=self.back_hours, forward_hours=self.forward_hours)
                            token_ids = self.build_tokens()
                            self.book = {}
                            if token_ids:
                                await ws.send(json.dumps({"type": "market", "assets_ids": token_ids}))
                                print(f"[ws] resubscribed tokens={len(token_ids)}", flush=True)
                            else:
                                print("[probe] no tokens to subscribe", flush=True)
                            last_refresh = now

                        if now - last_sample >= self.sample_sec:
                            wrote = self.write_rows(now)
                            print(f"[sample] {utc_iso(now)} wrote_rows={wrote}", flush=True)
                            last_sample = now
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                bad_json_count += 1
                                if now - last_bad_json_ts >= 30:
                                    print("[ws] bad json message ignored", flush=True)
                                    last_bad_json_ts = now
                                if bad_json_count >= 5:
                                    print("[ws] too many bad json messages -> reconnect", flush=True)
                                    await ws.close()
                                    break
                                continue
                            if isinstance(data, dict):
                                self.handle_ws_message(data)
                        except asyncio.TimeoutError:
                            pass
            except Exception as e:
                print(f"[ws] disconnected/error: {repr(e)}; retry in {backoff:.1f}s", flush=True)
                await asyncio.sleep(backoff + 0.2)
                backoff = min(backoff * 2.0, 30.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", default="", help="comma-separated slugs (optional)")
    ap.add_argument("--back-hours", type=int, default=1, help="hours to look back when auto-generating slugs")
    ap.add_argument("--forward-hours", type=int, default=2, help="hours to look forward when auto-generating slugs")
    ap.add_argument("--outdir", default="data", help="output directory (daily CSV)")
    ap.add_argument("--sample-sec", type=int, default=5)
    ap.add_argument("--refresh-sec", type=int, default=3600, help="refresh slugs/tokens interval in seconds")
    args = ap.parse_args()

    if args.slugs.strip():
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
    else:
        slugs = build_slug_window(args.back_hours, args.forward_hours)
    outdir = args.outdir.strip() or None
    if outdir:
        os.makedirs(outdir, exist_ok=True)

    asyncio.run(
        Probe(
            slugs=slugs,
            outdir=outdir,
            sample_sec=args.sample_sec,
            refresh_sec=args.refresh_sec,
            back_hours=args.back_hours,
            forward_hours=args.forward_hours,
        ).run()
    )


if __name__ == "__main__":
    main()
