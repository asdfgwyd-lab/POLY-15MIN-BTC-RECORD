#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Polymarket BTC 5m recorder (5s sampling, last 5m only), configured by .env

Features:
- Discover BTC 5m markets via Gamma API (open markets), filter by slug prefix.
- Record ONLY last 5 minutes of each market.
- Subscribe to CLOB Market WebSocket (L2/book + price_change), maintain best bid/ask in memory.
- Every SAMPLE_SEC seconds, write one row per active token to daily CSV.
- Auto reconnect with exponential backoff.
- Low memory: only keep latest best bid/ask per active token.

Output CSV (daily):
btc5m_ticks_YYYY-MM-DD.csv with columns:
sample_ts_utc,slug,side,token_id,bid,ask,mid,spread,last_book_update_ts
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
import websockets
from dotenv import load_dotenv

try:
    from sdnotify import SystemdNotifier as _SystemdNotifier
    SDNOTIFY_AVAILABLE = True
except Exception:
    SDNOTIFY_AVAILABLE = False

    class _SystemdNotifier:
        def notify(self, *_args, **_kwargs):
            return False

# ----------- endpoints -----------
GAMMA_BASE = "https://gamma-api.polymarket.com"
WSS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

SLUG_PREFIX = "btc-updown-5m-"

# ----------- utils -----------
def utc_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def utc_day_str(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")

def floor_to_grid(ts: int, step: int) -> int:
    return (ts // step) * step

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

def get_bool(v: Optional[str], default: bool) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def safe_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

def parse_list_field(x) -> List[str]:
    # Gamma sometimes returns list fields as JSON strings.
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

def best_price(levels: List[dict], side: str) -> Optional[float]:
    prices = [safe_float(l.get("price")) for l in levels if l.get("price") is not None]
    prices = [p for p in prices if p is not None]
    if not prices:
        return None
    return max(prices) if side == "bid" else min(prices)

# ----------- models -----------
@dataclass
class MarketInfo:
    slug: str
    end_ts: int
    token_yes: str
    token_no: str

# ----------- Gamma discovery -----------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "btc5m-ws-recorder/1.0"})

def gamma_list_markets(
    limit: int,
    offset: int,
    timeout_read: int,
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
) -> List[dict]:
    # Open markets only; optionally narrow by start_date window to reduce pagination.
    params = {"limit": limit, "offset": offset, "closed": "false"}
    if start_iso:
        params["start_date_min"] = start_iso
    if end_iso:
        params["start_date_max"] = end_iso
    r = SESSION.get(f"{GAMMA_BASE}/markets", params=params, timeout=(10, timeout_read))
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []

def gamma_get_market_by_slug(slug: str, timeout_read: int) -> Optional[dict]:
    try:
        r = SESSION.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=(10, timeout_read))
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def parse_start_ts_from_slug(slug: str) -> Optional[int]:
    try:
        return int(slug.split("-")[-1])
    except Exception:
        return None

def extract_yes_no_tokens(m: dict) -> Optional[Tuple[str, str]]:
    outcomes = parse_list_field(m.get("outcomes"))
    token_ids = parse_list_field(m.get("clobTokenIds"))

    if len(outcomes) < 2 or len(token_ids) < 2:
        return None

    outs = outcomes
    toks = token_ids

    if "Yes" in outs and "No" in outs:
        yi = outs.index("Yes")
        ni = outs.index("No")
        if yi < len(toks) and ni < len(toks):
            return toks[yi], toks[ni]

    # Fallback: first two as YES/NO (works if outcomes are UP/DOWN too; we still label as YES/NO in output)
    return toks[0], toks[1]

def discover_btc5m_markets(now_ts: int, lookahead_hours: int, timeout_read: int) -> List[MarketInfo]:
    """
    Real-time window strategy:
    - Construct slugs by 5m grid around now (start_ts in seconds).
    - Fetch market by slug to get endDate/clobTokenIds.
    - Keep only markets in a reasonable window around now.
    """
    out: List[MarketInfo] = []
    stats = {
        "slugs": 0,
        "slug_fetch_ok": 0,
        "end_ts_missing": 0,
        "window_skip": 0,
        "token_missing": 0,
    }

    # Generate slugs: from now-2h to now+lookahead_hours, step 5m
    back_hours = 2
    step = 5 * 60
    start_ts = floor_to_grid(now_ts - back_hours * 3600, step)
    end_ts = floor_to_grid(now_ts + lookahead_hours * 3600, step)

    for ts in range(start_ts, end_ts + 1, step):
        slug = f"{SLUG_PREFIX}{ts}"
        stats["slugs"] += 1

        m = gamma_get_market_by_slug(slug, timeout_read=timeout_read)
        if not m:
            continue
        stats["slug_fetch_ok"] += 1

        # Prefer endDate from Gamma; fallback to start_ts + 5m.
        end_iso = m.get("endDate") or m.get("endDateIso")
        end_ts_m = iso_to_ts(end_iso)
        if end_ts_m is None:
            end_ts_m = ts + 5 * 60

        # keep a reasonable window around now
        if end_ts_m < now_ts - 3600:
            stats["window_skip"] += 1
            continue
        if end_ts_m > now_ts + lookahead_hours * 3600:
            stats["window_skip"] += 1
            continue

        tokens = extract_yes_no_tokens(m)
        if not tokens:
            stats["token_missing"] += 1
            continue

        out.append(MarketInfo(slug=slug, end_ts=end_ts_m, token_yes=tokens[0], token_no=tokens[1]))

    out.sort(key=lambda x: x.end_ts)
    print(
        f"[gamma] slugs={stats['slugs']} slug_fetch_ok={stats['slug_fetch_ok']} "
        f"end_ts_missing={stats['end_ts_missing']} window_skip={stats['window_skip']} "
        f"token_missing={stats['token_missing']} kept={len(out)}",
        flush=True,
    )
    return out

# ----------- Recorder -----------
class Recorder:
    def __init__(
        self,
        outdir: str,
        sample_sec: int,
        refresh_sec: int,
        lookahead_hours: int,
        last5m_only: bool,
        gamma_timeout_read: int,
        ws_ping_interval: int,
        ws_ping_timeout: int,
        ws_stale_reconnect_sec: int,
        log_every_sec: int,
        watchdog_ping_sec: int,
    ):
        self.outdir = outdir
        self.sample_sec = sample_sec
        self.refresh_sec = refresh_sec
        self.lookahead_hours = lookahead_hours
        self.last5m_only = last5m_only
        self.gamma_timeout_read = gamma_timeout_read
        self.ws_ping_interval = ws_ping_interval
        self.ws_ping_timeout = ws_ping_timeout
        self.ws_stale_reconnect_sec = ws_stale_reconnect_sec
        self.log_every_sec = log_every_sec
        self.watchdog_ping_sec = watchdog_ping_sec

        os.makedirs(self.outdir, exist_ok=True)

        # token_id -> (bid, ask, last_book_update_ts)
        self.book: Dict[str, Tuple[Optional[float], Optional[float], Optional[int]]] = {}
        # token_id -> (slug, side)
        self.token_meta: Dict[str, Tuple[str, str]] = {}
        self.active_tokens: List[str] = []

        self._last_refresh_ts = 0
        self._last_msg_ts = 0
        self._last_log_ts = 0
        self._last_watchdog_ping = 0.0
        self._ready_sent = False
        self.notifier = _SystemdNotifier() if os.getenv("NOTIFY_SOCKET") else None

    def _csv_path_for_ts(self, ts: int) -> str:
        return os.path.join(self.outdir, f"btc5m_ticks_{utc_day_str(ts)}.csv")

    def _ensure_csv_header(self, path: str) -> None:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["sample_ts_utc", "slug", "side", "token_id", "bid", "ask", "mid", "spread", "last_book_update_ts"])

    def compute_active_tokens(self, markets: List[MarketInfo], now_ts: int) -> List[str]:
        meta: Dict[str, Tuple[str, str]] = {}
        active: List[str] = []

        for m in markets:
            if self.last5m_only:
                start_ts = m.end_ts - 5 * 60
                if not (start_ts <= now_ts <= m.end_ts):
                    continue
            else:
                if now_ts > m.end_ts:
                    continue

            meta[m.token_yes] = (m.slug, "YES")
            meta[m.token_no] = (m.slug, "NO")
            active.extend([m.token_yes, m.token_no])

        # dedupe preserving order
        seen = set()
        out = []
        for t in active:
            if t not in seen:
                seen.add(t)
                out.append(t)

        self.token_meta = meta
        return out

    async def ws_subscribe(self, ws, token_ids: List[str]) -> None:
        # market channel expects: {"type":"market","assets_ids":[...]}
        await ws.send(json.dumps({"type": "market", "assets_ids": token_ids}))

    def handle_ws_message(self, data: dict) -> None:
        self._last_msg_ts = int(time.time())
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

        # ignore other message types

    def write_sample(self, sample_ts: int) -> int:
        if not self.active_tokens:
            return 0

        path = self._csv_path_for_ts(sample_ts)
        self._ensure_csv_header(path)

        rows = []
        for token_id in self.active_tokens:
            slug, side = self.token_meta.get(token_id, ("", ""))
            bid, ask, last_u = self.book.get(token_id, (None, None, None))

            mid = None
            spread = None
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
                spread = ask - bid

            rows.append([
                utc_iso(sample_ts),
                slug,
                side,
                token_id,
                f"{bid:.6f}" if bid is not None else "",
                f"{ask:.6f}" if ask is not None else "",
                f"{mid:.6f}" if mid is not None else "",
                f"{spread:.6f}" if spread is not None else "",
                utc_iso(last_u) if last_u is not None else "",
            ])

        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerows(rows)
            f.flush()

        return len(rows)

    def _notify_ready(self) -> None:
        if self.notifier is None or self._ready_sent:
            return
        self.notifier.notify("READY=1")
        self._ready_sent = True
        print("[systemd] READY=1", flush=True)

    def _notify_watchdog(self, now_ts: float) -> None:
        if self.notifier is None or self.watchdog_ping_sec <= 0:
            return
        if (now_ts - self._last_watchdog_ping) < self.watchdog_ping_sec:
            return
        self.notifier.notify("WATCHDOG=1")
        self._last_watchdog_ping = now_ts

    async def run(self) -> None:
        backoff = 1.0
        now0 = int(time.time())
        next_sample = floor_to_grid(now0, self.sample_sec) + self.sample_sec

        while True:
            try:
                async with websockets.connect(
                    WSS_MARKET,
                    ping_interval=self.ws_ping_interval,
                    ping_timeout=self.ws_ping_timeout,
                    close_timeout=10,
                    max_queue=2048,
                ) as ws:
                    backoff = 1.0
                    self._last_refresh_ts = 0
                    self._last_msg_ts = int(time.time())
                    self._last_log_ts = 0
                    force_resubscribe = True
                    print(f"[ws] connected {WSS_MARKET}", flush=True)
                    self._notify_ready()
                    self._notify_watchdog(time.time())

                    while True:
                        now = int(time.time())
                        self._notify_watchdog(float(now))

                        # Refresh markets / active tokens
                        if now - self._last_refresh_ts >= self.refresh_sec:
                            markets = discover_btc5m_markets(
                                now_ts=now,
                                lookahead_hours=self.lookahead_hours,
                                timeout_read=self.gamma_timeout_read,
                            )
                            new_active = self.compute_active_tokens(markets, now_ts=now)
                            tokens_changed = new_active != self.active_tokens
                            self.active_tokens = new_active
                            if tokens_changed or force_resubscribe:
                                print(f"[refresh] active_tokens={len(self.active_tokens)} markets_in_window={len(self.token_meta)//2}", flush=True)
                                if self.active_tokens:
                                    await self.ws_subscribe(ws, self.active_tokens)
                                force_resubscribe = False
                            self._last_refresh_ts = now

                        # Sample every N seconds
                        if now >= next_sample:
                            wrote = self.write_sample(next_sample)
                            if self.log_every_sec > 0 and (now - self._last_log_ts) >= self.log_every_sec:
                                age = now - self._last_msg_ts
                                print(f"[sample] {utc_iso(next_sample)} wrote_rows={wrote} active_tokens={len(self.active_tokens)} last_msg_age={age}s", flush=True)
                                self._last_log_ts = now
                            next_sample += self.sample_sec

                        # Read WS messages with timeout so sampling continues
                        try:
                            if self.ws_stale_reconnect_sec > 0 and (now - self._last_msg_ts) >= self.ws_stale_reconnect_sec:
                                raise ConnectionError(
                                    f"stale market stream: last_msg_age={now - self._last_msg_ts}s >= {self.ws_stale_reconnect_sec}s"
                                )
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            if not msg:
                                continue
                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue
                            if isinstance(data, dict):
                                self.handle_ws_message(data)
                        except asyncio.TimeoutError:
                            pass

            except Exception as e:
                print(f"[ws] disconnected/error: {repr(e)}; retry in {backoff:.1f}s", flush=True)
                await asyncio.sleep(backoff + random.random() * 0.2)
                backoff = min(backoff * 2.0, 30.0)

# ----------- entry -----------
def main():
    load_dotenv()

    outdir = os.getenv("OUTDIR", "data")
    sample_sec = int(os.getenv("SAMPLE_SEC", "5"))
    refresh_sec = int(os.getenv("REFRESH_SEC", "60"))
    last5m_only = get_bool(os.getenv("LAST5M_ONLY"), get_bool(os.getenv("LAST15M_ONLY"), True))
    lookahead_hours = int(os.getenv("LOOKAHEAD_HOURS", "30"))

    gamma_timeout_read = int(os.getenv("GAMMA_TIMEOUT_READ_SEC", "30"))
    ws_ping_interval = int(os.getenv("WS_PING_INTERVAL_SEC", "20"))
    ws_ping_timeout = int(os.getenv("WS_PING_TIMEOUT_SEC", "20"))
    ws_stale_reconnect_sec = int(os.getenv("WS_STALE_RECONNECT_SEC", "120"))

    # log line interval (seconds). 60 means "about once a minute"
    log_every_sec = int(os.getenv("LOG_EVERY_SEC", "60"))
    watchdog_ping_sec = int(os.getenv("WATCHDOG_PING_SEC", "10"))

    if sample_sec <= 0:
        raise ValueError("SAMPLE_SEC must be > 0")
    if refresh_sec <= 0:
        raise ValueError("REFRESH_SEC must be > 0")
    if ws_stale_reconnect_sec < 0:
        raise ValueError("WS_STALE_RECONNECT_SEC must be >= 0")
    if watchdog_ping_sec < 0:
        raise ValueError("WATCHDOG_PING_SEC must be >= 0")
    if os.getenv("NOTIFY_SOCKET") and not SDNOTIFY_AVAILABLE:
        raise RuntimeError("NOTIFY_SOCKET is set but sdnotify is not installed. Run: pip install sdnotify")

    rec = Recorder(
        outdir=outdir,
        sample_sec=sample_sec,
        refresh_sec=refresh_sec,
        lookahead_hours=lookahead_hours,
        last5m_only=last5m_only,
        gamma_timeout_read=gamma_timeout_read,
        ws_ping_interval=ws_ping_interval,
        ws_ping_timeout=ws_ping_timeout,
        ws_stale_reconnect_sec=ws_stale_reconnect_sec,
        log_every_sec=log_every_sec,
        watchdog_ping_sec=watchdog_ping_sec,
    )

    asyncio.run(rec.run())

if __name__ == "__main__":
    main()


