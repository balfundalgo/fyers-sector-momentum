from __future__ import annotations

"""
FYERS Sector Momentum Breakout Strategy (history-first rebuild)

What this version adds:
- Closed 1m candles fetched only from FYERS history() and rebuilt into 5m candles
- Exact derivative leg resolver via FYERS symbol-master CSV (when provided)
- Separate Order WebSocket tracker for live execution mode
- Strict paper mode: no real orders are placed; entries/exits are simulated internally

FIX v2 (resolver):
- _infer_instrument_type: regex now matches space-separated CE/PE in Fyers description format
  e.g. "HCLTECH 27MAR2025 1200 CE" was failing with old r'\\dCE' pattern
- _normalize_row: for raw_type=="14", use fyers_symbol ticker (reliable) first, not description
- _load: prints CE/PE/FUT parsed counts so you can confirm CSV parsed correctly

Notes:
- In paper mode, strategy signals, entries, exits, SL/TP/trailing are fully simulated.
- In live mode, exact derivative execution requires a usable symbol-master CSV path.
"""

import csv
import json
import re
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws, order_ws

from fyers_token import generate_token
from fyers_connect import CLIENT_ID

IST = pytz.timezone("Asia/Kolkata")


# ============================================================================
# CONFIG
# ============================================================================
@dataclass
class StrategyConfig:
    nifty_symbol: str = "NSE:NIFTY50-INDEX"
    paper_trading: bool = True
    max_stock_move_pct: float = 3.0
    second_candle_max_range_pct: float = 1.0
    trailing_trigger_pct: float = 0.5
    risk_reward_ratio: float = 2.0
    force_exit_time: Tuple[int, int] = (15, 15)
    entry_resolution_minutes: int = 5
    trend_window_start: Tuple[int, int] = (9, 15)
    trend_window_end: Tuple[int, int] = (9, 25)
    poll_seconds_when_idle: float = 1.0
    max_history_rows: int = 500
    qty_option_lots: int = 1
    qty_future_lots: int = 1
    symbol_master_csv_path: Optional[str] = None
    symbol_master_refresh_days: int = 1
    constituents_cache_days: int = 15
    enable_order_socket_in_live_mode: bool = True
    entry_cutoff_time: Tuple[int, int] = (14, 55)
    use_breakout_confirmation: bool = True   # if True: wait for live price to breach min/max of 2 confirmed candles
    breakout_buffer_pct: float = 0.1         # entry buffer as % of spot price (e.g. 0.1 means 0.1% of spot below lowest low)
    sl_buffer_pct: float = 0.1              # SL buffer as % of entry price (e.g. 0.1 means price must go 0.1% beyond SL to trigger)
    max_trades_per_day: int = 2              # maximum number of stocks to trade per session


SECTORAL_INDICES: Dict[str, str] = {
    "NIFTY BANK": "NSE:NIFTYBANK-INDEX",
    "NIFTY IT": "NSE:NIFTYIT-INDEX",
    "NIFTY REALTY": "NSE:NIFTYREALTY-INDEX",
    "NIFTY INFRA": "NSE:NIFTYINFRA-INDEX",
    "NIFTY ENERGY": "NSE:NIFTYENERGY-INDEX",
    "NIFTY FMCG": "NSE:NIFTYFMCG-INDEX",
    "NIFTY PHARMA": "NSE:NIFTYPHARMA-INDEX",
    "NIFTY AUTO": "NSE:NIFTYAUTO-INDEX",
    "NIFTY METAL": "NSE:NIFTYMETAL-INDEX",
    "NIFTY FIN SERVICE": "NSE:FINNIFTY-INDEX",
}

SECTOR_CONSTITUENTS: Dict[str, List[str]] = {
    "NIFTY BANK": ["HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK", "INDUSINDBK"],
    "NIFTY IT": ["TCS", "INFY", "HCLTECH", "WIPRO", "TECHM", "LTIM"],
    "NIFTY PHARMA": ["SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "LUPIN"],
    "NIFTY AUTO": ["M&M", "MARUTI", "BAJAJ-AUTO", "EICHERMOT", "TATAMOTORS"],
    "NIFTY METAL": ["TATASTEEL", "HINDALCO", "JSWSTEEL", "VEDL", "NMDC"],
    "NIFTY REALTY": ["DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "LODHA"],
    "NIFTY ENERGY": ["RELIANCE", "ONGC", "NTPC", "POWERGRID", "BPCL"],
    "NIFTY FMCG": ["HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "TATACONSUM"],
    "NIFTY FIN SERVICE": ["HDFCBANK", "ICICIBANK", "BAJFINANCE", "BAJAJFINSV", "SBILIFE"],
    "NIFTY INFRA": ["RELIANCE", "BHARTIARTL", "LT", "ULTRACEMCO", "ADANIPORTS"],
}

FO_ELIGIBLE: set[str] = {
    "HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK", "INDUSINDBK",
    "TCS", "INFY", "HCLTECH", "WIPRO", "TECHM", "LTIM",
    "SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "LUPIN",
    "M&M", "MARUTI", "BAJAJ-AUTO", "EICHERMOT", "TATAMOTORS",
    "TATASTEEL", "HINDALCO", "JSWSTEEL", "VEDL", "NMDC",
    "DLF", "GODREJPROP", "OBEROIRLTY", "RELIANCE", "ONGC", "NTPC", "POWERGRID", "BPCL",
    "HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "TATACONSUM",
    "BAJFINANCE", "BAJAJFINSV", "SBILIFE", "BHARTIARTL", "LT", "ULTRACEMCO", "ADANIPORTS",
}


# ============================================================================
# DATA STRUCTURES
# ============================================================================
@dataclass
class Candle:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class SelectedStock:
    sector_name: str
    symbol: str
    fyers_symbol: str
    pct_change: float
    ltp: float


@dataclass
class ResolvedLeg:
    name: str
    symbol: str
    side: int  # 1 buy, -1 sell
    qty: int
    product_type: str = "INTRADAY"


@dataclass
class PaperOrder:
    paper_id: str
    symbol: str
    side: int
    qty: int
    entry_price: float
    status: str = "FILLED"
    entry_time: datetime = field(default_factory=lambda: datetime.now(IST))
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    entry_ltp: Optional[float] = None   # actual live LTP of this leg at entry
    exit_ltp: Optional[float] = None    # actual live LTP of this leg at exit


@dataclass
class TradeState:
    symbol: str
    direction: str
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_time: datetime
    trailing_armed: bool = False
    trail_step_count: int = 0          # how many 0.5% steps have fired so far
    live_order_ids: List[str] = field(default_factory=list)
    legs: List[ResolvedLeg] = field(default_factory=list)
    paper_orders: List[PaperOrder] = field(default_factory=list)
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_reason: Optional[str] = None


@dataclass
class StockRuntime:
    stock: SelectedStock
    pdh: float
    pdl: float
    setup_tracker: Any
    feed: Any
    trade: Optional[TradeState] = None
    last_5m_processed: Optional[datetime] = None
    has_traded: bool = False
    is_complete: bool = False


# ============================================================================
# HELPERS
# ============================================================================
def now_ist() -> datetime:
    return datetime.now(IST)


def dt_ist(y: int, m: int, d: int, hh: int, mm: int) -> datetime:
    return IST.localize(datetime(y, m, d, hh, mm))


def epoch_to_ist(ts: int | float) -> datetime:
    if ts > 1e12:
        ts = ts / 1000.0
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)


def floor_to_bucket(dt: datetime, minutes: int) -> datetime:
    minute = (dt.minute // minutes) * minutes
    return dt.replace(minute=minute, second=0, microsecond=0)


def pct_change(curr: float, prev: float) -> float:
    if prev == 0:
        return 0.0
    return ((curr - prev) / prev) * 100.0


def norm_symbol_base(symbol: str) -> str:
    """Extract the base ticker from any symbol form.

    Handles hyphenated names like BAJAJ-AUTO correctly by stripping known
    suffixes (-EQ, -INDEX, FO date codes) rather than blindly splitting at '-'.

    Examples:
        NSE:BAJAJ-AUTO-EQ           -> BAJAJ-AUTO
        NSE:BAJAJ-AUTO26MAR8900CE-EQ -> BAJAJ-AUTO
        NSE:HCLTECH26MAR1200CE-EQ   -> HCLTECH
        NSE:NIFTY50-INDEX           -> NIFTY50
        BAJAJ-AUTO                  -> BAJAJ-AUTO
    """
    s = symbol.split(":")[-1].upper()
    s = re.sub(r"-EQ$", "", s)
    s = re.sub(r"-INDEX$", "", s)
    # Strip FO date suffix: e.g. 26MAR8900CE, 26MAR, 26MARFUT
    s = re.sub(r"\d{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).*$", "", s)
    # Also handle space-separated dates from description: "BAJAJ-AUTO 30 Mar 26 8900 CE"
    s = re.sub(r"\s+\d{1,2}\s+(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).*$",
               "", s.strip(), flags=re.IGNORECASE)
    return s.strip()


# ── Symbol alias map ──────────────────────────────────────────────────────────
# Maps constituent ticker names (as downloaded from niftyindices.com) to the
# actual NSE F&O base symbol used in the symbol master CSV.
# This is needed because some companies trade under a different NSE ticker than
# their common name — e.g. Macrotech Developers is listed as LODHA on NSE but
# the constituent CSV from niftyindices.com uses "MACROTECH".
# Add new entries here whenever a mismatch is discovered.
CONSTITUENT_ALIAS_MAP: Dict[str, str] = {
    "MACROTECH":  "LODHA",        # Macrotech Developers → NSE ticker LODHA
    "LODHA":      "LODHA",        # already correct
    "ABREL":      "ABREL",        # Aditya Birla Real Estate
    "ANANTRAJ":   "ANANTRAJ",     # Anant Raj
    "PHOENIXLTD": "PHOENIXLTD",   # Phoenix Mills
    "M&M":        "M&M",          # Mahindra & Mahindra (hyphen)
    "BAJAJ-AUTO": "BAJAJ-AUTO",   # Bajaj Auto (hyphen)
}

# Reverse map: NSE F&O base → constituent ticker (for FO eligibility check)
_FO_BASE_TO_CONSTITUENT: Dict[str, str] = {v: k for k, v in CONSTITUENT_ALIAS_MAP.items()}

class FyersBroker:
    def __init__(self, access_token: Optional[str] = None) -> None:
        raw_token = access_token or generate_token()
        self.access_token = raw_token if ":" in raw_token else f"{CLIENT_ID}:{raw_token}"
        self.token_only = self.access_token.split(":", 1)[1]
        self.fyers = fyersModel.FyersModel(
            token=self.token_only,
            is_async=False,
            client_id=CLIENT_ID,
            log_path="",
        )

    def history(self, symbol: str, resolution: str, range_from: int, range_to: int) -> List[Candle]:
        payload = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "0",
            "range_from": str(range_from),
            "range_to": str(range_to),
            "cont_flag": "1",
        }
        resp = self.fyers.history(payload)
        candles: List[Candle] = []
        for row in resp.get("candles", []) or []:
            if len(row) < 6:
                continue
            candles.append(Candle(
                ts=epoch_to_ist(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
            ))
        return candles

    def quotes(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        payload = {"symbols": ",".join(symbols)}
        resp = self.fyers.quotes(payload)
        out: Dict[str, Dict[str, Any]] = {}
        for item in resp.get("d", []) or []:
            key = item.get("n") or item.get("symbol")
            v = item.get("v", {}) if isinstance(item.get("v"), dict) else {}
            if key:
                bid = float(v.get("bid_price", v.get("bid", 0.0)) or 0.0)
                ask = float(v.get("ask_price", v.get("ask", 0.0)) or 0.0)
                out[key] = {
                    "ltp":        float(v.get("lp", v.get("ltp", 0.0)) or 0.0),
                    "bid":        bid,
                    "ask":        ask,
                    "is_liquid":  bid > 0 and ask > 0,
                    "mid":        (bid + ask) / 2 if (bid > 0 and ask > 0) else 0.0,
                    "prev_close": float(v.get("prev_close_price", v.get("prevClose", 0.0)) or 0.0),
                    "open":       float(v.get("open_price", v.get("open", 0.0)) or 0.0),
                    "high":       float(v.get("high_price", v.get("high", 0.0)) or 0.0),
                    "low":        float(v.get("low_price", v.get("low", 0.0)) or 0.0),
                    "raw":        item,
                }
        return out

    def place_market_order(self, symbol: str, side: int, qty: int, product_type: str = "INTRADAY") -> Dict[str, Any]:
        payload = {
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": side,
            "productType": product_type,
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "stopLoss": 0,
            "takeProfit": 0,
            "isSliceOrder": False,
        }
        return self.fyers.place_order(payload)

    def close_live_position(self, leg: 'ResolvedLeg', reason: str) -> bool:
        """
        Close a live leg by placing a market order in the opposite direction.
        Returns True if the order was accepted, False on failure.

        Entry side:  1 = BUY  → exit side: -1 = SELL
        Entry side: -1 = SELL → exit side:  1 = BUY
        """
        exit_side    = -1 * leg.side
        side_txt     = "SELL" if exit_side == -1 else "BUY"
        try:
            resp = self.place_market_order(
                symbol       = leg.symbol,
                side         = exit_side,
                qty          = leg.qty,
                product_type = leg.product_type,
            )
            status = resp.get("s", "")
            oid    = resp.get("id") or resp.get("orderId") or ""
            if status == "ok":
                print(f"[LIVE EXIT] {reason} | {side_txt} {leg.symbol} x{leg.qty} → order_id={oid}")
                return True
            else:
                print(f"[LIVE EXIT ERROR] {side_txt} {leg.symbol} x{leg.qty} failed: {resp}")
                return False
        except Exception as e:
            print(f"[LIVE EXIT ERROR] {side_txt} {leg.symbol} x{leg.qty} exception: {e}")
            return False


# ============================================================================
# INSTRUMENT MASTER + RESOLVER
# ============================================================================
class InstrumentMaster:
    FYERS_FO_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"

    def __init__(self, csv_path: Optional[str], refresh_days: int = 1) -> None:
        self.refresh_days = max(1, int(refresh_days))
        self.csv_path = self._resolve_or_fetch_csv_path(csv_path)
        self.rows: List[Dict[str, str]] = []
        self.parsed_rows: List[Dict[str, Any]] = []
        self.available = False
        if self.csv_path and self.csv_path.exists():
            self._load()

    def _resolve_or_fetch_csv_path(self, csv_path: Optional[str]) -> Optional[Path]:
        if csv_path:
            p = Path(csv_path).expanduser()
            p.parent.mkdir(parents=True, exist_ok=True)
            if (not p.exists()) or self._is_stale(p):
                self._download_master(p)
            return p if p.exists() else None
        cache_dir = Path.cwd() / "cache" / "fyers"
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = cache_dir / "NSE_FO.csv"
        if (not target.exists()) or self._is_stale(target):
            self._download_master(target)
        if target.exists():
            print(f"[MASTER] Using cached symbol master: {target}")
            return target
        cwd = Path.cwd()
        candidates = [
            "fyers_symbol_master.csv",
            "fyers_fo_symbol_master.csv",
            "fyers_nfo_symbol_master.csv",
            "NSE_FO.csv",
            "NFO.csv",
            "fo.csv",
        ]
        for name in candidates:
            p = cwd / name
            if p.exists():
                print(f"[MASTER] Auto-detected symbol master: {p}")
                return p
        return None

    def _is_stale(self, path: Path) -> bool:
        age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
        return age > timedelta(days=self.refresh_days)

    def _download_master(self, target: Path) -> None:
        try:
            print(f"[MASTER] Downloading FYERS FO symbol master -> {target}")
            req = urllib.request.Request(self.FYERS_FO_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
            if not data:
                raise RuntimeError("empty response")
            target.write_bytes(data)
            print(f"[MASTER] Downloaded {len(data)} bytes from FYERS symbol master")
        except Exception as e:
            print(f"[MASTER WARN] Could not download FYERS symbol master: {e}")

    @staticmethod
    def _pick(d: Dict[str, str], *keys: str) -> str:
        lowered = {k.lower(): v for k, v in d.items()}
        for key in keys:
            if key.lower() in lowered:
                return lowered[key.lower()]
        return ""

    @staticmethod
    def _safe_float(v: Any) -> Optional[float]:
        try:
            if v is None or str(v).strip() == "":
                return None
            return float(str(v).replace(",", "").strip())
        except Exception:
            return None

    def _load(self) -> None:
        assert self.csv_path is not None
        with self.csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            try:
                has_header = csv.Sniffer().has_header(sample)
            except Exception:
                has_header = False

            # ── FIX: log header detection so you can diagnose column-name mismatches ──
            print(f"[MASTER] has_header={has_header} | first_line: {sample.splitlines()[0][:120] if sample else ''}")

            if has_header:
                reader = csv.DictReader(f)
                self.rows = [dict(r) for r in reader]
            else:
                fieldnames = [
                    "token", "description", "instrument_type_code", "lot_size", "tick_size",
                    "isin", "trading_session", "last_update_date", "expiry_epoch", "symbol",
                    "exchange", "segment", "scrip_code", "underlying", "underlying_code",
                    "strike", "option_type", "underlying_token", "reserved_1", "reserved_2", "ltp",
                ]
                reader = csv.reader(f)
                self.rows = []
                for raw in reader:
                    if not raw:
                        continue
                    self.rows.append({fieldnames[i]: raw[i] if i < len(raw) else "" for i in range(len(fieldnames))})

        self.parsed_rows = [self._normalize_row(r) for r in self.rows]
        self.available = bool(self.parsed_rows)
        print(f"[MASTER] Loaded {len(self.rows)} rows from {self.csv_path}")

        # ── FIX: sanity check — if CE/PE counts are 0, the regex or column mapping is wrong ──
        ce_count  = sum(1 for r in self.parsed_rows if r["instrument_type"] == "CE")
        pe_count  = sum(1 for r in self.parsed_rows if r["instrument_type"] == "PE")
        fut_count = sum(1 for r in self.parsed_rows if r["instrument_type"] == "FUT")
        print(f"[MASTER] Parsed types → CE: {ce_count}, PE: {pe_count}, FUT: {fut_count}")
        if ce_count == 0 or pe_count == 0:
            print("[MASTER WARN] CE/PE count is 0 — option resolver WILL fail. "
                  "Check that NSE_FO.csv columns match expected fieldnames above.")

    def fo_eligible_bases(self) -> set:
        """
        Return the set of base tickers that have active FO contracts in the loaded
        symbol master. This replaces the hardcoded FO_ELIGIBLE set with live data.

        A stock is considered FO-eligible if it has at least one FUT row in the master.
        Using FUT (not CE/PE) as the check because futures are the definitive indicator
        that a stock is in the F&O segment — every F&O stock has a FUT contract.
        """
        if not self.available:
            return set()
        return {
            r["base"]
            for r in self.parsed_rows
            if r["instrument_type"] == "FUT" and r["base"] and r["fyers_symbol"]
        }

    def _infer_instrument_type(self, symbol: str, raw_type: str) -> str:
        """
        FIX: Fyers NSE_FO.csv description column uses space-separated option type,
        e.g. "HCLTECH 27MAR2025 1200 CE" — the old r'\\dCE' regex required a digit
        immediately before CE/PE so it silently failed on these, leaving all options
        with instrument_type="14" instead of "CE"/"PE".

        New regex: r'(?:\\s|\\d)CE(?:\\b|-|$)' — matches both "1200CE" and "1200 CE".
        """
        txt = (raw_type or "").upper().replace(" ", "")
        sym = symbol.upper()
        if "FUT" in txt or "FUT" in sym:
            return "FUT"
        if "CE" in txt or re.search(r'(?:\s|\d)CE(?:\b|-|$)', sym):
            return "CE"
        if "PE" in txt or re.search(r'(?:\s|\d)PE(?:\b|-|$)', sym):
            return "PE"
        return txt

    def _infer_expiry(self, symbol: str, expiry_raw: str) -> Optional[date]:
        expiry_raw = (expiry_raw or "").strip()
        for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d%b%Y", "%d %b %Y"):
            if not expiry_raw:
                break
            try:
                return datetime.strptime(expiry_raw, fmt).date()
            except Exception:
                pass
        sym = symbol.upper()
        m = re.search(r'(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})', sym)
        if m:
            day = int(m.group(1))
            mon = datetime.strptime(m.group(2), "%b").month
            year = 2000 + int(m.group(3))
            try:
                return date(year, mon, day)
            except Exception:
                return None
        return None

    def _infer_strike(self, symbol: str, strike_raw: str) -> Optional[float]:
        strike = self._safe_float(strike_raw)
        if strike is not None:
            return strike
        sym = symbol.upper()
        m = re.search(r'(\d+(?:\.\d+)?)(CE|PE)(?:\b|$)', sym)
        if m:
            return self._safe_float(m.group(1))
        return None

    def _infer_base(self, symbol: str, underlying: str) -> str:
        if underlying:
            return norm_symbol_base(underlying)
        sym = symbol.split(":")[-1].upper()
        sym = re.sub(r"-EQ$", "", sym)
        sym = re.sub(r"-INDEX$", "", sym)
        # Strip compact FO date suffix (fyers_symbol format: no spaces)
        # e.g. "BAJAJ-AUTO26MAR11800CE" → "BAJAJ-AUTO"
        #      "M&M26MARFUT"            → "M&M"
        sym = re.sub(r"\d{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).*$", "", sym)
        # Strip spaced FO date suffix (description format, Windows CSV only has 11 columns
        # so underlying is empty and we fall back to the description string)
        # e.g. "BAJAJ-AUTO 30 MAR 26 11800 CE" → "BAJAJ-AUTO"
        #      "M&M 28 APR 26 FUT"              → "M&M"
        sym = re.sub(r"\s+\d{1,2}\s+(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).*$", "", sym)
        return sym.strip()

    def _normalize_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        display_symbol = self._pick(row, "description", "display_name", "symbol_desc", "symbol", "trading_symbol", "tradingsymbol")
        ticker_like    = self._pick(row, "symbol", "symbol_ticker", "fyers_symbol", "ticker", "tsym", "symticker", "sym_ticker")
        exchange       = (self._pick(row, "exchange", "exchange_name", "exchange_name_code", "exchangename", "exch") or "NSE").strip().upper()

        fyers_symbol = ticker_like if ":" in ticker_like else ""
        if not fyers_symbol and ticker_like:
            exch_prefix = exchange.split("-")[0].split("_")[0].strip() or "NSE"
            fyers_symbol = f"{exch_prefix}:{ticker_like}"
        if not display_symbol:
            display_symbol = fyers_symbol or ticker_like

        underlying  = self._pick(row, "underlying", "underlying_symbol", "under_sym", "underlyingasset", "underlying asset")
        raw_type    = self._pick(row, "instrument_type_code", "instrument_type", "instrument", "segment", "type", "opt_type")
        raw_type    = (raw_type or "").strip()
        option_type = (self._pick(row, "option_type", "opt_type") or "").strip().upper()

        # ── FIX: for raw_type=="14" (Fyers option code), prefer the explicit option_type
        # column first; if blank/invalid, infer from fyers_symbol ticker (e.g.
        # "NSE:HCLTECH25MAR1200CE-EQ") which is unambiguous, rather than from the
        # human-readable description which has space-separated CE/PE that the old
        # r'\dCE' regex couldn't match. ──
        if raw_type in {"11", "13"}:
            itype = "FUT"
        elif raw_type == "14":
            if option_type in {"CE", "PE"}:
                itype = option_type
            else:
                # Use ticker (fyers_symbol) — format like NSE:HCLTECH25MAR1200CE-EQ
                # is unambiguous for regex; fall back to description if ticker missing
                infer_src = fyers_symbol or display_symbol
                itype = self._infer_instrument_type(infer_src, raw_type or option_type)
        else:
            itype = self._infer_instrument_type(display_symbol or fyers_symbol, raw_type or option_type)

        expiry = None
        expiry_epoch = self._pick(row, "expiry_epoch")
        if expiry_epoch:
            try:
                expiry = datetime.fromtimestamp(int(float(expiry_epoch)), tz=timezone.utc).date()
            except Exception:
                expiry = None
        if expiry is None:
            expiry_raw = self._pick(row, "expiry", "expiry_date", "expirydate", "last_update_date")
            expiry = self._infer_expiry(display_symbol or fyers_symbol, expiry_raw)

        strike_raw = self._pick(row, "strike", "strike_price", "strikeprice")
        lot_raw    = self._pick(row, "lot_size", "lotsize", "qty_multiplier", "multiplier", "minimum_lot_size", "minqty")
        lot        = int(self._safe_float(lot_raw) or 1)

        normalized = {
            "base":             self._infer_base(fyers_symbol or display_symbol, underlying),
            "fyers_symbol":     fyers_symbol,
            "display_symbol":   display_symbol,
            "instrument_type":  itype,
            "expiry":           expiry,
            "strike":           self._infer_strike(display_symbol or fyers_symbol, strike_raw),
            "lot_size":         max(lot, 1),
        }
        return normalized

    def _pick_liquid_strike(self, candidates: List[Dict], broker: Optional['FyersBroker'], opt_type: str) -> Dict:
        """
        Walk the candidate strikes (sorted closest-to-spot first) and return the
        first one that has a live bid AND ask (i.e. is actually tradeable right now).

        Checks up to 5 nearest strikes. If none are liquid (e.g. very early morning,
        illiquid stock) falls back to ATM with a warning so we don't block the trade.

        broker=None means no liquidity check — just return ATM directly.
        """
        if broker is None:
            return candidates[0]

        MAX_STRIKES_TO_CHECK = 5
        for row in candidates[:MAX_STRIKES_TO_CHECK]:
            sym = row["fyers_symbol"]
            try:
                q = broker.quotes([sym])
                d = q.get(sym, {})
                bid = float(d.get("bid") or 0.0)
                ask = float(d.get("ask") or 0.0)
                if bid > 0 and ask > 0:
                    mid = (bid + ask) / 2
                    print(f"[RESOLVER] Liquid {opt_type} strike: {sym}  bid={bid:.2f}  ask={ask:.2f}  mid={mid:.2f}")
                    return row
                else:
                    strike = row.get("strike", "?")
                    print(f"[RESOLVER] Illiquid {opt_type} strike {strike} ({sym})  bid={bid}  ask={ask} — skipping")
            except Exception as e:
                print(f"[RESOLVER] Quote check failed for {sym}: {e}")

        # All checked strikes are illiquid — fall back to ATM and warn
        atm = candidates[0]
        print(f"[RESOLVER WARN] No liquid {opt_type} strike found in top {MAX_STRIKES_TO_CHECK} — "
              f"falling back to ATM {atm['fyers_symbol']} (may be stale LTP)")
        return atm

    def resolve(self, base_symbol: str, spot_price: float, direction: str, opt_lots: int, fut_lots: int,
                broker: Optional['FyersBroker'] = None) -> List[ResolvedLeg]:
        if not self.available:
            raise RuntimeError(
                "Symbol master CSV not loaded. Set StrategyConfig.symbol_master_csv_path "
                "or keep a FO symbol-master CSV in the script folder."
            )
        base      = base_symbol.upper()
        base_rows = [r for r in self.parsed_rows if r["base"] == base and r["fyers_symbol"]]

        # ── Safety net: description prefix scan ─────────────────────────────────────
        # The primary fix is using fyers_symbol in _infer_base (no spaces → regex
        # always strips correctly). This scan fires only if somehow base still misses.
        if not base_rows:
            prefix = (base + " ").upper()
            base_rows = [
                r for r in self.parsed_rows
                if r["fyers_symbol"] and r.get("display_symbol", "").upper().startswith(prefix)
            ]
            if base_rows:
                print(f"[RESOLVER] Fallback description scan matched {base}: {len(base_rows)} rows")

        if not base_rows:
            # ── Diagnostic: show what bases ARE available (helps debug expiry/naming issues) ──
            all_bases = sorted({r["base"] for r in self.parsed_rows if r["fyers_symbol"]})
            similar   = [b for b in all_bases if b.startswith(base[:3])]
            print(f"[RESOLVER DIAG] '{base}' not found. Similar bases: {similar or all_bases[:10]}")
            raise RuntimeError(f"No master rows found for {base}")

        today    = now_ist().date()
        fut_rows = [r for r in base_rows if r["instrument_type"] == "FUT" and r["expiry"] and r["expiry"] >= today]
        opt_rows = [r for r in base_rows if r["instrument_type"] in {"CE", "PE"} and r["expiry"] and r["expiry"] >= today and r["strike"] is not None]

        if not fut_rows:
            raise RuntimeError(f"No live future contract found for {base}")
        if not opt_rows:
            raise RuntimeError(f"No live option contracts found for {base}")

        fut_rows.sort(key=lambda x: (x["expiry"], x["display_symbol"]))
        nearest_future = fut_rows[0]

        nearest_expiry = min(r["expiry"] for r in opt_rows if r["expiry"] is not None)
        typed = [
            r for r in opt_rows
            if r["expiry"] == nearest_expiry
            and r["instrument_type"] == ("PE" if direction == "BULLISH" else "CE")
        ]
        if not typed:
            raise RuntimeError(f"No matching option contracts found for {base} on nearest expiry")

        typed.sort(key=lambda x: (abs(float(x["strike"]) - spot_price), float(x["strike"]), x["display_symbol"]))
        opt_type = "PE" if direction == "BULLISH" else "CE"
        atm = self._pick_liquid_strike(typed, broker, opt_type)

        option_side  = 1                                    # always BUY the option (PE for bullish, CE for bearish)
        future_side  = 1 if direction == "BULLISH" else -1  # BUY FUT for bullish, SELL FUT for bearish
        option_qty   = int(atm["lot_size"]) * max(int(opt_lots), 1)
        future_qty   = int(nearest_future["lot_size"]) * max(int(fut_lots), 1)

        print(
            f"[RESOLVER] {base} spot={spot_price:.2f} -> "
            f"{atm['instrument_type']} {atm['display_symbol']} "
            f"(strike={float(atm['strike']):.2f}, expiry={atm['expiry']}) + "
            f"FUT {nearest_future['display_symbol']} (expiry={nearest_future['expiry']})"
        )

        return [
            ResolvedLeg(name="atm_option",  symbol=atm["fyers_symbol"],             side=option_side, qty=option_qty),
            ResolvedLeg(name="future",       symbol=nearest_future["fyers_symbol"],  side=future_side, qty=future_qty),
        ]


# ============================================================================
# ORDER SOCKET TRACKER (live mode only)
# ============================================================================
class OrderSocketTracker:
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.socket = None
        self.order_events:    List[Dict[str, Any]] = []
        self.trade_events:    List[Dict[str, Any]] = []
        self.position_events: List[Dict[str, Any]] = []

    def on_order(self, msg: Dict[str, Any]) -> None:
        self.order_events.append(msg)
        print("[ORDER WS][ORDER]", json.dumps(msg, default=str)[:600])

    def on_trade(self, msg: Dict[str, Any]) -> None:
        self.trade_events.append(msg)
        print("[ORDER WS][TRADE]", json.dumps(msg, default=str)[:600])

    def on_position(self, msg: Dict[str, Any]) -> None:
        self.position_events.append(msg)
        print("[ORDER WS][POSITION]", json.dumps(msg, default=str)[:600])

    def on_general(self, msg: Dict[str, Any]) -> None:
        print("[ORDER WS][GENERAL]", json.dumps(msg, default=str)[:400])

    def on_connect(self) -> None:
        print("[ORDER WS] Connected")
        self.socket.subscribe(data_type="OnOrders,OnTrades,OnPositions,OnGeneral")
        self.socket.keep_running()

    def on_error(self, msg: Dict[str, Any]) -> None:
        print("[ORDER WS][ERROR]", msg)

    def on_close(self, msg: Dict[str, Any]) -> None:
        print("[ORDER WS][CLOSED]", msg)

    def start(self) -> None:
        self.socket = order_ws.FyersOrderSocket(
            access_token=self.access_token,
            write_to_file=False,
            log_path="",
            on_connect=self.on_connect,
            on_close=self.on_close,
            on_error=self.on_error,
            on_general=self.on_general,
            on_orders=self.on_order,
            on_positions=self.on_position,
            on_trades=self.on_trade,
        )
        self.socket.connect()


# ============================================================================
# WEBSOCKET + HISTORY-SYNC FEED
# ============================================================================
class LiveSymbolFeed:
    def __init__(self, broker: FyersBroker, access_token: str, symbol: str, max_history_rows: int = 500) -> None:
        self.broker           = broker
        self.access_token     = access_token
        self.symbol           = symbol
        self.max_history_rows = max_history_rows
        self.socket           = None
        self.closed_1m:        List[Candle] = []
        self.closed_5m:        List[Candle] = []
        self.last_price:       Optional[float] = None
        self.first_market_payload_seen = False
        self.last_fetched_1m_bucket:   Optional[datetime] = None

    def _extract_price(self, msg: Dict[str, Any]) -> Optional[float]:
        for key in ("ltp", "lp", "last_price", "price", "c"):
            if key in msg and msg[key] is not None:
                try:
                    return float(msg[key])
                except Exception:
                    pass
        return None

    def _extract_ts(self, msg: Dict[str, Any]) -> Optional[int]:
        for key in ("timestamp", "t", "tt", "exchange_time", "last_traded_time", "exch_feed_time"):
            if key in msg and msg[key] is not None:
                try:
                    return int(msg[key])
                except Exception:
                    pass
        return None

    def _unwrap_ws_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(item.get("v"), dict):
            merged = dict(item["v"])
            for k in ("symbol", "n", "t", "timestamp", "tt"):
                if k not in merged and k in item:
                    merged[k] = item[k]
            return merged
        return item

    def _append_closed_1m(self, candle: Candle) -> None:
        existing = {c.ts: c for c in self.closed_1m[-10:]}
        existing[candle.ts] = candle
        prefix = [c for c in self.closed_1m[:-10] if c.ts not in existing]
        suffix = sorted(existing.values(), key=lambda x: x.ts)
        self.closed_1m = (prefix + suffix)[-self.max_history_rows:]
        self.last_fetched_1m_bucket = candle.ts

    def _fetch_1m_candle(self, bucket_1m: datetime) -> Optional[Candle]:
        start_ts = int(bucket_1m.timestamp())
        end_ts   = start_ts + 60
        candles  = self.broker.history(self.symbol, "1", start_ts, end_ts)
        for c in candles:
            c_bucket = c.ts.replace(second=0, microsecond=0)
            if c_bucket == bucket_1m:
                return Candle(ts=bucket_1m, open=c.open, high=c.high, low=c.low, close=c.close, volume=c.volume)
        return None

    def sync_to_now(self) -> None:
        active_minute      = now_ist().replace(second=0, microsecond=0)
        target_last_closed = active_minute - timedelta(minutes=1)
        if self.last_fetched_1m_bucket is None:
            next_bucket = target_last_closed
        else:
            next_bucket = self.last_fetched_1m_bucket + timedelta(minutes=1)
        while next_bucket <= target_last_closed:
            candle = self._fetch_1m_candle(next_bucket)
            if candle is not None:
                self._append_closed_1m(candle)
                print(f"[1M SYNC] {candle.ts.strftime('%H:%M')} O={candle.open:.2f} H={candle.high:.2f} L={candle.low:.2f} C={candle.close:.2f}")
            next_bucket += timedelta(minutes=1)
        self._rebuild_5m()

    def _bootstrap_recent_1m(self, lookback_minutes: int = 30) -> None:
        current_minute = now_ist().replace(second=0, microsecond=0)
        last_closed    = current_minute - timedelta(minutes=1)
        start          = last_closed - timedelta(minutes=max(lookback_minutes - 1, 0))
        candles        = self.broker.history(self.symbol, "1", int(start.timestamp()), int((last_closed + timedelta(minutes=1)).timestamp()))
        rows = sorted(candles, key=lambda x: x.ts)
        for c in rows:
            bucket = c.ts.replace(second=0, microsecond=0)
            if start <= bucket <= last_closed:
                self._append_closed_1m(Candle(ts=bucket, open=c.open, high=c.high, low=c.low, close=c.close, volume=c.volume))
        self._rebuild_5m()
        if self.closed_1m:
            print(f"[BOOTSTRAP] Loaded {len(self.closed_1m)} official 1m candles through {self.closed_1m[-1].ts.strftime('%H:%M')}")

    def on_message(self, message: Dict[str, Any]) -> None:
        if isinstance(message, dict) and message.get("s") == "ok":
            print("[WS CTRL PAYLOAD]", json.dumps(message, default=str)[:500])
            return

        items: List[Dict[str, Any]] = []
        if isinstance(message, dict) and isinstance(message.get("d"), list):
            items.extend(x for x in message["d"] if isinstance(x, dict))
        elif isinstance(message, dict):
            items.append(message)

        for raw_item in items:
            item       = self._unwrap_ws_item(raw_item)
            msg_symbol = item.get("symbol") or item.get("n")
            if msg_symbol and msg_symbol != self.symbol:
                continue
            px = self._extract_price(item)
            ts = self._extract_ts(item)
            if px is not None:
                self.last_price = px
            if ts is not None:
                _ = epoch_to_ist(ts)

    def _rebuild_5m(self) -> None:
        buckets: Dict[datetime, List[Candle]] = {}
        for c in self.closed_1m:
            k = floor_to_bucket(c.ts, 5)
            buckets.setdefault(k, []).append(c)
        built: List[Candle] = []
        for k in sorted(buckets):
            rows = sorted(buckets[k], key=lambda x: x.ts)
            if len(rows) < 5:
                continue
            built.append(Candle(
                ts=k,
                open=rows[0].open,
                high=max(r.high for r in rows),
                low=min(r.low for r in rows),
                close=rows[-1].close,
                volume=sum(r.volume for r in rows),
            ))
        self.closed_5m = built[-200:]

    def on_connect(self) -> None:
        print(f"[WS] Connected. Subscribing to {self.symbol}")
        self.socket.subscribe(symbols=[self.symbol], data_type="SymbolUpdate")
        self.socket.keep_running()

    def on_error(self, message: Dict[str, Any]) -> None:
        print("[WS ERROR]", message)

    def on_close(self, message: Dict[str, Any]) -> None:
        print("[WS CLOSED]", message)

    def start(self) -> None:
        self._bootstrap_recent_1m()


class SharedMarketDataSocket:
    def __init__(self, access_token: str, feeds: List[LiveSymbolFeed]) -> None:
        self.access_token      = access_token
        self.feeds_by_symbol: Dict[str, LiveSymbolFeed] = {feed.symbol: feed for feed in feeds}
        self.socket            = None
        self.first_market_payload_seen = False
        self.ltp_cache: Dict[str, float] = {}  # live LTP for ALL subscribed symbols (equity + F&O)

    def subscribe_additional(self, symbols: List[str]) -> None:
        """Subscribe extra symbols (e.g. option/future legs) after initial connect."""
        if self.socket is None:
            return
        try:
            self.socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
            print(f"[WS] Subscribed additional: {symbols}")
        except Exception as e:
            print(f"[WS WARN] Could not subscribe {symbols}: {e}")

    def on_connect(self) -> None:
        symbols = list(self.feeds_by_symbol.keys())
        print(f"[WS] Connected. Subscribing to {len(symbols)} symbols.")
        self.socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
        self.socket.keep_running()

    def on_error(self, message: Dict[str, Any]) -> None:
        print("[WS ERROR]", message)

    def on_close(self, message: Dict[str, Any]) -> None:
        print("[WS CLOSED]", message)

    def on_message(self, message: Dict[str, Any]) -> None:
        if isinstance(message, dict) and message.get("s") == "ok":
            print("[WS CTRL PAYLOAD]", json.dumps(message, default=str)[:500])
            return

        items: List[Dict[str, Any]] = []
        if isinstance(message, dict) and isinstance(message.get("d"), list):
            items.extend(x for x in message["d"] if isinstance(x, dict))
        elif isinstance(message, dict):
            items.append(message)

        for raw_item in items:
            item = raw_item
            if isinstance(item.get("v"), dict):
                merged = dict(item["v"])
                for k in ("symbol", "n", "t", "timestamp", "tt"):
                    if k not in merged and k in item:
                        merged[k] = item[k]
                item = merged

            if not self.first_market_payload_seen:
                print("[WS FIRST MARKET PAYLOAD]", json.dumps(item, default=str)[:800])
                self.first_market_payload_seen = True

            msg_symbol = item.get("symbol") or item.get("n")
            if not msg_symbol:
                continue
            # Cache LTP for every symbol (equity AND F&O legs)
            ltp_val = None
            for k in ("ltp", "lp", "last_price"):
                if k in item and item[k] is not None:
                    try:
                        ltp_val = float(item[k])
                        break
                    except Exception:
                        pass
            if ltp_val is not None:
                self.ltp_cache[msg_symbol] = ltp_val
                if ":" not in msg_symbol:
                    self.ltp_cache[f"NSE:{msg_symbol}"] = ltp_val
            feed = self.feeds_by_symbol.get(msg_symbol)
            if feed is None and ":" not in msg_symbol:
                feed = self.feeds_by_symbol.get(f"NSE:{msg_symbol}")
            if feed is None:
                continue
            feed.on_message(item)

    def start(self) -> None:
        if not self.feeds_by_symbol:
            return
        self.socket = data_ws.FyersDataSocket(
            access_token=self.access_token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=self.on_connect,
            on_close=self.on_close,
            on_error=self.on_error,
            on_message=self.on_message,
        )
        self.socket.connect()


# ============================================================================
# STRATEGY PHASES
# ============================================================================
NIFTY_CONSTITUENT_URLS: Dict[str, str] = {
    "NIFTY BANK":        "https://www.niftyindices.com/IndexConstituent/ind_niftybanklist.csv",
    "NIFTY IT":          "https://www.niftyindices.com/IndexConstituent/ind_niftyitlist.csv",
    "NIFTY REALTY":      "https://www.niftyindices.com/IndexConstituent/ind_niftyrealtylist.csv",
    "NIFTY INFRA":       "https://www.niftyindices.com/IndexConstituent/ind_niftyinfralist.csv",
    "NIFTY ENERGY":      "https://www.niftyindices.com/IndexConstituent/ind_niftyenergylist.csv",
    "NIFTY FMCG":        "https://www.niftyindices.com/IndexConstituent/ind_niftyfmcglist.csv",
    "NIFTY PHARMA":      "https://www.niftyindices.com/IndexConstituent/ind_niftypharmalist.csv",
    "NIFTY AUTO":        "https://www.niftyindices.com/IndexConstituent/ind_niftyautolist.csv",
    "NIFTY METAL":       "https://www.niftyindices.com/IndexConstituent/ind_niftymetallist.csv",
    "NIFTY FIN SERVICE": "https://www.niftyindices.com/IndexConstituent/ind_niftyfinancelist.csv",
}


def _cache_is_fresh(path: Path, days: int) -> bool:
    if not path.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age <= timedelta(days=days)


def _download_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    return data.decode("utf-8", errors="ignore")


def load_sector_constituents(sector_name: str, cache_days: int) -> List[str]:
    cache_dir  = Path.cwd() / "cache" / "niftyindices_constituents"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / (sector_name.lower().replace(" ", "_") + ".json")

    if _cache_is_fresh(cache_path, cache_days):
        try:
            data    = json.loads(cache_path.read_text())
            symbols = [str(x).strip().upper() for x in data.get("symbols", []) if str(x).strip()]
            if symbols:
                print(f"[CONSTITUENTS] Using cached {sector_name}: {len(symbols)} symbols")
                return symbols
        except Exception:
            pass

    url = NIFTY_CONSTITUENT_URLS.get(sector_name)
    if url:
        try:
            raw    = _download_text(url)
            lines  = [ln for ln in raw.splitlines() if ln.strip()]
            reader = csv.DictReader(lines)
            symbols: List[str] = []
            for row in reader:
                row_low = {str(k).lower(): ("" if v is None else str(v).strip()) for k, v in row.items()}
                sym = row_low.get("symbol") or row_low.get("ticker")
                if sym:
                    sym = sym.upper()
                    if sym not in symbols:
                        symbols.append(sym)
            if symbols:
                cache_path.write_text(json.dumps({
                    "sector":     sector_name,
                    "fetched_at": now_ist().isoformat(),
                    "source":     url,
                    "symbols":    symbols,
                }, indent=2))
                print(f"[CONSTITUENTS] Downloaded and cached {sector_name}: {len(symbols)} symbols")
                return symbols
            print(f"[CONSTITUENTS WARN] Empty constituent parse for {sector_name} from {url}")
        except Exception as e:
            print(f"[CONSTITUENTS WARN] Could not download {sector_name} constituents: {e}")

    fallback = SECTOR_CONSTITUENTS.get(sector_name, [])
    if fallback:
        print(f"[CONSTITUENTS] Using static fallback for {sector_name}: {len(fallback)} symbols")
    return fallback


class StrategyPhases:
    def __init__(self, broker: FyersBroker, cfg: StrategyConfig, master: "InstrumentMaster") -> None:
        self.broker         = broker
        self.cfg            = cfg
        self.broker_master  = master  # used for live FO eligibility check

    def identify_day_trend(self, session_date: datetime) -> str:
        start   = dt_ist(session_date.year, session_date.month, session_date.day, *self.cfg.trend_window_start)
        end     = dt_ist(session_date.year, session_date.month, session_date.day, *self.cfg.trend_window_end)
        candles = self.broker.history(self.cfg.nifty_symbol, "10", int(start.timestamp()), int(end.timestamp()))
        if not candles:
            raise RuntimeError("Could not fetch first 10-minute NIFTY candle")
        first = candles[0]
        trend = "BULLISH" if first.close > first.open else "BEARISH"
        print(f"[TREND] O={first.open:.2f} C={first.close:.2f} => {trend}")
        return trend

    def rank_sectors(self, direction: str) -> List[Tuple[str, float]]:
        quote_map = self.broker.quotes(list(SECTORAL_INDICES.values()))
        scored: List[Tuple[str, float]] = []
        for sector_name, fy_symbol in SECTORAL_INDICES.items():
            q = quote_map.get(fy_symbol)
            if not q:
                continue
            chg = pct_change(q["ltp"], q["prev_close"])
            scored.append((sector_name, chg))
        scored.sort(key=lambda x: x[1], reverse=(direction == "BULLISH"))
        print("[SECTORS] Ranked:", scored[:5])
        return scored

    def _sector_candidates(self, sector_name: str, direction: str) -> List[SelectedStock]:
        stocks        = load_sector_constituents(sector_name, self.cfg.constituents_cache_days)
        fyers_symbols = [f"NSE:{s}-EQ" for s in stocks]
        quote_map     = self.broker.quotes(fyers_symbols)

        # Use the live symbol master to determine FO eligibility — more accurate
        # than the hardcoded FO_ELIGIBLE set which goes stale as NSE adds/removes stocks.
        # Falls back to the hardcoded set only if master is not loaded.
        live_fo_bases = self.broker_master.fo_eligible_bases() if hasattr(self, "broker_master") else set()
        fo_check      = live_fo_bases if live_fo_bases else FO_ELIGIBLE

        candidates: List[SelectedStock] = []
        for s in stocks:
            # ── Alias resolution ─────────────────────────────────────────────
            # The constituent ticker from niftyindices.com may differ from the
            # NSE F&O base in the symbol master. Check both the original ticker
            # and any known alias so we don't miss eligible stocks.
            fo_ticker = CONSTITUENT_ALIAS_MAP.get(s, s)
            if fo_ticker not in fo_check and s not in fo_check:
                continue
            # Use whichever ticker is confirmed in F&O
            effective_ticker = fo_ticker if fo_ticker in fo_check else s
            fy = f"NSE:{s}-EQ"
            q  = quote_map.get(fy)
            if not q:
                continue
            chg = pct_change(q["ltp"], q["prev_close"])
            if abs(chg) > self.cfg.max_stock_move_pct:
                continue
            candidates.append(SelectedStock(sector_name=sector_name, symbol=effective_ticker, fyers_symbol=fy, pct_change=chg, ltp=q["ltp"]))

        if not candidates:
            raise RuntimeError(f"No eligible stock found for {sector_name}")

        candidates.sort(key=lambda x: x.pct_change, reverse=(direction == "BULLISH"))
        return candidates

    def select_stock(self, sector_name: str, direction: str) -> SelectedStock:
        candidates = self._sector_candidates(sector_name, direction)
        chosen     = candidates[0]
        print(f"[STOCK] {chosen.symbol} ({chosen.fyers_symbol}) change={chosen.pct_change:.2f}%")
        return chosen

    def select_stocks(self, sector_name: str, direction: str) -> List[SelectedStock]:
        candidates = self._sector_candidates(sector_name, direction)
        preview    = ", ".join(f"{c.symbol}({c.pct_change:.2f}%)" for c in candidates[:10])
        suffix     = " ..." if len(candidates) > 10 else ""
        print(f"[STOCKS] Monitoring {len(candidates)} eligible stocks in {sector_name}: {preview}{suffix}")
        return candidates

    def fetch_pdh_pdl(self, fyers_symbol: str, asof_date: datetime) -> Tuple[float, float]:
        asof_day = asof_date.date()

        def _pick_from_daily() -> Optional[Candle]:
            start = dt_ist(asof_date.year, asof_date.month, asof_date.day, 0, 0) - timedelta(days=45)
            end   = dt_ist(asof_date.year, asof_date.month, asof_date.day, 23, 59)
            daily = self.broker.history(fyers_symbol, "D", int(start.timestamp()), int(end.timestamp()))
            if not daily:
                return None
            by_date: Dict[date, Candle] = {}
            for c in sorted(daily, key=lambda x: x.ts):
                d = c.ts.date()
                prev_seen = by_date.get(d)
                if prev_seen is None or c.ts > prev_seen.ts:
                    by_date[d] = c
            prior_days = sorted(d for d in by_date.keys() if d < asof_day)
            if prior_days:
                return by_date[prior_days[-1]]
            ordered = sorted(by_date.values(), key=lambda x: x.ts)
            if len(ordered) >= 2:
                return ordered[-2]
            return None

        def _pick_from_intraday() -> Optional[Candle]:
            start = dt_ist(asof_date.year, asof_date.month, asof_date.day, 0, 0) - timedelta(days=10)
            end   = dt_ist(asof_date.year, asof_date.month, asof_date.day, 23, 59)
            mins  = self.broker.history(fyers_symbol, "1", int(start.timestamp()), int(end.timestamp()))
            if not mins:
                return None
            by_date: Dict[date, List[Candle]] = {}
            for c in mins:
                d = c.ts.date()
                if d >= asof_day:
                    continue
                by_date.setdefault(d, []).append(c)
            if not by_date:
                return None
            last_day = sorted(by_date.keys())[-1]
            rows     = sorted(by_date[last_day], key=lambda x: x.ts)
            if not rows:
                return None
            return Candle(
                ts=rows[-1].ts,
                open=rows[0].open,
                high=max(r.high for r in rows),
                low=min(r.low for r in rows),
                close=rows[-1].close,
                volume=sum(r.volume for r in rows),
            )

        last   = _pick_from_daily()
        source = "daily"
        if last is None:
            last   = _pick_from_intraday()
            source = "intraday-rebuild"

        if last is None:
            raise RuntimeError(f"Could not fetch previous daily candle for {fyers_symbol}")

        print(f"[PDH/PDL] {fyers_symbol} => PDH={last.high:.2f} PDL={last.low:.2f} ({source})")
        return last.high, last.low


# ============================================================================
# EXECUTION LAYER
# ============================================================================
class PaperLedger:
    def __init__(self, broker: 'FyersBroker') -> None:
        self.broker  = broker
        self.counter = 0

    def _fetch_entry_price(self, symbol: str) -> Optional[float]:
        """
        Fetch a realistic entry price for a symbol.

        Priority:
          1. (bid + ask) / 2  — if bid > 0 AND ask > 0 (live, liquid market)
          2. ltp              — last traded price (may be stale for illiquid options)
          3. None             — if quotes API fails entirely

        Using the bid/ask midpoint avoids the BOSCHLTD-style bug where the API
        returns a stale LTP from the previous session for an option that has had
        no trades yet in the current session.
        """
        try:
            q = self.broker.quotes([symbol])
            d = q.get(symbol, {})
            bid = float(d.get("bid") or 0.0)
            ask = float(d.get("ask") or 0.0)
            ltp = float(d.get("ltp") or 0.0) or None

            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2
                print(f"[LTP] {symbol}  bid={bid:.2f}  ask={ask:.2f}  mid={mid:.2f}  ltp={ltp}  → using MID (liquid)")
                return mid
            elif ltp:
                print(f"[LTP] {symbol}  bid={bid}  ask={ask}  ltp={ltp:.2f}  → using LTP (illiquid/stale warning)")
                return ltp
            else:
                print(f"[LTP WARN] {symbol}  bid=0  ask=0  ltp=0 — no price available")
                return None
        except Exception as e:
            print(f"[LTP WARN] Could not fetch quote for {symbol}: {e}")
            return None

    def _fetch_ltp(self, symbol: str) -> Optional[float]:
        """Alias kept for exit-side calls (exit LTP still uses best available price)."""
        return self._fetch_entry_price(symbol)

    def open_orders(self, legs: List[ResolvedLeg], ref_price: float) -> List[PaperOrder]:
        out: List[PaperOrder] = []
        for leg in legs:
            self.counter += 1
            paper_id  = f"PAPER-{self.counter:05d}"
            entry_ltp = self._fetch_entry_price(leg.symbol)   # mid price when liquid, LTP otherwise
            po        = PaperOrder(paper_id=paper_id, symbol=leg.symbol, side=leg.side,
                                   qty=leg.qty, entry_price=ref_price, entry_ltp=entry_ltp)
            out.append(po)
            side_txt  = "BUY" if leg.side == 1 else "SELL"
            ltp_str   = f" live_ltp={entry_ltp:.2f}" if entry_ltp is not None else " live_ltp=N/A"
            print(f"[PAPER OPEN] {paper_id} {side_txt} {leg.symbol} qty={leg.qty} "
                  f"@ ref {ref_price:.2f}{ltp_str}")
        return out

    def close_orders(self, orders: List[PaperOrder], fill_price: float, ts: datetime) -> None:
        for o in orders:
            if o.exit_time is not None:
                continue
            o.exit_price = fill_price
            o.exit_time  = ts
            o.exit_ltp   = self._fetch_ltp(o.symbol)  # fetch actual live LTP at exit
            side_txt     = "BUY" if o.side == 1 else "SELL"
            # Use actual LTPs when available, fall back to spot proxy
            if o.entry_ltp is not None and o.exit_ltp is not None:
                leg_pnl   = (o.exit_ltp - o.entry_ltp) * o.qty * o.side
                price_src = f"entry_ltp={o.entry_ltp:.2f} exit_ltp={o.exit_ltp:.2f} [ACTUAL]"
            else:
                leg_pnl   = (fill_price - o.entry_price) * o.qty * o.side
                price_src = f"entry_ref={o.entry_price:.2f} exit_ref={fill_price:.2f} [SPOT PROXY]"
            print(f"[PAPER CLOSE] {o.paper_id} {side_txt} {o.symbol} qty={o.qty} "
                  f"{price_src} P&L=\u20b9{leg_pnl:+,.2f} ({ts.strftime('%H:%M:%S')})")

    @staticmethod
    def calc_trade_pnl(orders: List[PaperOrder]) -> float:
        """Sum P&L across all legs. Uses actual entry_ltp/exit_ltp when available, spot proxy otherwise."""
        total = 0.0
        for o in orders:
            if o.entry_ltp is not None and o.exit_ltp is not None:
                total += (o.exit_ltp - o.entry_ltp) * o.qty * o.side
            elif o.exit_price is not None:
                total += (o.exit_price - o.entry_price) * o.qty * o.side
        return total


class ExecutionRouter:
    def __init__(self, broker: FyersBroker, cfg: StrategyConfig, master: InstrumentMaster, order_tracker: Optional[OrderSocketTracker]) -> None:
        self.broker        = broker
        self.cfg           = cfg
        self.master        = master
        self.order_tracker = order_tracker
        self.paper_ledger  = PaperLedger(broker)

    def resolve_execution_legs(self, stock: SelectedStock, direction: str, entry_price: float) -> List[ResolvedLeg]:
        try:
            return self.master.resolve(stock.symbol, entry_price, direction, self.cfg.qty_option_lots, self.cfg.qty_future_lots, broker=self.broker)
        except Exception as e:
            print(f"[RESOLVER WARN] {e}")
            print("[RESOLVER WARN] Falling back to stock-equity proxy leg (no FO contracts — possibly expiry day or symbol mismatch).")
            proxy_qty = max(1, self.cfg.qty_option_lots)
            return [ResolvedLeg(name="stock_equity", symbol=stock.fyers_symbol, side=(1 if direction == "BULLISH" else -1), qty=proxy_qty)]

    def enter(self, stock: SelectedStock, direction: str, entry_price: float, stop_loss: float) -> TradeState:
        sl_dist = abs(entry_price - stop_loss)
        if sl_dist <= 0:
            raise RuntimeError("Invalid SL distance")
        tp   = (entry_price + (sl_dist * self.cfg.risk_reward_ratio)) if direction == "BULLISH" else (entry_price - (sl_dist * self.cfg.risk_reward_ratio))
        legs = self.resolve_execution_legs(stock, direction, entry_price)

        print(f"[ENTRY] {direction} {stock.symbol} @ {entry_price:.2f} | SL={stop_loss:.2f} TP={tp:.2f}")
        print("[LEGS]", [f"{'BUY' if l.side == 1 else 'SELL'} {l.symbol} x{l.qty}" for l in legs])

        order_ids:    List[str]        = []
        paper_orders: List[PaperOrder] = []
        if self.cfg.paper_trading:
            paper_orders = self.paper_ledger.open_orders(legs, entry_price)
        else:
            for leg in legs:
                resp = self.broker.place_market_order(leg.symbol, side=leg.side, qty=leg.qty, product_type=leg.product_type)
                print("[LIVE ORDER]", resp)
                oid = resp.get("id") or resp.get("orderId") or resp.get("s")
                if oid:
                    order_ids.append(str(oid))

        return TradeState(
            symbol=stock.fyers_symbol,
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=tp,
            entry_time=now_ist(),
            live_order_ids=order_ids,
            legs=legs,
            paper_orders=paper_orders,
        )

    def on_exit(self, trade: TradeState, exit_price: float, ts: datetime) -> None:
        if self.cfg.paper_trading:
            self.paper_ledger.close_orders(trade.paper_orders, exit_price, ts)
        else:
            # ── Live mode: place reverse market orders for every entry leg ──
            # Each leg is closed with the opposite side to the original entry.
            # We attempt all legs even if one fails — partial close is better
            # than leaving all positions open.
            reason = trade.exit_reason or "EXIT"
            print(f"[LIVE EXIT] Closing {len(trade.legs)} leg(s) for {trade.symbol} | reason={reason}")
            failed = []
            # Close future leg first, then option — reverse of entry order
            for leg in reversed(trade.legs):
                success = self.broker.close_live_position(leg, reason)
                if not success:
                    failed.append(leg.symbol)
                time.sleep(0.2)   # small pause between legs to avoid rate limit
            if failed:
                print(f"[LIVE EXIT WARN] Failed to close: {failed} — check positions manually in Fyers!")
            else:
                print(f"[LIVE EXIT] All legs closed successfully for {trade.symbol}")


# ============================================================================
# ENTRY + POSITION MANAGEMENT
# ============================================================================
class SetupTracker:
    """
    Entry logic with optional live-price breakout confirmation.

    use_breakout_confirmation=True (default / new behaviour):
    ──────────────────────────────────────────────────────────
    Phase 1  1st 5m candle closes below PDL (bearish) / above PDH (bullish)
             → recorded, wait for 2nd candle.

    Phase 2  2nd consecutive candle closes below PDL / above PDH
             AND candle range <= max_range_pct
             → setup LOCKED FOREVER (never cancels after this point).
               Locked values:
                 breakout_level  = min(c1.low,  c2.low)  - buffer   (bearish)
                               or  max(c1.high, c2.high) + buffer   (bullish)
                 locked_sl       = c2.high  (bearish) / c2.low  (bullish)

    Phase 3  Live price monitoring via check_live_price().
             The moment last_price <= breakout_level (bearish)
                               or >= breakout_level (bullish)
             → enter immediately at that live price, SL = locked_sl.
             No candle close needed. No cancellation possible.

    use_breakout_confirmation=False (legacy):
    ─────────────────────────────────────────
    Entry fires at the close of the 2nd confirmed candle, immediately.
    SL = c2.high (bearish) / c2.low (bullish).
    """

    def __init__(self, direction: str, pdh: float, pdl: float,
                 max_range_pct: float,
                 use_breakout_confirmation: bool = True,
                 breakout_buffer_pct: float = 0.1,
                 sl_buffer_pct: float = 0.1) -> None:
        self.direction                = direction
        self.pdh                      = pdh
        self.pdl                      = pdl
        self.max_range_pct            = max_range_pct
        self.use_breakout_confirmation = use_breakout_confirmation
        self.breakout_buffer_pct      = breakout_buffer_pct
        self.sl_buffer_pct            = sl_buffer_pct
        # phase state
        self.first: Optional[Candle]       = None
        self.setup_confirmed: bool         = False  # True once phase-2 done — never resets
        self.breakout_level: Optional[float] = None  # live-price trigger
        self.locked_sl: Optional[float]      = None  # SL locked at phase-2, never changes
        self.needs_reset: bool             = False  # True after range rejection — price must return inside PDH/PDL before retrying

    def _bar_valid(self, bar: Candle) -> bool:
        """Phase 1/2: candle close beyond PDH/PDL."""
        return bar.close > self.pdh if self.direction == "BULLISH" else bar.close < self.pdl

    def _range_valid(self, bar: Candle) -> bool:
        if bar.close == 0:
            return False
        return ((bar.high - bar.low) / bar.close) * 100.0 <= self.max_range_pct

    def on_new_5m_bar(self, bar: Candle) -> Optional[Tuple[float, float]]:
        """
        Called on every new closed 5m candle.
        Returns (entry_price, sl) only in legacy mode (use_breakout_confirmation=False).
        In breakout mode, returns None always — entry is triggered by check_live_price().
        """
        ref      = self.pdh if self.direction == "BULLISH" else self.pdl
        relation = "above" if self.direction == "BULLISH" else "below"

        # Once setup is confirmed in breakout mode, candle closes are irrelevant for entry
        if self.setup_confirmed:
            return None

        print(f"[CHECK] {bar.ts.strftime('%H:%M')} close={bar.close:.2f} is {relation} ref {ref:.2f}? {'YES' if self._bar_valid(bar) else 'NO'}")

        if not self._bar_valid(bar):
            # Candle closed back inside PDH/PDL boundary
            self.first = None
            if self.needs_reset:
                self.needs_reset = False
                print(f"[SETUP] Cooling-off cleared — price returned inside {ref:.2f}. Ready for fresh setup.")
            return None

        # Candle is valid (beyond PDH/PDL) but we are in cooling-off state
        if self.needs_reset:
            # Price hasn't come back inside yet — ignore this candle entirely
            return None

        if self.first is None:
            # Phase 1 — first valid candle
            self.first = bar
            print(f"[SETUP] First valid 5m candle at {bar.ts.strftime('%H:%M')} close={bar.close:.2f}")
            return None

        # Phase 2 — second consecutive candle
        if not self._range_valid(bar):
            print("[SETUP] Second candle rejected. Range too large. Needs cooling-off — price must return inside PDH/PDL before retrying.")
            self.first = None
            self.needs_reset = True
            return None

        # ── CONFIRMATION ─────────────────────────────────────────────────
        if self.use_breakout_confirmation:
            # Entry buffer in points = % of 2nd candle close (spot proxy)
            buffer_points = bar.close * self.breakout_buffer_pct / 100.0
            # Lock breakout level and SL — these never change after this point
            if self.direction == "BULLISH":
                raw_level           = max(self.first.high, bar.high)
                self.breakout_level = raw_level + buffer_points
                # SL = 2nd candle low minus sl_buffer% of that low
                sl_buffer           = bar.low * self.sl_buffer_pct / 100.0
                self.locked_sl      = bar.low - sl_buffer
            else:
                raw_level           = min(self.first.low, bar.low)
                self.breakout_level = raw_level - buffer_points
                # SL = 2nd candle high plus sl_buffer% of that high
                sl_buffer           = bar.high * self.sl_buffer_pct / 100.0
                self.locked_sl      = bar.high + sl_buffer

            self.setup_confirmed = True
            self.first = None
            print(f"[SETUP] ✅ Setup confirmed at {bar.ts.strftime('%H:%M')} | "
                  f"breakout_level={self.breakout_level:.2f} "
                  f"(raw={raw_level:.2f} entry_buf={buffer_points:.2f}pts={self.breakout_buffer_pct}% of {bar.close:.2f}) | "
                  f"locked_sl={self.locked_sl:.2f} "
                  f"(sl_buf={sl_buffer:.2f}pts={self.sl_buffer_pct}% of {bar.high if self.direction == 'BEARISH' else bar.low:.2f}) | "
                  f"Monitoring live price now...")
            return None

        else:
            # Legacy: enter immediately at 2nd candle close
            entry = bar.close
            sl    = bar.low if self.direction == "BULLISH" else bar.high
            print(f"[SETUP] Confirmed by 2nd candle at {bar.ts.strftime('%H:%M')}")
            self.first = None
            return entry, sl

    def check_live_price(self, price: float) -> Optional[Tuple[float, float]]:
        """
        Phase 3: called on every live price tick for stocks in confirmed-setup state.
        Returns (entry_price, locked_sl) the instant price breaches breakout_level.
        Never cancels — once setup_confirmed=True this keeps firing until entry or day end.
        """
        if not self.setup_confirmed or self.breakout_level is None or self.locked_sl is None:
            return None
        if self.direction == "BULLISH":
            if price >= self.breakout_level:
                print(f"[BREAKOUT] ✅ BULLISH breakout triggered! "
                      f"live_price={price:.2f} >= breakout_level={self.breakout_level:.2f} | "
                      f"SL={self.locked_sl:.2f}")
                self.setup_confirmed = False  # prevent re-trigger
                return price, self.locked_sl
        else:
            if price <= self.breakout_level:
                print(f"[BREAKOUT] ✅ BEARISH breakout triggered! "
                      f"live_price={price:.2f} <= breakout_level={self.breakout_level:.2f} | "
                      f"SL={self.locked_sl:.2f}")
                self.setup_confirmed = False  # prevent re-trigger
                return price, self.locked_sl
        return None


class PositionManager:
    def __init__(self, cfg: StrategyConfig) -> None:
        self.cfg = cfg

    def update(self, trade: TradeState, last_price: float, current_dt: datetime) -> Optional[str]:
        if trade.exit_reason:
            return trade.exit_reason

        if trade.direction == "BULLISH":
            if last_price <= trade.stop_loss:
                return self._exit(trade, trade.stop_loss, current_dt, "SL")
            if last_price >= trade.take_profit:
                return self._exit(trade, trade.take_profit, current_dt, "TP")
            move_pct = pct_change(last_price, trade.entry_price)
        else:
            if last_price >= trade.stop_loss:
                return self._exit(trade, trade.stop_loss, current_dt, "SL")
            if last_price <= trade.take_profit:
                return self._exit(trade, trade.take_profit, current_dt, "TP")
            move_pct = pct_change(trade.entry_price, last_price)

        # ── Multi-step trailing SL ────────────────────────────────────────────
        # step_size = entry × trailing_trigger_pct %
        # Every time price crosses another full step, SL moves up by one step.
        # Example (0.5% steps, entry=1348.20, step=6.74):
        #   Price >= 1354.94 (+0.5%)  → SL = 1348.20 (breakeven)
        #   Price >= 1361.68 (+1.0%)  → SL = 1354.94 (+0.5% from entry)
        #   Price >= 1368.42 (+1.5%)  → SL = 1361.68 (+1.0% from entry)
        step_size = trade.entry_price * self.cfg.trailing_trigger_pct / 100.0
        if step_size > 0:
            if trade.direction == "BULLISH":
                steps_crossed = int(move_pct / self.cfg.trailing_trigger_pct)
            else:
                steps_crossed = int(move_pct / self.cfg.trailing_trigger_pct)

            if steps_crossed > trade.trail_step_count:
                # Advance SL by however many new steps price has crossed
                for step in range(trade.trail_step_count + 1, steps_crossed + 1):
                    if trade.direction == "BULLISH":
                        new_sl = trade.entry_price + (step - 1) * step_size
                    else:
                        new_sl = trade.entry_price - (step - 1) * step_size

                    # SL can only move in the favourable direction
                    if trade.direction == "BULLISH":
                        if new_sl > trade.stop_loss:
                            trade.stop_loss = round(new_sl, 2)
                            label = "breakeven" if step == 1 else f"+{(step-1)*self.cfg.trailing_trigger_pct:.1f}% from entry"
                            print(f"[TRAIL] {trade.symbol} Step {step}: SL → {trade.stop_loss:.2f} ({label})")
                    else:
                        if new_sl < trade.stop_loss:
                            trade.stop_loss = round(new_sl, 2)
                            label = "breakeven" if step == 1 else f"-{(step-1)*self.cfg.trailing_trigger_pct:.1f}% from entry"
                            print(f"[TRAIL] {trade.symbol} Step {step}: SL → {trade.stop_loss:.2f} ({label})")

                trade.trail_step_count = steps_crossed
                trade.trailing_armed   = True

        hh, mm  = self.cfg.force_exit_time
        cutoff  = current_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if current_dt >= cutoff:
            return self._exit(trade, last_price, current_dt, "TIME")
        return None

    @staticmethod
    def _exit(trade: TradeState, px: float, current_dt: datetime, reason: str) -> str:
        trade.exit_price  = px
        trade.exit_time   = current_dt
        trade.exit_reason = reason
        print(f"[EXIT] {reason} @ {px:.2f} ({current_dt.strftime('%H:%M:%S')})")
        return reason


# ============================================================================
# ORCHESTRATOR
# ============================================================================
class SectorMomentumFyersStrategy:
    def __init__(self, cfg: Optional[StrategyConfig] = None) -> None:
        self.cfg           = cfg or StrategyConfig()
        self.broker        = FyersBroker()
        self.master        = InstrumentMaster(self.cfg.symbol_master_csv_path, self.cfg.symbol_master_refresh_days)
        self.order_tracker = None if self.cfg.paper_trading or not self.cfg.enable_order_socket_in_live_mode else OrderSocketTracker(self.broker.access_token)
        if self.cfg.paper_trading:
            print("[MODE] Paper mode enabled. No real broker orders will be placed.")
        self.phases       = StrategyPhases(self.broker, self.cfg, self.master)
        self.executor     = ExecutionRouter(self.broker, self.cfg, self.master, self.order_tracker)
        self.position_mgr = PositionManager(self.cfg)
        self.trend:              Optional[str]              = None
        self.selected_sector:    Optional[str]              = None
        self.stock_runtimes:     List[StockRuntime]         = []
        self.traded_stock_symbols: set[str]                 = set()
        self.shared_socket:      Optional[SharedMarketDataSocket] = None
        self.day_pnl:            float                      = 0.0  # running INR P&L for the session

    def _market_phase(self, dt: datetime) -> str:
        hm = (dt.hour, dt.minute)
        if hm < (9, 15):
            return "pre_open"
        if hm < (9, 25):
            return "waiting_first_candle"
        if hm >= (15, 30):
            return "closed"
        return "active"

    def wait_for_trading_window(self) -> bool:
        while True:
            phase = self._market_phase(now_ist())
            if phase == "pre_open":
                print("[WAIT] Waiting for 09:15 IST. Market has not opened yet.")
                time.sleep(15)
                continue
            if phase == "waiting_first_candle":
                print("[WAIT] Market is open. Waiting for 09:25 IST to fetch the first 10-minute candle.")
                time.sleep(15)
                continue
            if phase == "closed":
                print("[CLOSED] Market is closed for the day.")
                return False
            return True

    def bootstrap_selection(self) -> None:
        today      = now_ist()
        self.trend = self.phases.identify_day_trend(today)
        ranked     = self.phases.rank_sectors(self.trend)
        if not ranked:
            raise RuntimeError("No sectors could be ranked")

        # Try sectors in ranked order — skip immediately to next if no eligible stocks
        stocks = None
        for sector_name, sector_chg in ranked:
            try:
                stocks = self.phases.select_stocks(sector_name, self.trend)
                self.selected_sector = sector_name
                break
            except RuntimeError as e:
                print(f"[SECTOR SKIP] {sector_name} skipped: {e}. Trying next sector...")
                continue

        if not stocks:
            raise RuntimeError("No eligible stocks found in any ranked sector")
        self.stock_runtimes  = []
        if self.order_tracker is not None:
            self.order_tracker.start()
        for stock in stocks:
            try:
                pdh, pdl       = self.phases.fetch_pdh_pdl(stock.fyers_symbol, today)
                setup_tracker  = SetupTracker(
                                     direction=self.trend or "BULLISH",
                                     pdh=pdh, pdl=pdl,
                                     max_range_pct=self.cfg.second_candle_max_range_pct,
                                     use_breakout_confirmation=self.cfg.use_breakout_confirmation,
                                     breakout_buffer_pct=self.cfg.breakout_buffer_pct,
                                     sl_buffer_pct=self.cfg.sl_buffer_pct)
                feed           = LiveSymbolFeed(self.broker, self.broker.access_token, stock.fyers_symbol, max_history_rows=self.cfg.max_history_rows)
                feed.start()
                self.stock_runtimes.append(StockRuntime(stock=stock, pdh=pdh, pdl=pdl, setup_tracker=setup_tracker, feed=feed))
                print(f"[BOOTSTRAP] Prepared {stock.fyers_symbol} for 5m breakout setup...")
            except Exception as e:
                print(f"[BOOTSTRAP WARN] Skipping {stock.fyers_symbol}: {e}")
            time.sleep(0.3)  # small delay to avoid Fyers API rate limiting on sequential history() calls
        if not self.stock_runtimes:
            raise RuntimeError("No stocks could be initialized for monitoring")
        self.shared_socket = SharedMarketDataSocket(self.broker.access_token, [rt.feed for rt in self.stock_runtimes])
        self.shared_socket.start()
        print(f"[BOOTSTRAP] Live socket started for {len(self.stock_runtimes)} symbols.")

    def _entry_allowed_now(self) -> bool:
        hh, mm  = self.cfg.entry_cutoff_time
        current = now_ist()
        cutoff  = current.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return current < cutoff

    def on_new_5m_bar(self, runtime: StockRuntime, bar: Candle) -> None:
        """Handles phase 1 and 2 of SetupTracker (candle-close based).
        Phase 3 (live breakout) is handled separately in the main loop via check_live_price."""
        if runtime.trade or runtime.has_traded or runtime.is_complete:
            return
        if len(self.traded_stock_symbols) >= self.cfg.max_trades_per_day:
            return
        if not self._entry_allowed_now():
            print(f"[ENTRY BLOCKED] Entry cutoff reached for {runtime.stock.symbol}.")
            runtime.is_complete = True
            return
        setup = runtime.setup_tracker.on_new_5m_bar(bar)
        # Legacy mode only: setup returns (entry, sl) immediately on 2nd candle
        if setup is None:
            return
        entry, sl      = setup
        runtime.trade  = self.executor.enter(runtime.stock, self.trend or "BULLISH", entry, sl)
        runtime.has_traded = True
        self.traded_stock_symbols.add(runtime.stock.symbol)
        print(f"[DAY COUNT] Traded stocks today: {len(self.traded_stock_symbols)}/{self.cfg.max_trades_per_day}")

    def _replay_historical_5m_bars(self) -> None:
        """
        After bootstrap, replay ALL already-closed 5m candles for every runtime
        in chronological order through on_new_5m_bar().

        Why this is needed:
        The strategy starts after 09:25 IST (to wait for the first 10-min NIFTY
        candle). By the time bootstrap runs, the 09:15 and 09:20 stock candles
        are already built in feed.closed_5m. The main live loop only ever looks
        at closed_5m[-1] (the latest candle), so without this replay the 09:15
        candle — which is candle-1 of the two-consecutive setup — is permanently
        skipped. This causes the code to treat 09:20 as c1 and 09:25 as c2,
        producing a wrong breakout level and missing the earliest valid setup.

        After replay, last_5m_processed is set to the last replayed candle's ts,
        so the main live loop will not re-process any of them.
        """
        print("[REPLAY] Replaying historical 5m candles for all stocks...")
        for runtime in self.stock_runtimes:
            if not runtime.feed.closed_5m:
                continue
            for bar in runtime.feed.closed_5m:
                if runtime.is_complete or runtime.has_traded:
                    break
                print(f"[5M CLOSED] {runtime.stock.symbol} {bar.ts.strftime('%H:%M')} "
                      f"O={bar.open:.2f} H={bar.high:.2f} L={bar.low:.2f} C={bar.close:.2f}")
                self.on_new_5m_bar(runtime, bar)
                runtime.last_5m_processed = bar.ts
        print("[REPLAY] Historical replay complete.")

    def loop(self) -> None:
        if not self.wait_for_trading_window():
            return

        while True:
            try:
                try:
                    self.bootstrap_selection()
                    break
                except RuntimeError as e:
                    if "first 10-minute NIFTY candle" in str(e):
                        if self._market_phase(now_ist()) == "closed":
                            print("[CLOSED] Market is closed for the day.")
                            return
                        print("[WAIT] 09:25 IST reached. Waiting for the first 10-minute NIFTY candle to become available...")
                        time.sleep(10)
                        continue
                    raise
            except KeyboardInterrupt:
                print("\n[STOPPED] User interrupted.")
                return
            except Exception as e:
                print("[ERROR]", e)
                time.sleep(2.0)

        # Replay all candles already built during bootstrap (e.g. 09:15, 09:20)
        # so that the two-consecutive-candle setup check starts from the very
        # first candle of the day, not from whichever candle happens to be [-1].
        self._replay_historical_5m_bars()

        while True:
            try:
                active_trade_count = 0
                any_live_runtime   = False
                for runtime in self.stock_runtimes:
                    if runtime.is_complete:
                        continue
                    any_live_runtime = True
                    runtime.feed.sync_to_now()
                    if runtime.feed.closed_5m:
                        latest_5m = runtime.feed.closed_5m[-1]
                        if runtime.last_5m_processed != latest_5m.ts:
                            runtime.last_5m_processed = latest_5m.ts
                            print(f"[5M CLOSED] {runtime.stock.symbol} {latest_5m.ts.strftime('%H:%M')} O={latest_5m.open:.2f} H={latest_5m.high:.2f} L={latest_5m.low:.2f} C={latest_5m.close:.2f}")
                            self.on_new_5m_bar(runtime, latest_5m)
                    # ── Live breakout check for confirmed setups (phase 3) ──
                    if (not runtime.trade and not runtime.has_traded
                            and not runtime.is_complete
                            and runtime.setup_tracker.setup_confirmed
                            and runtime.feed.last_price is not None
                            and len(self.traded_stock_symbols) < self.cfg.max_trades_per_day
                            and self._entry_allowed_now()):
                        result = runtime.setup_tracker.check_live_price(runtime.feed.last_price)
                        if result is not None:
                            entry, sl = result
                            runtime.trade      = self.executor.enter(runtime.stock, self.trend or "BULLISH", entry, sl)
                            runtime.has_traded = True
                            self.traded_stock_symbols.add(runtime.stock.symbol)
                            print(f"[DAY COUNT] Traded stocks today: {len(self.traded_stock_symbols)}/{self.cfg.max_trades_per_day}")
                            # Subscribe option/future legs to WS for live P&L
                            if self.shared_socket and runtime.trade:
                                leg_syms = [o.symbol for o in runtime.trade.paper_orders]
                                if leg_syms:
                                    self.shared_socket.subscribe_additional(leg_syms)

                    if runtime.trade and runtime.feed.last_price is not None:
                        active_trade_count += 1
                        self.position_mgr.update(runtime.trade, runtime.feed.last_price, now_ist())
                        # ── Live unrealized P&L from WS tick cache ──────────
                        if self.shared_socket and runtime.trade.paper_orders:
                            live_pnl = 0.0
                            all_priced = True
                            for o in runtime.trade.paper_orders:
                                ltp = self.shared_socket.ltp_cache.get(o.symbol)
                                if ltp is not None and o.entry_ltp is not None:
                                    live_pnl += (ltp - o.entry_ltp) * o.qty * o.side
                                else:
                                    all_priced = False
                            if all_priced:
                                print(f"[LIVE P&L] {runtime.stock.symbol} unrealized = ₹{live_pnl:+,.2f}")
                        if runtime.trade.exit_reason:
                            self.executor.on_exit(runtime.trade, runtime.trade.exit_price or runtime.feed.last_price, runtime.trade.exit_time or now_ist())
                            # ── P&L summary ──
                            trade_pnl   = PaperLedger.calc_trade_pnl(runtime.trade.paper_orders)
                            self.day_pnl += trade_pnl
                            pnl_symbol  = "✅" if trade_pnl >= 0 else "❌"
                            print(f"[P&L] {pnl_symbol} {runtime.stock.symbol} trade P&L = "
                                  f"\u20b9{trade_pnl:+,.2f}  |  Day P&L = \u20b9{self.day_pnl:+,.2f} "
                                  f"[exit={runtime.trade.exit_reason}]")
                            print(f"[DONE] Trade cycle completed for {runtime.stock.symbol}.")
                            runtime.trade       = None
                            runtime.is_complete = True
                if len(self.traded_stock_symbols) >= self.cfg.max_trades_per_day and active_trade_count == 0:
                    print("[DONE] Two stocks have traded. Day limit reached.")
                    print(f"[DAY SUMMARY] Total Day P&L = \u20b9{self.day_pnl:+,.2f}")
                    break
                if not any_live_runtime:
                    print("[DONE] No active stocks left to monitor.")
                    print(f"[DAY SUMMARY] Total Day P&L = \u20b9{self.day_pnl:+,.2f}")
                    break
                time.sleep(self.cfg.poll_seconds_when_idle)
            except KeyboardInterrupt:
                print("\n[STOPPED] User interrupted.")
                # ── Close any open live positions on manual stop ──
                if not self.cfg.paper_trading:
                    for runtime in self.stock_runtimes:
                        if runtime.trade and not runtime.trade.exit_reason:
                            print(f"[LIVE EXIT] Emergency close for {runtime.stock.symbol} on interrupt...")
                            self.executor.on_exit(runtime.trade, runtime.feed.last_price or 0.0, now_ist())
                print(f"[DAY SUMMARY] Total Day P&L = \u20b9{self.day_pnl:+,.2f}")
                break
            except Exception as e:
                print("[ERROR]", e)
                time.sleep(2.0)


if __name__ == "__main__":
    cfg = StrategyConfig(
        paper_trading=True,
        symbol_master_csv_path=None,  # optional: set path or keep NSE_FO.csv in same folder
    )
    engine = SectorMomentumFyersStrategy(cfg)
    engine.loop()
