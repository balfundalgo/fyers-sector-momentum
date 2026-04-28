"""
gui.py
Fyers Sector Momentum Strategy - GUI
Balfund Trading Private Limited

CustomTkinter-based GUI wrapper around strategy.py.
Runs strategy in a background thread, captures all stdout/stderr
into the log panel, and displays live P&L (updates every WS tick).
"""

from __future__ import annotations

import io
import json
import os
import queue
import re
import sys
import threading
import time
from datetime import datetime
from typing import Optional

import customtkinter as ctk

# ── Strategy imports (all merged via bundler) ──────────────────────────────────
from strategy import StrategyConfig, SectorMomentumFyersStrategy


# ============================================================================
# STDOUT REDIRECTOR — captures all print() output into a queue
# ============================================================================
class QueueWriter(io.TextIOBase):
    def __init__(self, q: queue.Queue) -> None:
        self._q = q

    def write(self, s: str) -> int:
        if s and s != "\n":
            self._q.put(s)
        return len(s)

    def flush(self) -> None:
        pass


# ============================================================================
# STRATEGY RUNNER THREAD
# ============================================================================
class StrategyRunner(threading.Thread):
    def __init__(self, cfg: StrategyConfig, log_queue: queue.Queue,
                 status_callback, pnl_callback) -> None:
        super().__init__(daemon=True)
        self.cfg             = cfg
        self.log_queue       = log_queue
        self.status_callback = status_callback
        self.pnl_callback    = pnl_callback
        self._stop_event     = threading.Event()
        self.engine: Optional[SectorMomentumFyersStrategy] = None

    def run(self) -> None:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = QueueWriter(self.log_queue)
        sys.stderr = QueueWriter(self.log_queue)

        try:
            self.status_callback("RUNNING")
            self.engine = SectorMomentumFyersStrategy(self.cfg)

            # Patch position manager to check stop event + push P&L updates
            original_update = self.engine.position_mgr.update

            def patched_update(trade, last_price, current_dt):
                result = original_update(trade, last_price, current_dt)
                self.pnl_callback(self.engine.day_pnl)
                if self._stop_event.is_set() and not trade.exit_reason:
                    from strategy import PositionManager
                    PositionManager._exit(trade, last_price, current_dt, "MANUAL_STOP")
                return result

            self.engine.position_mgr.update = patched_update
            self.engine.loop()
            self.pnl_callback(self.engine.day_pnl)

        except Exception as e:
            self.log_queue.put(f"[ERROR] Strategy crashed: {e}")
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            self.status_callback("STOPPED")

    def stop(self) -> None:
        self._stop_event.set()
        self.log_queue.put("[GUI] Stop requested. Finishing current cycle...")


# ============================================================================
# MAIN GUI
# ============================================================================
class StrategyGUI:
    # ── Colour palette ────────────────────────────────────────────────────────
    CLR_BG     = "#1a1a2e"
    CLR_PANEL  = "#16213e"
    CLR_CARD   = "#0f3460"
    CLR_GREEN  = "#00d26a"
    CLR_RED    = "#ff4757"
    CLR_TEXT   = "#eaeaea"
    CLR_MUTED  = "#8892a4"
    CLR_BORDER = "#2a2a4a"
    CREDS_FILE = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), "fyers_credentials.json")

    @staticmethod
    def _load_credentials() -> dict:
        """Load saved credentials from JSON file, return defaults if not found."""
        creds_path = StrategyGUI.CREDS_FILE
        if os.path.exists(creds_path):
            try:
                with open(creds_path, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        return {"fyers_id": "", "pin": "", "totp_key": "", "app_id": "", "secret_key": ""}

    @staticmethod
    def _save_credentials(fyers_id: str, pin: str, totp_key: str, app_id: str, secret_key: str) -> None:
        """Save credentials to JSON file."""
        with open(StrategyGUI.CREDS_FILE, "w") as f:
            json.dump({"fyers_id": fyers_id, "pin": pin, "totp_key": totp_key, "app_id": app_id, "secret_key": secret_key}, f)

    def __init__(self) -> None:
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.root = ctk.CTk()
        self.root.title("Fyers Sector Momentum Strategy  |  Balfund Trading Pvt. Ltd.")
        self.root.geometry("1100x750")
        self.root.minsize(900, 600)
        self.root.configure(fg_color=self.CLR_BG)

        self.log_queue:   queue.Queue = queue.Queue()
        self.runner:      Optional[StrategyRunner] = None
        self._status      = "STOPPED"
        self._day_pnl     = 0.0
        self._trade_count = 0
        self._live_pnl_active = False  # True while a trade is open
        self._last_5m_sym: Optional[str] = None  # last symbol from [5M CLOSED] line
        # active_trades: {stock_symbol -> dict with entry/sl/tp/direction/live_pnl}
        self._active_trades: dict = {}
        # Monitored stocks panel: symbol -> {pdh, pdl, breakout, locked_sl, entry, tp, status}
        # status: "Waiting" | "Phase1" | "Setup ✓" | "ENTERED" | "DONE ✓" | "DONE ✗"
        self._monitored_stocks: dict = {}
        self._stock_row_widgets: dict = {}  # sym -> dict of label widgets

        self._build_ui()
        self._poll_log()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── UI construction ───────────────────────────────────────────────────────
    def _build_ui(self) -> None:
        header = ctk.CTkFrame(self.root, fg_color=self.CLR_PANEL, height=60, corner_radius=0)
        header.pack(fill="x", side="top")
        header.pack_propagate(False)

        ctk.CTkLabel(
            header,
            text="  Fyers Sector Momentum Strategy",
            font=ctk.CTkFont(family="Segoe UI", size=18, weight="bold"),
            text_color=self.CLR_TEXT,
        ).pack(side="left", padx=20, pady=15)

        ctk.CTkLabel(
            header,
            text="Balfund Trading Pvt. Ltd.  ",
            font=ctk.CTkFont(family="Segoe UI", size=12),
            text_color=self.CLR_MUTED,
        ).pack(side="right", padx=10, pady=15)

        body = ctk.CTkFrame(self.root, fg_color=self.CLR_BG, corner_radius=0)
        body.pack(fill="both", expand=True)

        left = ctk.CTkFrame(body, fg_color=self.CLR_PANEL, width=320, corner_radius=0)
        left.pack(fill="y", side="left", padx=(10, 5), pady=10)
        left.pack_propagate(False)

        # Scrollable container inside left panel
        left_scroll = ctk.CTkScrollableFrame(
            left, fg_color=self.CLR_PANEL, corner_radius=0,
            scrollbar_button_color=self.CLR_BORDER,
            scrollbar_button_hover_color=self.CLR_MUTED,
        )
        left_scroll.pack(fill="both", expand=True)

        right = ctk.CTkFrame(body, fg_color=self.CLR_PANEL, corner_radius=10)
        right.pack(fill="both", expand=True, side="left", padx=(5, 10), pady=10)

        self._build_left_panel(left_scroll)
        self._build_right_panel(right)

    def _section(self, parent, title: str) -> ctk.CTkFrame:
        ctk.CTkLabel(
            parent, text=title,
            font=ctk.CTkFont(size=11, weight="bold"),
            text_color=self.CLR_MUTED,
        ).pack(anchor="w", padx=16, pady=(14, 2))
        frame = ctk.CTkFrame(parent, fg_color=self.CLR_CARD, corner_radius=8)
        frame.pack(fill="x", padx=10, pady=(0, 4))
        return frame

    def _row(self, parent, label: str, widget_fn):
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=4)
        ctk.CTkLabel(row, text=label, font=ctk.CTkFont(size=12),
                     text_color=self.CLR_TEXT, width=160, anchor="w").pack(side="left")
        w = widget_fn(row)
        w.pack(side="right")
        return w

    def _build_left_panel(self, parent: ctk.CTkFrame) -> None:
        ctk.CTkLabel(
            parent, text="Configuration",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=self.CLR_TEXT,
        ).pack(anchor="w", padx=16, pady=(14, 0))

        # Fyers Credentials
        fc = self._section(parent, "FYERS CREDENTIALS")
        saved = self._load_credentials()
        self.fyers_id_var = ctk.StringVar(value=saved.get("fyers_id", ""))
        self.pin_var = ctk.StringVar(value=saved.get("pin", ""))
        self.totp_key_var = ctk.StringVar(value=saved.get("totp_key", ""))
        self.app_id_var = ctk.StringVar(value=saved.get("app_id", ""))
        self.secret_key_var = ctk.StringVar(value=saved.get("secret_key", ""))
        self._row(fc, "Fyers ID", lambda p: ctk.CTkEntry(p, textvariable=self.fyers_id_var, width=150, font=ctk.CTkFont(size=12), placeholder_text="e.g. YN04712"))
        self._row(fc, "PIN", lambda p: ctk.CTkEntry(p, textvariable=self.pin_var, width=150, font=ctk.CTkFont(size=12), show="*", placeholder_text="4-digit PIN"))
        self._row(fc, "TOTP Secret", lambda p: ctk.CTkEntry(p, textvariable=self.totp_key_var, width=150, font=ctk.CTkFont(size=12), show="*", placeholder_text="TOTP secret key"))
        self._row(fc, "App ID", lambda p: ctk.CTkEntry(p, textvariable=self.app_id_var, width=150, font=ctk.CTkFont(size=12), placeholder_text="e.g. XXXXXX-200"))
        self._row(fc, "Secret Key", lambda p: ctk.CTkEntry(p, textvariable=self.secret_key_var, width=150, font=ctk.CTkFont(size=12), show="*", placeholder_text="app secret key"))
        save_row = ctk.CTkFrame(fc, fg_color="transparent")
        save_row.pack(fill="x", padx=10, pady=(2, 6))
        self.creds_status = ctk.CTkLabel(save_row, text="", font=ctk.CTkFont(size=10), text_color=self.CLR_GREEN)
        self.creds_status.pack(side="left", padx=(0, 5))
        ctk.CTkButton(
            save_row, text="Save", width=60, height=26,
            font=ctk.CTkFont(size=11), corner_radius=6,
            command=self._save_creds_clicked,
        ).pack(side="right")

        # Mode
        f = self._section(parent, "TRADING MODE")
        self.paper_var = ctk.StringVar(value="Paper")
        self._row(f, "Mode", lambda p: ctk.CTkSegmentedButton(
            p, values=["Paper", "Live"], variable=self.paper_var,
            font=ctk.CTkFont(size=12), width=130,
        ))

        # Entry settings
        f2 = self._section(parent, "ENTRY SETTINGS")
        self.rr_var         = ctk.StringVar(value="2.0")
        self.buf_pct_var    = ctk.StringVar(value="0.1")
        self.sl_buf_pct_var = ctk.StringVar(value="0.1")
        self.maxmov_var     = ctk.StringVar(value="3.0")
        self.range_var      = ctk.StringVar(value="1.0")
        self.trail_var      = ctk.StringVar(value="0.5")
        self.breakout_var   = ctk.StringVar(value="Yes")
        self.max_trades_var = ctk.StringVar(value="2")

        self._row(f2, "Risk:Reward",        lambda p: ctk.CTkEntry(p, textvariable=self.rr_var,         width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Breakout Buffer %",  lambda p: ctk.CTkEntry(p, textvariable=self.buf_pct_var,    width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "SL Buffer %",        lambda p: ctk.CTkEntry(p, textvariable=self.sl_buf_pct_var, width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Max Stock Move%",    lambda p: ctk.CTkEntry(p, textvariable=self.maxmov_var,     width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "2nd Candle Range%",  lambda p: ctk.CTkEntry(p, textvariable=self.range_var,      width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Trail Trigger%",     lambda p: ctk.CTkEntry(p, textvariable=self.trail_var,      width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Max Trades/Day",     lambda p: ctk.CTkEntry(p, textvariable=self.max_trades_var, width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Breakout Confirm",   lambda p: ctk.CTkSegmentedButton(
            p, values=["Yes", "No"], variable=self.breakout_var,
            font=ctk.CTkFont(size=12), width=90,
        ))

        # Lots
        f3 = self._section(parent, "LOTS")
        self.opt_lots_var = ctk.StringVar(value="1")
        self.fut_lots_var = ctk.StringVar(value="1")
        self._row(f3, "Option Lots", lambda p: ctk.CTkEntry(p, textvariable=self.opt_lots_var, width=90, font=ctk.CTkFont(size=12)))
        self._row(f3, "Future Lots", lambda p: ctk.CTkEntry(p, textvariable=self.fut_lots_var, width=90, font=ctk.CTkFont(size=12)))

        # Active Trades
        ctk.CTkLabel(
            parent, text="ACTIVE TRADES",
            font=ctk.CTkFont(size=11, weight="bold"),
            text_color=self.CLR_MUTED,
        ).pack(anchor="w", padx=16, pady=(14, 2))
        self.trades_frame = ctk.CTkFrame(parent, fg_color=self.CLR_CARD, corner_radius=8)
        self.trades_frame.pack(fill="x", padx=10, pady=(0, 4))
        self.no_trades_label = ctk.CTkLabel(
            self.trades_frame,
            text="No active trades",
            font=ctk.CTkFont(size=11),
            text_color=self.CLR_MUTED,
        )
        self.no_trades_label.pack(pady=8)
        # Dict to hold per-symbol card frames
        self._trade_cards: dict = {}

        # P&L display
        f4 = self._section(parent, "TODAY'S P&L")
        pnl_card = ctk.CTkFrame(f4, fg_color="transparent")
        pnl_card.pack(fill="x", padx=10, pady=8)

        self.pnl_label = ctk.CTkLabel(
            pnl_card,
            text="Rs. 0.00",
            font=ctk.CTkFont(family="Segoe UI", size=26, weight="bold"),
            text_color=self.CLR_TEXT,
        )
        self.pnl_label.pack()

        self.pnl_type_label = ctk.CTkLabel(
            pnl_card, text="",
            font=ctk.CTkFont(size=10), text_color=self.CLR_MUTED,
        )
        self.pnl_type_label.pack()

        self.trade_count_label = ctk.CTkLabel(
            pnl_card, text="Trades today: 0 / 2",
            font=ctk.CTkFont(size=11), text_color=self.CLR_MUTED,
        )
        self.trade_count_label.pack()

        # Status
        self.status_label = ctk.CTkLabel(
            parent, text="  STOPPED",
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=self.CLR_RED,
        )
        self.status_label.pack(pady=(10, 4))

        # Buttons
        btn_frame = ctk.CTkFrame(parent, fg_color="transparent")
        btn_frame.pack(fill="x", padx=10, pady=4)

        self.start_btn = ctk.CTkButton(
            btn_frame, text="START",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=self.CLR_GREEN, hover_color="#00b359",
            text_color="#000000", height=42, corner_radius=8,
            command=self._start,
        )
        self.start_btn.pack(fill="x", pady=(0, 6))

        self.stop_btn = ctk.CTkButton(
            btn_frame, text="STOP",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=self.CLR_RED, hover_color="#cc0000",
            text_color="#ffffff", height=42, corner_radius=8,
            state="disabled", command=self._stop,
        )
        self.stop_btn.pack(fill="x")

    def _build_right_panel(self, parent: ctk.CTkFrame) -> None:
        # ── Top: Monitored Stocks section ────────────────────────────────────
        stocks_hdr = ctk.CTkFrame(parent, fg_color="transparent")
        stocks_hdr.pack(fill="x", padx=12, pady=(10, 2))
        ctk.CTkLabel(
            stocks_hdr, text="Monitored Stocks",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=self.CLR_TEXT,
        ).pack(side="left")

        # Column header row
        col_hdr = ctk.CTkFrame(parent, fg_color="#0d1a2e", corner_radius=6)
        col_hdr.pack(fill="x", padx=10, pady=(0, 1))
        # Column widths must match _STOCK_COLS in _refresh_stock_rows
        for _key, title, width, anchor in self._STOCK_COLS:
            ctk.CTkLabel(col_hdr, text=title, width=width, anchor=anchor,
                         font=ctk.CTkFont(size=10, weight="bold"),
                         text_color=self.CLR_MUTED).pack(side="left", padx=2, pady=3)

        # Scrollable stock rows container
        self.stocks_frame = ctk.CTkFrame(parent, fg_color=self.CLR_CARD,
                                          corner_radius=6)
        self.stocks_frame.pack(fill="x", padx=10, pady=(0, 4))
        self.no_stocks_label = ctk.CTkLabel(
            self.stocks_frame, text="Waiting for market open...",
            font=ctk.CTkFont(size=11), text_color=self.CLR_MUTED)
        self.no_stocks_label.pack(pady=8)

        # ── Bottom: Live Log ─────────────────────────────────────────────────
        log_hdr = ctk.CTkFrame(parent, fg_color="transparent")
        log_hdr.pack(fill="x", padx=12, pady=(4, 2))
        ctk.CTkLabel(
            log_hdr, text="Live Log",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=self.CLR_TEXT,
        ).pack(side="left")
        ctk.CTkButton(
            log_hdr, text="Clear", width=60, height=26,
            font=ctk.CTkFont(size=11),
            fg_color=self.CLR_CARD, hover_color=self.CLR_BORDER,
            command=self._clear_log,
        ).pack(side="right")

        self.log_box = ctk.CTkTextbox(
            parent,
            font=ctk.CTkFont(family="Consolas", size=11),
            fg_color="#0d0d1a", text_color=self.CLR_TEXT,
            wrap="word", corner_radius=8, state="disabled",
        )
        self.log_box.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.log_box._textbox.tag_config("entry",    foreground="#00d26a")
        self.log_box._textbox.tag_config("exit",     foreground="#ff6b81")
        self.log_box._textbox.tag_config("pnl",      foreground="#ffd700")
        self.log_box._textbox.tag_config("livepnl",  foreground="#a0c4ff")
        self.log_box._textbox.tag_config("warn",     foreground="#ffa502")
        self.log_box._textbox.tag_config("error",    foreground="#ff4757")
        self.log_box._textbox.tag_config("setup",    foreground="#70a1ff")
        self.log_box._textbox.tag_config("breakout", foreground="#00d26a")
        self.log_box._textbox.tag_config("info",     foreground=self.CLR_TEXT)

    # ── Log handling ──────────────────────────────────────────────────────────
    def _tag_for(self, text: str) -> str:
        t = text.upper()
        if "[LIVE P&L]" in t:
            return "livepnl"
        if any(x in t for x in ("[ENTRY]", "[PAPER OPEN]", "[LIVE ORDER]")):
            return "entry"
        if any(x in t for x in ("[EXIT]", "[PAPER CLOSE]", "[DONE]")):
            return "exit"
        if any(x in t for x in ("[P&L]", "[DAY SUMMARY]")):
            return "pnl"
        if any(x in t for x in ("[WARN]", "[RESOLVER WARN]", "[BOOTSTRAP WARN]",
                                  "[LTP WARN]", "[CONSTITUENTS WARN]")):
            return "warn"
        if "[ERROR]" in t:
            return "error"
        if any(x in t for x in ("[SETUP]", "[CHECK]")):
            return "setup"
        if "[BREAKOUT]" in t:
            return "breakout"
        return "info"

    def _append_log(self, text: str) -> None:
        tag  = self._tag_for(text)
        ts   = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {text.rstrip()}\n"

        self.log_box.configure(state="normal")
        self.log_box._textbox.insert("end", line, tag)
        self.log_box._textbox.see("end")
        self.log_box.configure(state="disabled")

        # Parse trade count
        if "DAY COUNT" in text.upper():
            try:
                parts = text.split(":")[1].strip().split("/")
                count = int(parts[0])
                total = int(parts[1]) if len(parts) > 1 else int(self.max_trades_var.get())
                self._trade_count = count
                self.trade_count_label.configure(text=f"Trades today: {count} / {total}")
            except Exception:
                pass

        # ── Parse [STOCKS] → init monitored stock rows ──────────────────────
        # [STOCKS] Monitoring 4 eligible stocks in NIFTY AUTO: M&M(-1.92%), ...
        if "[STOCKS]" in text.upper() and "MONITORING" in text.upper():
            m = re.search(r"Monitoring \d+ eligible stocks in [^:]+:\s*(.+)", text, re.IGNORECASE)
            if m:
                for part in m.group(1).split(","):
                    sm = re.match(r"\s*([A-Z0-9&\-.]+)\(", part.strip(), re.IGNORECASE)
                    if sm:
                        sym = sm.group(1).strip()
                        if sym not in self._monitored_stocks:
                            self._monitored_stocks[sym] = {
                                "pdh": "--", "pdl": "--",
                                "breakout": "--", "locked_sl": "--",
                                "entry": "--", "tp": "--",
                                "status": "Waiting",
                            }
                self.root.after(0, self._refresh_stock_rows)

        # ── Parse [PDH/PDL] → fill PDH / PDL ────────────────────────────────
        # [PDH/PDL] NSE:M&M-EQ => PDH=3157.90 PDL=3082.90 (daily)
        if "[PDH/PDL]" in text.upper():
            m = re.search(r"NSE:([A-Z0-9&\-.]+)-EQ\s*=>\s*PDH=([\d.]+)\s+PDL=([\d.]+)",
                          text, re.IGNORECASE)
            if m:
                sym, pdh, pdl = m.group(1), m.group(2), m.group(3)
                if sym in self._monitored_stocks:
                    self._monitored_stocks[sym]["pdh"] = pdh
                    self._monitored_stocks[sym]["pdl"] = pdl
                    self.root.after(0, self._refresh_stock_rows)

        # ── Parse [SETUP] First valid → Phase 1 ─────────────────────────────
        # [SETUP] First valid 5m candle at 09:20 close=3068.80
        if "[SETUP] FIRST" in text.upper():
            for sym, info in self._monitored_stocks.items():
                if info["status"] == "Waiting":
                    info["status"] = "Phase 1 ✓"
                    self.root.after(0, self._refresh_stock_rows)
                    break

        # ── Parse [SETUP] ✅ confirmed → lock breakout + SL ──────────────────
        # [SETUP] ✅ Setup confirmed at 09:25 | breakout_level=3065.30 | locked_sl=3076.90
        if "[SETUP]" in text.upper() and "SETUP CONFIRMED" in text.upper():
            mb = re.search(r"breakout_level=([\d.]+)", text, re.IGNORECASE)
            ms = re.search(r"locked_sl=([\d.]+)", text, re.IGNORECASE)
            if mb and ms:
                bl, lsl = mb.group(1), ms.group(1)
                for sym, info in self._monitored_stocks.items():
                    if info["status"] in ("Waiting", "Phase 1 ✓"):
                        info["breakout"]  = bl
                        info["locked_sl"] = lsl
                        info["status"]    = "Setup ✓"
                        self.root.after(0, self._refresh_stock_rows)
                        break

        # ── Parse ENTRY line → add to active trades ─────────────────────────
        # [ENTRY] BEARISH M&M @ 3065.00 | SL=3076.90 TP=3041.20
        if "[ENTRY]" in text.upper():
            m = re.search(
                r"\[ENTRY\]\s+(BEARISH|BULLISH)\s+(\S+)\s+@\s+([\d.]+)\s*\|\s*SL=([\d.]+)\s+TP=([\d.]+)",
                text, re.IGNORECASE)
            if m:
                direction, sym, entry, sl, tp = m.group(1).upper(), m.group(2), m.group(3), m.group(4), m.group(5)
                self._active_trades[sym] = {
                    "direction": direction, "entry": entry,
                    "sl": sl, "tp": tp, "live_pnl": 0.0,
                }
                # Also update monitored stocks table
                if sym in self._monitored_stocks:
                    self._monitored_stocks[sym]["entry"]  = entry
                    self._monitored_stocks[sym]["tp"]     = tp
                    self._monitored_stocks[sym]["status"] = "ENTERED"
                self.root.after(0, self._refresh_trade_cards)
                self.root.after(0, self._refresh_stock_rows)

        # ── Parse LIVE P&L → update card ────────────────────────────────────
        if "[LIVE P&L]" in text.upper():
            m = re.search(r'\[LIVE P&L\]\s+(\S+)\s+unrealized\s*=\s*₹([+\-][\d,]+\.?\d*)', text)
            if m:
                sym = m.group(1)
                try:
                    val = float(m.group(2).replace(",", ""))
                    if sym in self._active_trades:
                        self._active_trades[sym]["live_pnl"] = val
                        self.root.after(0, self._refresh_trade_cards)
                except Exception:
                    pass
            # Also update main P&L display
            m2 = re.search(r'₹([+\-][\d,]+\.?\d*)', text)
            if m2:
                try:
                    val = float(m2.group(1).replace(",", ""))
                    color = self.CLR_GREEN if val >= 0 else self.CLR_RED
                    self.pnl_label.configure(text=f"Rs. {val:+,.2f}", text_color=color)
                    self.pnl_type_label.configure(text="unrealized (live)")
                    self._live_pnl_active = True
                except Exception:
                    pass

        # ── Parse DONE line → remove from active trades ──────────────────────
        if "[DONE] TRADE CYCLE COMPLETED FOR" in text.upper():
            m = re.search(r"completed for (\S+)", text, re.IGNORECASE)
            if m:
                sym = m.group(1).rstrip(".")
                self._active_trades.pop(sym, None)
                # Mark in monitored stocks
                if sym in self._monitored_stocks:
                    self._monitored_stocks[sym]["status"] = "Done"
                self.root.after(0, self._refresh_trade_cards)
                self.root.after(0, self._refresh_stock_rows)

        # ── Parse TRAIL → update SL on card to new stepped value ────────────
        # [TRAIL] NSE:AXISBANK-EQ Step 1: SL → 1348.20 (breakeven)
        # [TRAIL] NSE:AXISBANK-EQ Step 2: SL → 1354.94 (+0.5% from entry)
        if "[TRAIL]" in text.upper():
            m = re.search(r"\[TRAIL\]\s+(\S+)\s+Step\s+(\d+):\s+SL\s+[→>]\s+([\d.]+)", text, re.IGNORECASE)
            if m:
                raw_sym  = m.group(1)
                step_num = int(m.group(2))
                new_sl   = m.group(3)
                # Strip NSE: prefix and -EQ suffix to get base ticker
                base = re.sub(r"^NSE:", "", raw_sym, flags=re.IGNORECASE)
                base = re.sub(r"-EQ$", "", base, flags=re.IGNORECASE).upper()
                # Update only the matching active trade
                for sym in list(self._active_trades.keys()):
                    if sym.upper() == base or sym.upper() in raw_sym.upper():
                        self._active_trades[sym]["sl"]       = new_sl
                        self._active_trades[sym]["sl_label"] = f"SL (Trail S{step_num}):"
                        if sym in self._monitored_stocks:
                            self._monitored_stocks[sym]["sl"]    = new_sl
                            self._monitored_stocks[sym]["sl_be"] = True
                        break
                self.root.after(0, self._refresh_trade_cards)
                self.root.after(0, self._refresh_stock_rows)

        # ── Stock Monitor parsing ────────────────────────────────────────────

        # [STOCKS] Monitoring 4 eligible stocks in NIFTY AUTO: M&M(-1.92%), EICHERMOT(-1.62%), ...
        if "[STOCKS]" in text.upper() and "MONITORING" in text.upper():
            m = re.search(r"Monitoring \d+ eligible stocks in (.+?):\s*(.+)$", text, re.IGNORECASE)
            if m:
                # Reset previous stocks
                for w in self._stock_row_widgets.values():
                    if "_frame" in w:
                        w["_frame"].destroy()
                self._stock_row_widgets.clear()
                self._monitored_stocks.clear()
                # Parse each "SYM(pct%)" entry
                for item in re.finditer(r"([A-Z0-9&\-]+)\(([+\-]?[\d.]+)%\)", m.group(2)):
                    sym = item.group(1)
                    pct = f"({item.group(2)}%)"
                    self._monitored_stocks[sym] = {"status": "WATCHING", "pct": pct}
                self.root.after(0, self._refresh_stock_rows)

        # [PDH/PDL] NSE:M&M-EQ => PDH=3157.90 PDL=3082.90 (daily)
        if "[PDH/PDL]" in text.upper():
            m = re.search(r"NSE:(.+?)-EQ\s*=>\s*PDH=([\d.]+)\s+PDL=([\d.]+)", text, re.IGNORECASE)
            if m:
                sym = m.group(1).upper()
                if sym in self._monitored_stocks:
                    self._monitored_stocks[sym]["pdh"] = m.group(2)
                    self._monitored_stocks[sym]["pdl"] = m.group(3)
                    self.root.after(0, self._refresh_stock_rows)

        # [SETUP] First valid 5m candle at 09:20 close=3068.80
        # We don't know which stock this belongs to from this line alone —
        # but the stocks are processed sequentially so we track "last stock being set up"
        # via a simpler approach: use the last [5M CLOSED] symbol
        if "[SETUP] FIRST VALID" in text.upper():
            # Mark the stock most recently bootstrapped as PHASE1
            # We track this via _last_setup_sym which is set by [5M CLOSED] parsing
            sym = getattr(self, "_last_5m_sym", None)
            if sym and sym in self._monitored_stocks:
                self._monitored_stocks[sym]["status"] = "PHASE1"
                self.root.after(0, self._refresh_stock_rows)

        # [5M CLOSED] M&M 09:25 ... — track which symbol the next SETUP line belongs to
        if "[5M CLOSED]" in text.upper():
            m = re.search(r"\[5M CLOSED\]\s+(\S+)\s+", text, re.IGNORECASE)
            if m:
                self._last_5m_sym = m.group(1).upper()

        # [SETUP] ✅ Setup confirmed at 09:25 | breakout_level=3065.30 ... | locked_sl=3076.90
        if "[SETUP]" in text.upper() and "SETUP CONFIRMED" in text.upper():
            m_bl = re.search(r"breakout_level=([\d.]+)", text, re.IGNORECASE)
            m_sl = re.search(r"locked_sl=([\d.]+)", text, re.IGNORECASE)
            sym  = getattr(self, "_last_5m_sym", None)
            if sym and sym in self._monitored_stocks and m_bl and m_sl:
                self._monitored_stocks[sym]["status"]   = "SETUP"
                self._monitored_stocks[sym]["breakout"] = m_bl.group(1)
                self._monitored_stocks[sym]["sl"]       = m_sl.group(1)
                self.root.after(0, self._refresh_stock_rows)

        # [ENTRY] BEARISH M&M @ 3065.00 | SL=3076.90 TP=3041.20
        # (also handled in active trades section but we update stock monitor too)
        if "[ENTRY]" in text.upper():
            m = re.search(
                r"\[ENTRY\]\s+(BEARISH|BULLISH)\s+(\S+)\s+@\s+([\d.]+)\s*\|\s*SL=([\d.]+)\s+TP=([\d.]+)",
                text, re.IGNORECASE)
            if m:
                sym = m.group(2).upper()
                if sym in self._monitored_stocks:
                    self._monitored_stocks[sym]["status"] = "ENTERED"
                    self._monitored_stocks[sym]["entry"]  = m.group(3)
                    self._monitored_stocks[sym]["sl"]     = m.group(4)
                    self._monitored_stocks[sym]["tp"]     = m.group(5)
                    self.root.after(0, self._refresh_stock_rows)

        # [TRAIL] Armed — update SL to breakeven on stock monitor row too
        if "[TRAIL]" in text.upper():
            m = re.search(r"SL moved to breakeven ([\d.]+)", text, re.IGNORECASE)
            if m:
                for sym in self._monitored_stocks:
                    if self._monitored_stocks[sym].get("status") == "ENTERED":
                        self._monitored_stocks[sym]["sl"]     = m.group(1)
                        self._monitored_stocks[sym]["sl_be"]  = True
                self.root.after(0, self._refresh_stock_rows)

        # [EXIT] SL/TP/TIME — update status on stock monitor
        if "[EXIT]" in text.upper() and "[PAPER" not in text.upper():
            m = re.search(r"\[EXIT\]\s+(SL|TP|TIME)\s+@", text, re.IGNORECASE)
            if m:
                reason = m.group(1).upper()
                exit_status = {"SL": "SL HIT", "TP": "TP HIT", "TIME": "TIME EXIT"}.get(reason, "DONE")
                for sym in self._monitored_stocks:
                    if self._monitored_stocks[sym].get("status") == "ENTERED":
                        self._monitored_stocks[sym]["status"] = exit_status
                self.root.after(0, self._refresh_stock_rows)

        # [DONE] Trade cycle completed for M&M.
        if "[DONE] TRADE CYCLE COMPLETED FOR" in text.upper():
            m = re.search(r"completed for (\S+)", text, re.IGNORECASE)
            if m:
                sym = m.group(1).rstrip(".").upper()
                if sym in self._monitored_stocks:
                    # Keep final status (SL HIT / TP HIT / TIME EXIT), just ensure not "ENTERED"
                    if self._monitored_stocks[sym].get("status") == "ENTERED":
                        self._monitored_stocks[sym]["status"] = "DONE"
                    self.root.after(0, self._refresh_stock_rows)

        # ── Realized P&L at trade close ──────────────────────────────────────
        if "[P&L]" in text.upper() and "[LIVE P&L]" not in text.upper():
            m = re.search(r'Day P&L\s*=\s*₹([+\-][\d,]+\.?\d*)', text)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    color = self.CLR_GREEN if val >= 0 else self.CLR_RED
                    self.pnl_label.configure(
                        text=f"Rs. {val:+,.2f}", text_color=color)
                    self.pnl_type_label.configure(text="realized")
                    self._live_pnl_active = False
                    # Mark last completed stock as done with result
                    done_sym = re.search(r"\[P&L\]\s+[✅❌]\s+(\S+)", text)
                    if done_sym:
                        s = done_sym.group(1)
                        tick = "✅" if val >= 0 else "❌"
                        if s in self._monitored_stocks:
                            self._monitored_stocks[s]["status"] = f"Done {tick}"
                    self.root.after(0, self._refresh_stock_rows)
                except Exception:
                    pass

    def _clear_log(self) -> None:
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")

    def _poll_log(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                for line in str(msg).splitlines():
                    if line.strip():
                        self._append_log(line)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_log)

    # ── Monitored Stocks Table ───────────────────────────────────────────────
    # Status colour map
    _STATUS_COLORS = {
        "Waiting":   "#8892a4",   # muted grey
        "Phase 1 ✓": "#ffa502",   # orange — first candle confirmed
        "Setup ✓":   "#70a1ff",   # blue — setup locked, monitoring live
        "ENTERED":   "#00d26a",   # green — trade open
        "Done":      "#8892a4",   # muted
        "Done ✅":    "#00d26a",   # green profit
        "Done ❌":    "#ff4757",   # red loss
    }

    def _refresh_stock_rows(self) -> None:
        """Rebuild stock rows whenever _monitored_stocks changes."""
        if not self._monitored_stocks:
            self.no_stocks_label.pack(pady=8)
            return
        self.no_stocks_label.pack_forget()

        cols_def = [
            ("sym",      80,  "w"),
            ("pdh",      70,  "e"),
            ("pdl",      70,  "e"),
            ("breakout", 80,  "e"),
            ("locked_sl",70,  "e"),
            ("tp",       70,  "e"),
            ("entry",    70,  "e"),
            ("status",  110,  "center"),
        ]

        for sym, info in self._monitored_stocks.items():
            if sym not in self._stock_row_widgets:
                # Create the row frame
                row_frame = ctk.CTkFrame(self.stocks_frame, fg_color="transparent",
                                          corner_radius=0)
                row_frame.pack(fill="x", padx=4, pady=1)

                widgets = {}
                values = [
                    sym,
                    info.get("pdh",      "--"),
                    info.get("pdl",      "--"),
                    info.get("breakout", "--"),
                    info.get("locked_sl","--"),
                    info.get("tp",       "--"),
                    info.get("entry",    "--"),
                    info.get("status",   "Waiting"),
                ]

                for (key, w, anchor), val in zip(cols_def, values):
                    is_status = (key == "status")
                    color = self._STATUS_COLORS.get(info.get("status","Waiting"), self.CLR_MUTED)                             if is_status else self.CLR_TEXT
                    font_w = "bold" if is_status or key == "sym" else "normal"
                    lbl = ctk.CTkLabel(
                        row_frame, text=val, width=w, anchor=anchor,
                        font=ctk.CTkFont(size=10, weight=font_w),
                        text_color=color,
                    )
                    lbl.pack(side="left", padx=2)
                    widgets[key] = lbl

                self._stock_row_widgets[sym] = widgets

                # Separator
                sep = ctk.CTkFrame(self.stocks_frame, fg_color="#1e2a3a", height=1, corner_radius=0)
                sep.pack(fill="x", padx=4)

            else:
                # Update existing row widgets
                widgets = self._stock_row_widgets[sym]
                field_map = {
                    "pdh":       info.get("pdh",      "--"),
                    "pdl":       info.get("pdl",      "--"),
                    "breakout":  info.get("breakout", "--"),
                    "locked_sl": info.get("locked_sl","--"),
                    "tp":        info.get("tp",       "--"),
                    "entry":     info.get("entry",    "--"),
                    "status":    info.get("status",   "Waiting"),
                }
                for key, val in field_map.items():
                    if key in widgets:
                        is_status = (key == "status")
                        color = self._STATUS_COLORS.get(val, self.CLR_MUTED)                                 if is_status else self.CLR_TEXT
                        font_w = "bold" if is_status else "normal"
                        widgets[key].configure(
                            text=val, text_color=color,
                            font=ctk.CTkFont(size=10, weight=font_w))

    # ── Active Trade Cards ────────────────────────────────────────────────────
    def _refresh_trade_cards(self) -> None:
        """Rebuild the active trade cards from _active_trades dict."""
        # Remove cards for closed trades
        for sym in list(self._trade_cards.keys()):
            if sym not in self._active_trades:
                self._trade_cards[sym].destroy()
                del self._trade_cards[sym]

        # Show/hide placeholder
        if self._active_trades:
            self.no_trades_label.pack_forget()
        else:
            self.no_trades_label.pack(pady=8)
            return

        # Create or update cards
        for sym, info in self._active_trades.items():
            if sym not in self._trade_cards:
                card = ctk.CTkFrame(self.trades_frame, fg_color="#0a2a4a", corner_radius=6)
                card.pack(fill="x", padx=6, pady=4)
                self._trade_cards[sym] = card

                # Symbol + direction header
                header = ctk.CTkFrame(card, fg_color="transparent")
                header.pack(fill="x", padx=8, pady=(6, 2))
                dir_color = self.CLR_RED if info["direction"] == "BEARISH" else self.CLR_GREEN
                ctk.CTkLabel(
                    header, text=f"▶ {sym}",
                    font=ctk.CTkFont(size=12, weight="bold"),
                    text_color=self.CLR_TEXT,
                ).pack(side="left")
                ctk.CTkLabel(
                    header, text=info["direction"],
                    font=ctk.CTkFont(size=10, weight="bold"),
                    text_color=dir_color,
                ).pack(side="right")

                # Entry / SL / TP rows
                details = ctk.CTkFrame(card, fg_color="transparent")
                details.pack(fill="x", padx=8, pady=(0, 4))

                def _detail_row(parent, label, value, color):
                    row = ctk.CTkFrame(parent, fg_color="transparent")
                    row.pack(fill="x")
                    ctk.CTkLabel(row, text=label, font=ctk.CTkFont(size=10),
                                 text_color=self.CLR_MUTED, width=55, anchor="w").pack(side="left")
                    lbl = ctk.CTkLabel(row, text=value, font=ctk.CTkFont(size=10, weight="bold"),
                                       text_color=color)
                    lbl.pack(side="left")
                    return lbl

                _detail_row(details, "Entry:", f"₹{info['entry']}", self.CLR_TEXT)
                sl_label = info.get("sl_label", "SL:   ")
                sl_color = "#ffa502" if "BE" in sl_label else self.CLR_RED
                # Store SL key + value label refs for live updates
                sl_row = ctk.CTkFrame(details, fg_color="transparent")
                sl_row.pack(fill="x")
                card._sl_key_label = ctk.CTkLabel(sl_row, text=sl_label, font=ctk.CTkFont(size=10),
                    text_color=self.CLR_MUTED, width=55, anchor="w")
                card._sl_key_label.pack(side="left")
                card._sl_label = ctk.CTkLabel(sl_row, text=f"₹{info['sl']}",
                    font=ctk.CTkFont(size=10, weight="bold"), text_color=sl_color)
                card._sl_label.pack(side="left")
                _detail_row(details, "TP:   ", f"₹{info['tp']}", self.CLR_GREEN)

                # Live P&L label
                pnl_row = ctk.CTkFrame(card, fg_color="transparent")
                pnl_row.pack(fill="x", padx=8, pady=(0, 6))
                ctk.CTkLabel(pnl_row, text="Live P&L:", font=ctk.CTkFont(size=10),
                             text_color=self.CLR_MUTED, width=55, anchor="w").pack(side="left")
                pnl_lbl = ctk.CTkLabel(pnl_row, text="₹0.00",
                                       font=ctk.CTkFont(size=10, weight="bold"),
                                       text_color=self.CLR_TEXT)
                pnl_lbl.pack(side="left")
                # Store ref to update live pnl
                card._pnl_label = pnl_lbl
            else:
                # Update live P&L on existing card
                card = self._trade_cards[sym]
                if hasattr(card, "_pnl_label"):
                    pnl = info.get("live_pnl", 0.0)
                    color = self.CLR_GREEN if pnl >= 0 else self.CLR_RED
                    card._pnl_label.configure(
                        text=f"₹{pnl:+,.2f}", text_color=color)
                # Update SL label if trail armed
                if hasattr(card, "_sl_label"):
                    sl_lbl = info.get("sl_label", "SL:   ")
                    sl_color = "#ffa502" if "BE" in sl_lbl else self.CLR_RED
                    card._sl_label.configure(
                        text=f"₹{info['sl']}", text_color=sl_color)
                if hasattr(card, "_sl_key_label"):
                    sl_lbl = info.get("sl_label", "SL:   ")
                    card._sl_key_label.configure(text=sl_lbl)

    # ── Stock Monitor rows ───────────────────────────────────────────────────
    _STOCK_COLS = [
        ("symbol",   "Symbol",   80,  "w"),
        ("pdh",      "PDH",      72,  "e"),
        ("pdl",      "PDL",      72,  "e"),
        ("breakout", "Breakout", 82,  "e"),
        ("sl",       "SL",       72,  "e"),
        ("tp",       "TP",       72,  "e"),
        ("entry",    "Entry",    72,  "e"),
        ("status",   "Status",  108,  "center"),
    ]

    def _refresh_stock_rows(self) -> None:
        """Create or update one row per monitored stock in the stocks_frame."""
        if not self._monitored_stocks:
            self.no_stocks_label.pack(pady=8)
            return
        self.no_stocks_label.pack_forget()

        for sym, info in self._monitored_stocks.items():
            if sym not in self._stock_row_widgets:
                # ── Build a new row ──────────────────────────────────────────
                row_frame = ctk.CTkFrame(self.stocks_frame,
                                         fg_color="transparent", corner_radius=0)
                row_frame.pack(fill="x", padx=4, pady=1)

                widgets = {}
                for key, _title, width, anchor in self._STOCK_COLS:
                    lbl = ctk.CTkLabel(
                        row_frame, text="—", width=width, anchor=anchor,
                        font=ctk.CTkFont(size=10),
                        text_color=self.CLR_TEXT,
                    )
                    lbl.pack(side="left", padx=2)
                    widgets[key] = lbl
                self._stock_row_widgets[sym] = widgets
                self._stock_row_widgets[sym]["_frame"] = row_frame

            # ── Update all cells ─────────────────────────────────────────────
            w   = self._stock_row_widgets[sym]
            st  = info.get("status", "WATCHING")

            # Symbol + % change
            pct = info.get("pct", "")
            w["symbol"].configure(
                text=f"{sym} {pct}",
                text_color=self.CLR_TEXT,
                font=ctk.CTkFont(size=10, weight="bold"),
            )

            # PDH / PDL
            w["pdh"].configure(text=info.get("pdh", "—"), text_color="#aaaacc")
            w["pdl"].configure(text=info.get("pdl", "—"), text_color="#aaaacc")

            # Breakout level — highlight once confirmed
            bl = info.get("breakout", "")
            w["breakout"].configure(
                text=bl if bl else "—",
                text_color="#ffd700" if bl else self.CLR_MUTED,
            )

            # SL — red, or orange after trail
            sl = info.get("sl", "")
            sl_color = "#ffa502" if info.get("sl_be") else self.CLR_RED
            w["sl"].configure(text=sl if sl else "—", text_color=sl_color if sl else self.CLR_MUTED)

            # TP — green once entered
            tp = info.get("tp", "")
            w["tp"].configure(text=tp if tp else "—", text_color=self.CLR_GREEN if tp else self.CLR_MUTED)

            # Entry price — green once entered
            entry = info.get("entry", "")
            w["entry"].configure(text=entry if entry else "—",
                                  text_color=self.CLR_GREEN if entry else self.CLR_MUTED)

            # Status badge
            STATUS_COLORS = {
                "WATCHING":  ("#8892a4", "#1a1a2e"),
                "PHASE1":    ("#70a1ff", "#0f2040"),
                "SETUP":     ("#ffd700", "#1a1500"),
                "ENTERED":   ("#00d26a", "#001a0e"),
                "SL HIT":    ("#ff4757", "#1a0000"),
                "TP HIT":    ("#00d26a", "#001a0e"),
                "TIME EXIT": ("#ffa502", "#1a0e00"),
                "DONE":      ("#555566", "#111122"),
            }
            fg, bg_hint = STATUS_COLORS.get(st, ("#8892a4", "#1a1a2e"))
            w["status"].configure(text=st, text_color=fg)

    # ── P&L callback (from PositionManager patch) ─────────────────────────────
    def _update_pnl(self, value: float) -> None:
        self._day_pnl = value
        if not self._live_pnl_active:
            color = self.CLR_GREEN if value >= 0 else self.CLR_RED
            self.pnl_label.configure(text=f"Rs. {value:+,.2f}", text_color=color)
            self.pnl_type_label.configure(text="realized")

    # ── Status callback ───────────────────────────────────────────────────────
    def _update_status(self, status: str) -> None:
        self._status = status
        if status == "RUNNING":
            self.status_label.configure(text="  RUNNING", text_color=self.CLR_GREEN)
            self.start_btn.configure(state="disabled")
            self.stop_btn.configure(state="normal")
        else:
            self.status_label.configure(text="  STOPPED", text_color=self.CLR_RED)
            self.start_btn.configure(state="normal")
            self.stop_btn.configure(state="disabled")
            self._live_pnl_active = False
            self.pnl_type_label.configure(text="")

    # ── Start / Stop ──────────────────────────────────────────────────────────
    def _build_config(self) -> StrategyConfig:
        return StrategyConfig(
            paper_trading               = (self.paper_var.get() == "Paper"),
            risk_reward_ratio           = float(self.rr_var.get()),
            breakout_buffer_pct         = float(self.buf_pct_var.get()),
            sl_buffer_pct               = float(self.sl_buf_pct_var.get()),
            max_stock_move_pct          = float(self.maxmov_var.get()),
            second_candle_max_range_pct = float(self.range_var.get()),
            trailing_trigger_pct        = float(self.trail_var.get()),
            use_breakout_confirmation   = (self.breakout_var.get() == "Yes"),
            qty_option_lots             = int(self.opt_lots_var.get()),
            qty_future_lots             = int(self.fut_lots_var.get()),
            max_trades_per_day          = int(self.max_trades_var.get()),
        )

    def _save_creds_clicked(self) -> None:
        fyers_id = self.fyers_id_var.get().strip()
        pin = self.pin_var.get().strip()
        totp_key = self.totp_key_var.get().strip()
        app_id = self.app_id_var.get().strip()
        secret = self.secret_key_var.get().strip()
        if not fyers_id or not pin or not totp_key or not app_id or not secret:
            self.creds_status.configure(text="All fields required", text_color=self.CLR_RED)
            return
        self._save_credentials(fyers_id, pin, totp_key, app_id, secret)
        self.creds_status.configure(text="Saved ✓", text_color=self.CLR_GREEN)

    def _apply_credentials(self) -> None:
        """Override fyers_connect and fyers_token credentials at runtime from saved file."""
        saved = self._load_credentials()
        fyers_id = saved.get("fyers_id", "").strip()
        pin = saved.get("pin", "").strip()
        totp_key = saved.get("totp_key", "").strip()
        app_id_full = saved.get("app_id", "").strip()
        secret = saved.get("secret_key", "").strip()
        if not app_id_full or not secret:
            return
        # Parse APP_ID and APP_TYPE from "XXXX-200" format
        if "-" in app_id_full:
            app_id, app_type = app_id_full.rsplit("-", 1)
        else:
            app_id, app_type = app_id_full, "200"
        client_id = f"{app_id}-{app_type}"
        # Patch fyers_connect module
        import fyers_connect
        fyers_connect.APP_ID = app_id
        fyers_connect.APP_TYPE = app_type
        fyers_connect.SECRET_KEY = secret
        fyers_connect.CLIENT_ID = client_id
        if fyers_id:
            fyers_connect.FY_ID = fyers_id
        if pin:
            fyers_connect.PIN = pin
        if totp_key:
            fyers_connect.TOTP_KEY = totp_key
        # Patch fyers_token module
        import fyers_token
        fyers_token.APP_ID = app_id
        fyers_token.APP_TYPE = app_type
        fyers_token.SECRET_KEY = secret
        fyers_token.CLIENT_ID = client_id
        if fyers_id:
            fyers_token.FY_ID = fyers_id
        if pin:
            fyers_token.PIN = pin
        if totp_key:
            fyers_token.TOTP_KEY = totp_key
        # Patch strategy module CLIENT_ID import
        import strategy
        strategy.CLIENT_ID = client_id

    def _start(self) -> None:
        if self.runner and self.runner.is_alive():
            return
        self._apply_credentials()
        try:
            cfg = self._build_config()
        except ValueError as e:
            self._append_log(f"[ERROR] Invalid config: {e}")
            return

        self._day_pnl         = 0.0
        self._trade_count     = 0
        self._live_pnl_active = False
        self._active_trades       = {}
        self._trade_cards         = {}
        self._monitored_stocks    = {}
        self._stock_row_widgets   = {}
        # Destroy existing stock row frames
        for w in self.stocks_frame.winfo_children():
            w.destroy()
        self.no_stocks_label = ctk.CTkLabel(
            self.stocks_frame, text="Waiting for market open...",
            font=ctk.CTkFont(size=11), text_color=self.CLR_MUTED)
        self.no_stocks_label.pack(pady=8)
        self.pnl_label.configure(text="Rs. 0.00", text_color=self.CLR_TEXT)
        self.pnl_type_label.configure(text="")
        self.trade_count_label.configure(text=f"Trades today: 0 / {self.max_trades_var.get()}")
        self._clear_log()
        self._append_log("[GUI] Starting strategy...")

        self.runner = StrategyRunner(
            cfg             = cfg,
            log_queue       = self.log_queue,
            status_callback = lambda s: self.root.after(0, self._update_status, s),
            pnl_callback    = lambda v: self.root.after(0, self._update_pnl, v),
        )
        self.runner.start()

    def _stop(self) -> None:
        if self.runner and self.runner.is_alive():
            self.runner.stop()
        self._append_log("[GUI] Stop signal sent.")

    def _on_close(self) -> None:
        if self.runner and self.runner.is_alive():
            self.runner.stop()
            time.sleep(0.5)
        self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    app = StrategyGUI()
    app.run()
