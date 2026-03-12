"""
gui.py
Fyers Sector Momentum Strategy - GUI
Balfund Trading Private Limited

CustomTkinter-based GUI wrapper around strategy.py.
Runs strategy in a background thread, captures all stdout/stderr
into the log panel, and displays live P&L.
"""

from __future__ import annotations

import io
import queue
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from typing import Optional

import customtkinter as ctk

# ── Strategy imports (all merged via bundler) ──────────────────────────────
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
    def __init__(self, cfg: StrategyConfig, log_queue: queue.Queue, status_callback, pnl_callback) -> None:
        super().__init__(daemon=True)
        self.cfg             = cfg
        self.log_queue       = log_queue
        self.status_callback = status_callback
        self.pnl_callback    = pnl_callback
        self._stop_event     = threading.Event()
        self.engine: Optional[SectorMomentumFyersStrategy] = None

    def run(self) -> None:
        # Redirect stdout so all strategy prints go to the log panel
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = QueueWriter(self.log_queue)
        sys.stderr = QueueWriter(self.log_queue)

        try:
            self.status_callback("RUNNING")
            self.engine = SectorMomentumFyersStrategy(self.cfg)

            # Monkey-patch day_pnl updates so GUI gets them live
            original_loop = self.engine.loop

            def patched_loop():
                original_loop()
                self.pnl_callback(self.engine.day_pnl)

            self.engine.loop = patched_loop

            # Patch inner loop to send P&L updates in real-time
            # and also check stop event
            original_update = self.engine.position_mgr.update

            def patched_update(trade, last_price, current_dt):
                result = original_update(trade, last_price, current_dt)
                self.pnl_callback(self.engine.day_pnl)
                if self._stop_event.is_set():
                    # Force exit all open trades
                    if not trade.exit_reason:
                        from strategy import PositionManager
                        PositionManager._exit(trade, last_price, current_dt, "MANUAL_STOP")
                return result

            self.engine.position_mgr.update = patched_update
            self.engine.loop()

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
    # ── Colour palette ──────────────────────────────────────────────────────
    CLR_BG        = "#1a1a2e"
    CLR_PANEL     = "#16213e"
    CLR_CARD      = "#0f3460"
    CLR_ACCENT    = "#e94560"
    CLR_GREEN     = "#00d26a"
    CLR_RED       = "#ff4757"
    CLR_TEXT      = "#eaeaea"
    CLR_MUTED     = "#8892a4"
    CLR_BORDER    = "#2a2a4a"

    def __init__(self) -> None:
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.root = ctk.CTk()
        self.root.title("Fyers Sector Momentum Strategy  |  Balfund Trading Pvt. Ltd.")
        self.root.geometry("1100x750")
        self.root.minsize(900, 600)
        self.root.configure(fg_color=self.CLR_BG)

        self.log_queue:    queue.Queue = queue.Queue()
        self.runner:       Optional[StrategyRunner] = None
        self._status       = "STOPPED"
        self._day_pnl      = 0.0
        self._trade_count  = 0

        self._build_ui()
        self._poll_log()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── UI construction ─────────────────────────────────────────────────────
    def _build_ui(self) -> None:
        # ── Top header bar ──────────────────────────────────────────────────
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

        # ── Main body ───────────────────────────────────────────────────────
        body = ctk.CTkFrame(self.root, fg_color=self.CLR_BG, corner_radius=0)
        body.pack(fill="both", expand=True, padx=0, pady=0)

        # Left panel — config + controls
        left = ctk.CTkFrame(body, fg_color=self.CLR_PANEL, width=320, corner_radius=0)
        left.pack(fill="y", side="left", padx=(10, 5), pady=10)
        left.pack_propagate(False)

        # Right panel — logs
        right = ctk.CTkFrame(body, fg_color=self.CLR_PANEL, corner_radius=10)
        right.pack(fill="both", expand=True, side="left", padx=(5, 10), pady=10)

        self._build_left_panel(left)
        self._build_right_panel(right)

    def _section(self, parent, title: str) -> ctk.CTkFrame:
        ctk.CTkLabel(
            parent,
            text=title,
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
            parent,
            text="Configuration",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=self.CLR_TEXT,
        ).pack(anchor="w", padx=16, pady=(14, 0))

        # ── Mode ──────────────────────────────────────────────────────────
        f = self._section(parent, "TRADING MODE")
        self.paper_var = ctk.StringVar(value="Paper")
        self._row(f, "Mode", lambda p: ctk.CTkSegmentedButton(
            p, values=["Paper", "Live"],
            variable=self.paper_var,
            font=ctk.CTkFont(size=12),
            width=130,
        ))

        # ── Entry settings ─────────────────────────────────────────────────
        f2 = self._section(parent, "ENTRY SETTINGS")
        self.rr_var     = ctk.StringVar(value="2.0")
        self.buf_var    = ctk.StringVar(value="1.0")
        self.maxmov_var = ctk.StringVar(value="3.0")
        self.range_var  = ctk.StringVar(value="1.0")
        self.trail_var  = ctk.StringVar(value="0.5")
        self.breakout_var = ctk.StringVar(value="Yes")

        self._row(f2, "Risk:Reward",     lambda p: ctk.CTkEntry(p, textvariable=self.rr_var,     width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Buffer Points",   lambda p: ctk.CTkEntry(p, textvariable=self.buf_var,    width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Max Stock Move%", lambda p: ctk.CTkEntry(p, textvariable=self.maxmov_var, width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "2nd Candle Range%", lambda p: ctk.CTkEntry(p, textvariable=self.range_var, width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Trail Trigger%",  lambda p: ctk.CTkEntry(p, textvariable=self.trail_var,  width=90, font=ctk.CTkFont(size=12)))
        self._row(f2, "Breakout Confirm", lambda p: ctk.CTkSegmentedButton(
            p, values=["Yes", "No"],
            variable=self.breakout_var,
            font=ctk.CTkFont(size=12),
            width=90,
        ))

        # ── Lots ──────────────────────────────────────────────────────────
        f3 = self._section(parent, "LOTS")
        self.opt_lots_var = ctk.StringVar(value="1")
        self.fut_lots_var = ctk.StringVar(value="1")
        self._row(f3, "Option Lots", lambda p: ctk.CTkEntry(p, textvariable=self.opt_lots_var, width=90, font=ctk.CTkFont(size=12)))
        self._row(f3, "Future Lots", lambda p: ctk.CTkEntry(p, textvariable=self.fut_lots_var, width=90, font=ctk.CTkFont(size=12)))

        # ── P&L display ───────────────────────────────────────────────────
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

        self.trade_count_label = ctk.CTkLabel(
            pnl_card,
            text="Trades today: 0 / 2",
            font=ctk.CTkFont(size=11),
            text_color=self.CLR_MUTED,
        )
        self.trade_count_label.pack()

        # ── Status ────────────────────────────────────────────────────────
        self.status_label = ctk.CTkLabel(
            parent,
            text="  STOPPED",
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=self.CLR_RED,
        )
        self.status_label.pack(pady=(10, 4))

        # ── Buttons ───────────────────────────────────────────────────────
        btn_frame = ctk.CTkFrame(parent, fg_color="transparent")
        btn_frame.pack(fill="x", padx=10, pady=4)

        self.start_btn = ctk.CTkButton(
            btn_frame,
            text="START",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=self.CLR_GREEN,
            hover_color="#00b359",
            text_color="#000000",
            height=42,
            corner_radius=8,
            command=self._start,
        )
        self.start_btn.pack(fill="x", pady=(0, 6))

        self.stop_btn = ctk.CTkButton(
            btn_frame,
            text="STOP",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=self.CLR_RED,
            hover_color="#cc0000",
            text_color="#ffffff",
            height=42,
            corner_radius=8,
            state="disabled",
            command=self._stop,
        )
        self.stop_btn.pack(fill="x")

    def _build_right_panel(self, parent: ctk.CTkFrame) -> None:
        # Header row
        header = ctk.CTkFrame(parent, fg_color="transparent")
        header.pack(fill="x", padx=12, pady=(10, 4))

        ctk.CTkLabel(
            header,
            text="Live Log",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=self.CLR_TEXT,
        ).pack(side="left")

        ctk.CTkButton(
            header,
            text="Clear",
            width=60,
            height=26,
            font=ctk.CTkFont(size=11),
            fg_color=self.CLR_CARD,
            hover_color=self.CLR_BORDER,
            command=self._clear_log,
        ).pack(side="right")

        # Log text box
        self.log_box = ctk.CTkTextbox(
            parent,
            font=ctk.CTkFont(family="Consolas", size=11),
            fg_color="#0d0d1a",
            text_color=self.CLR_TEXT,
            wrap="word",
            corner_radius=8,
            state="disabled",
        )
        self.log_box.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # Colour tags for log lines
        self.log_box._textbox.tag_config("entry",   foreground="#00d26a")
        self.log_box._textbox.tag_config("exit",    foreground="#ff6b81")
        self.log_box._textbox.tag_config("pnl",     foreground="#ffd700")
        self.log_box._textbox.tag_config("warn",    foreground="#ffa502")
        self.log_box._textbox.tag_config("error",   foreground="#ff4757")
        self.log_box._textbox.tag_config("setup",   foreground="#70a1ff")
        self.log_box._textbox.tag_config("breakout",foreground="#00d26a")
        self.log_box._textbox.tag_config("info",    foreground=self.CLR_TEXT)

    # ── Log handling ────────────────────────────────────────────────────────
    def _tag_for(self, text: str) -> str:
        t = text.upper()
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
        tag = self._tag_for(text)
        ts  = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {text.rstrip()}\n"

        self.log_box.configure(state="normal")
        self.log_box._textbox.insert("end", line, tag)
        self.log_box._textbox.see("end")
        self.log_box.configure(state="disabled")

        # Parse trade count from log
        if "DAY COUNT" in text.upper():
            try:
                count = int(text.split(":")[1].strip().split("/")[0])
                self._trade_count = count
                self.trade_count_label.configure(text=f"Trades today: {count} / 2")
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

    # ── P&L callback ────────────────────────────────────────────────────────
    def _update_pnl(self, value: float) -> None:
        self._day_pnl = value
        color = self.CLR_GREEN if value >= 0 else self.CLR_RED
        self.pnl_label.configure(
            text=f"Rs. {value:+,.2f}",
            text_color=color,
        )

    # ── Status callback ─────────────────────────────────────────────────────
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

    # ── Start / Stop ────────────────────────────────────────────────────────
    def _build_config(self) -> StrategyConfig:
        return StrategyConfig(
            paper_trading             = (self.paper_var.get() == "Paper"),
            risk_reward_ratio         = float(self.rr_var.get()),
            breakout_buffer_points    = float(self.buf_var.get()),
            max_stock_move_pct        = float(self.maxmov_var.get()),
            second_candle_max_range_pct = float(self.range_var.get()),
            trailing_trigger_pct      = float(self.trail_var.get()),
            use_breakout_confirmation = (self.breakout_var.get() == "Yes"),
            qty_option_lots           = int(self.opt_lots_var.get()),
            qty_future_lots           = int(self.fut_lots_var.get()),
        )

    def _start(self) -> None:
        if self.runner and self.runner.is_alive():
            return
        try:
            cfg = self._build_config()
        except ValueError as e:
            self._append_log(f"[ERROR] Invalid config: {e}")
            return

        self._day_pnl     = 0.0
        self._trade_count = 0
        self.pnl_label.configure(text="Rs. 0.00", text_color=self.CLR_TEXT)
        self.trade_count_label.configure(text="Trades today: 0 / 2")
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

    # ── Run ─────────────────────────────────────────────────────────────────
    def run(self) -> None:
        self.root.mainloop()


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    app = StrategyGUI()
    app.run()
