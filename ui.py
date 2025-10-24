# ui.py
import logging
import asyncio
import threading
import socket
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from pathlib import Path
import time
from typing import Optional

from db import (
    list_wbs,
    upsert_wbs,
    delete_wbs,
    list_mappings,
    upsert_mapping,
    delete_mapping,
    list_rules,
    upsert_rule,
    delete_rule,
)
from io_json import export_snapshot, import_snapshot
from sync.node import Node
from sync.broadcaster import Broadcaster
from sync.discovery import Discovery  # 자동 발견 사용 중이면 유지

logger = logging.getLogger("ui")


# ----------------------------
# Asyncio 백그라운드 워커
# ----------------------------
class AsyncioWorker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.loop = asyncio.new_event_loop()
        self._started_evt = threading.Event()

    def run(self):
        asyncio.set_event_loop(self.loop)
        self._started_evt.set()
        try:
            self.loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self.loop)
            for t in pending:
                t.cancel()
            if pending:
                self.loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self.loop.close()

    def start_loop(self):
        if not self.is_alive():
            self.start()
            self._started_evt.wait()

    def submit(self, coro_or_fn, *args, **kwargs):
        """코루틴 객체/함수/콜러블 모두 허용."""
        import inspect

        self.start_loop()
        if inspect.iscoroutine(coro_or_fn):
            coro = coro_or_fn
        elif inspect.iscoroutinefunction(coro_or_fn):
            coro = coro_or_fn(*args, **kwargs)
        elif callable(coro_or_fn):
            result = coro_or_fn(*args, **kwargs)
            if inspect.iscoroutine(result):
                coro = result
            else:

                async def _wrap(x):
                    return x

                coro = _wrap(result)
        else:
            raise TypeError("submit() requires coroutine (obj or fn) or callable")
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop_loop(self):
        if self.is_alive():
            self.loop.call_soon_threadsafe(self.loop.stop)


def _default_node_id() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "NODE"


# ----------------------------
# Frames (WBS/Mapping/Rules) - 기존과 동일
# ----------------------------
class WBSFrame(ttk.Frame):
    def __init__(self, parent, db_path: Path):
        super().__init__(parent)
        self.db_path = db_path
        self._build()

    def _build(self):
        self.tree = ttk.Treeview(
            self,
            columns=("wbs_code", "description", "updated_ts"),
            show="headings",
            height=10,
        )
        for col, txt, w in [
            ("wbs_code", "WBS Code", 150),
            ("description", "Description", 300),
            ("updated_ts", "Updated TS", 120),
        ]:
            self.tree.heading(col, text=txt)
            self.tree.column(col, width=w, stretch=False)
        self.tree.grid(row=0, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)
        ttk.Label(self, text="WBS Code").grid(row=1, column=0, sticky="e")
        ttk.Label(self, text="Description").grid(row=2, column=0, sticky="e")
        self.wbs_code = ttk.Entry(self)
        self.desc = ttk.Entry(self, width=50)
        self.wbs_code.grid(row=1, column=1, sticky="w")
        self.desc.grid(row=2, column=1, sticky="w")
        ttk.Button(self, text="Save/Upsert", command=self.save).grid(
            row=1, column=2, padx=5
        )
        ttk.Button(self, text="Delete", command=self.delete).grid(
            row=2, column=2, padx=5
        )
        ttk.Button(self, text="Refresh", command=self.refresh).grid(
            row=0, column=2, padx=5, sticky="ne"
        )
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.refresh()

    def refresh(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for r in list_wbs(self.db_path):
            self.tree.insert(
                "", "end", values=(r["wbs_code"], r["description"], r["updated_ts"])
            )

    def on_select(self, _):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.wbs_code.delete(0, tk.END)
        self.wbs_code.insert(0, vals[0])
        self.desc.delete(0, tk.END)
        self.desc.insert(0, vals[1])

    def save(self):
        code = self.wbs_code.get().strip()
        desc = self.desc.get().strip()
        if not code:
            messagebox.showwarning("Validation", "WBS Code is required")
            return
        upsert_wbs(self.db_path, code, desc)
        self.refresh()

    def delete(self):
        code = self.wbs_code.get().strip()
        if not code:
            messagebox.showwarning("Validation", "Select a WBS to delete")
            return
        if messagebox.askyesno("Confirm", f"Delete WBS '{code}'?"):
            delete_wbs(self.db_path, code)
            self.refresh()


class MappingFrame(ttk.Frame):
    def __init__(self, parent, db_path: Path):
        super().__init__(parent)
        self.db_path = db_path
        self._build()

    def _build(self):
        self.tree = ttk.Treeview(
            self,
            columns=("type_name", "wbs_code", "updated_ts"),
            show="headings",
            height=10,
        )
        for col, txt, w in [
            ("type_name", "Revit Type", 280),
            ("wbs_code", "WBS Code", 150),
            ("updated_ts", "Updated TS", 120),
        ]:
            self.tree.heading(col, text=txt)
            self.tree.column(col, width=w, stretch=False)
        self.tree.grid(row=0, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)
        ttk.Label(self, text="Revit Type").grid(row=1, column=0, sticky="e")
        ttk.Label(self, text="WBS Code").grid(row=2, column=0, sticky="e")
        self.type_name = ttk.Entry(self, width=40)
        self.wbs_code = ttk.Entry(self, width=20)
        self.type_name.grid(row=1, column=1, sticky="w")
        self.wbs_code.grid(row=2, column=1, sticky="w")
        ttk.Button(self, text="Save/Upsert", command=self.save).grid(
            row=1, column=2, padx=5
        )
        ttk.Button(self, text="Delete", command=self.delete).grid(
            row=2, column=2, padx=5
        )
        ttk.Button(self, text="Refresh", command=self.refresh).grid(
            row=0, column=2, padx=5, sticky="ne"
        )
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.refresh()

    def refresh(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for r in list_mappings(self.db_path):
            self.tree.insert(
                "", "end", values=(r["type_name"], r["wbs_code"], r["updated_ts"])
            )

    def on_select(self, _):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.type_name.delete(0, tk.END)
        self.type_name.insert(0, vals[0])
        self.wbs_code.delete(0, tk.END)
        self.wbs_code.insert(0, vals[1])

    def save(self):
        t = self.type_name.get().strip()
        w = self.wbs_code.get().strip()
        if not t or not w:
            messagebox.showwarning("Validation", "Type and WBS Code are required")
            return
        upsert_mapping(self.db_path, t, w)
        self.refresh()

    def delete(self):
        t = self.type_name.get().strip()
        w = self.wbs_code.get().strip()
        if not t or not w:
            messagebox.showwarning("Validation", "Select a mapping to delete")
            return
        if messagebox.askyesno("Confirm", f"Delete mapping '{t} | {w}'?"):
            delete_mapping(self.db_path, t, w)
            self.refresh()


class RulesFrame(ttk.Frame):
    def __init__(self, parent, db_path: Path):
        super().__init__(parent)
        self.db_path = db_path
        self._build()

    def _build(self):
        self.tree = ttk.Treeview(
            self,
            columns=("wbs_code", "rule_type", "formula", "unit", "updated_ts"),
            show="headings",
            height=10,
        )
        defs = [
            ("wbs_code", "WBS Code", 140),
            ("rule_type", "Rule Type", 120),
            ("formula", "Formula", 300),
            ("unit", "Unit", 80),
            ("updated_ts", "Updated TS", 120),
        ]
        for col, txt, w in defs:
            self.tree.heading(col, text=txt)
            self.tree.column(col, width=w, stretch=False)
        self.tree.grid(row=0, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)
        ttk.Label(self, text="WBS Code").grid(row=1, column=0, sticky="e")
        ttk.Label(self, text="Rule Type").grid(row=2, column=0, sticky="e")
        ttk.Label(self, text="Formula").grid(row=3, column=0, sticky="e")
        ttk.Label(self, text="Unit").grid(row=4, column=0, sticky="e")
        self.wbs_code = ttk.Entry(self, width=20)
        self.rule_type = ttk.Entry(self, width=20)
        self.formula = ttk.Entry(self, width=50)
        self.unit = ttk.Entry(self, width=10)
        self.wbs_code.grid(row=1, column=1, sticky="w")
        self.rule_type.grid(row=2, column=1, sticky="w")
        self.formula.grid(row=3, column=1, sticky="w")
        self.unit.grid(row=4, column=1, sticky="w")
        ttk.Button(self, text="Save/Upsert", command=self.save).grid(
            row=1, column=2, padx=5
        )
        ttk.Button(self, text="Delete", command=self.delete).grid(
            row=2, column=2, padx=5
        )
        ttk.Button(self, text="Refresh", command=self.refresh).grid(
            row=0, column=2, padx=5, sticky="ne"
        )
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.refresh()

    def refresh(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for r in list_rules(self.db_path):
            self.tree.insert(
                "",
                "end",
                values=(r["wbs_code"], r["rule_type"], r["formula"], r["updated_ts"]),
            )

    def on_select(self, _):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.wbs_code.delete(0, tk.END)
        self.wbs_code.insert(0, vals[0])
        self.rule_type.delete(0, tk.END)
        self.rule_type.insert(0, vals[1])
        self.formula.delete(0, tk.END)
        self.formula.insert(0, vals[2])
        self.unit.delete(0, tk.END)
        self.unit.insert(0, vals[3])

    def save(self):
        w = self.wbs_code.get().strip()
        rt = self.rule_type.get().strip()
        f = self.formula.get().strip()
        u = self.unit.get().strip()
        if not w:
            messagebox.showwarning("Validation", "WBS Code is required")
            return
        upsert_rule(self.db_path, w, rt, f, u)
        self.refresh()

    def delete(self):
        w = self.wbs_code.get().strip()
        if not w:
            messagebox.showwarning("Validation", "Select a rule to delete")
            return
        if messagebox.askyesno("Confirm", f"Delete rule for '{w}'?"):
            delete_rule(self.db_path, w)
            self.refresh()


# ----------------------------
# Peer Monitor Window (Toplevel)
# ----------------------------
class PeerMonitor(tk.Toplevel):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.title("피어 목록 (실시간)")
        self.geometry("720x320")
        self.resizable(True, True)
        self.app = app_ref  # App 참조 (node, worker, discovery 등 접근용)
        cols = (
            "host",
            "port",
            "direction",
            "state",
            "remote_node_id",
            "connected_at",
            "last_seen",
            "errors",
        )
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=12)
        headers = {
            "host": "Host",
            "port": "Port",
            "direction": "Dir",
            "state": "State",
            "remote_node_id": "Remote Node",
            "connected_at": "Connected",
            "last_seen": "Last Seen",
            "errors": "Errors",
        }
        widths = {
            "host": 150,
            "port": 60,
            "direction": 60,
            "state": 90,
            "remote_node_id": 160,
            "connected_at": 120,
            "last_seen": 120,
            "errors": 60,
        }
        for c in cols:
            self.tree.heading(c, text=headers[c])
            self.tree.column(c, width=widths[c], anchor="w", stretch=False)
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)

        # 하단 버튼
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=6, pady=4)
        ttk.Button(btns, text="지금 찾기", command=self._discover_now).pack(side="left")
        ttk.Button(btns, text="닫기", command=self._on_close).pack(side="right")

        # 창 닫기 핸들러
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        # App이 콜백 해제/정리는 외부에서 수행
        self.destroy()

    def _discover_now(self):
        # Discovery가 있으면 즉시 탐색 송신
        if not self.app.discovery:
            messagebox.showwarning(
                "자동 발견", "자동 발견이 꺼져있습니다. P2P 시작 후 사용하세요."
            )
            return

        async def runner():
            await self.app.discovery.send_discover_now()

        self.app.worker.submit(runner)
        self.app.status.set("Discover sent now")

    def _fmt_time(self, epoch: float) -> str:
        if not epoch:
            return ""
        try:
            return time.strftime("%H:%M:%S", time.localtime(epoch))
        except Exception:
            return str(epoch)

    def update_peers(self, peers: list[dict]):
        # 전체 갱신(소규모라 부담 적음)
        try:
            current_ids = set(self.tree.get_children())
            for iid in current_ids:
                self.tree.delete(iid)
            for m in peers:
                vals = (
                    m.get("host", ""),
                    str(m.get("port", "")),
                    m.get("direction", ""),
                    m.get("state", ""),
                    m.get("remote_node_id", ""),
                    self._fmt_time(m.get("connected_at", 0.0)),
                    self._fmt_time(m.get("last_seen", 0.0)),
                    str(m.get("errors", 0)),
                )
                self.tree.insert("", "end", values=vals)
        except Exception as e:
            logger.exception(f"PeerMonitor update error: {e}")


# ----------------------------
# Main App
# ----------------------------
class App(ttk.Frame):
    def __init__(self, master, db_path: Path):
        super().__init__(master)
        self.master = master
        self.db_path = db_path

        self.project_var = tk.StringVar(value="TVEP")
        self.node_id_var = tk.StringVar(value=_default_node_id())
        self.port_var = tk.StringVar(value="9000")
        self.token_var = tk.StringVar(value="SHARED-SECRET")

        self.node: Optional[Node] = None
        self.broadcaster: Optional[Broadcaster] = None
        self.discovery: Optional[Discovery] = None
        self.worker = AsyncioWorker()

        # UI 이벤트 큐 (DB/Peer 이벤트 수신)
        self.ui_event_q = queue.Queue()

        # 자동 새로고침(페일세이프)
        self.auto_refresh_enabled = tk.BooleanVar(value=True)
        self.auto_refresh_ms = tk.IntVar(value=1000)

        # 자동 발견 옵션
        self.autodiscover_enabled = tk.BooleanVar(value=True)
        self.discovery_port = tk.IntVar(value=9999)
        self.discovery_interval_ms = tk.IntVar(value=3000)
        self.discovery_iface_ip = tk.StringVar(value="")  # 비워두면 ANY

        # 피어 목록 창
        self.peer_window: Optional[PeerMonitor] = None

        self._build()

    def _build(self):
        self.master.title("Rules Collaboration - P2P LWW (B)")
        self.pack(fill="both", expand=True)

        # Export/Import
        top = ttk.Frame(self)
        top.pack(fill="x", padx=5, pady=5)
        ttk.Label(top, text="Project").pack(side="left")
        ttk.Entry(top, textvariable=self.project_var, width=12).pack(
            side="left", padx=5
        )
        ttk.Button(top, text="Export JSON", command=self.on_export).pack(
            side="left", padx=5
        )
        ttk.Button(top, text="Import JSON", command=self.on_import).pack(
            side="left", padx=5
        )

        # P2P bar
        p2p = ttk.Frame(self)
        p2p.pack(fill="x", padx=5, pady=5)
        ttk.Label(p2p, text="Node ID").pack(side="left")
        ttk.Entry(p2p, textvariable=self.node_id_var, width=18).pack(
            side="left", padx=5
        )
        ttk.Label(p2p, text="Port").pack(side="left")
        ttk.Entry(p2p, textvariable=self.port_var, width=6).pack(side="left", padx=5)
        ttk.Label(p2p, text="Token").pack(side="left")
        ttk.Entry(p2p, textvariable=self.token_var, width=16).pack(side="left", padx=5)
        ttk.Button(p2p, text="P2P 시작", command=self.start_p2p).pack(
            side="left", padx=5
        )
        ttk.Button(p2p, text="P2P 중지", command=self.stop_p2p).pack(
            side="left", padx=5
        )
        ttk.Button(p2p, text="피어 추가", command=self.add_peer).pack(
            side="left", padx=5
        )
        ttk.Button(p2p, text="피어 목록", command=self.open_peer_window).pack(
            side="left", padx=5
        )  # ★ 추가

        # Auto-Discovery bar
        ad = ttk.Frame(self)
        ad.pack(fill="x", padx=5, pady=5)
        ttk.Checkbutton(
            ad, text="자동 피어 발견", variable=self.autodiscover_enabled
        ).pack(side="left")
        ttk.Label(ad, text="UDP 포트").pack(side="left", padx=(8, 2))
        ttk.Spinbox(
            ad,
            from_=1024,
            to=65535,
            increment=1,
            textvariable=self.discovery_port,
            width=7,
        ).pack(side="left")
        ttk.Label(ad, text="주기(ms)").pack(side="left", padx=(8, 2))
        ttk.Spinbox(
            ad,
            from_=500,
            to=10000,
            increment=500,
            textvariable=self.discovery_interval_ms,
            width=7,
        ).pack(side="left")
        ttk.Label(ad, text="iface_ip").pack(side="left", padx=(8, 2))
        ttk.Entry(ad, textvariable=self.discovery_iface_ip, width=14).pack(side="left")
        ttk.Button(ad, text="지금 찾기", command=self.discover_now).pack(
            side="left", padx=8
        )

        # Notebook tabs
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=5, pady=5)
        self.wbs_frame = WBSFrame(nb, self.db_path)
        self.map_frame = MappingFrame(nb, self.db_path)
        self.rules_frame = RulesFrame(nb, self.db_path)
        nb.add(self.wbs_frame, text="WBS 관리")
        nb.add(self.map_frame, text="타입-매핑")
        nb.add(self.rules_frame, text="산출규칙")

        # Status bar
        self.status = tk.StringVar(value=f"Ready. DB: {self.db_path}")
        status_bar = ttk.Label(self, textvariable=self.status, anchor="w")
        status_bar.pack(fill="x")

        # 종료 훅
        self.master.protocol("WM_DELETE_WINDOW", self._on_close)

        # 이벤트 큐 폴링 & 페일세이프 자동 새로고침 루프
        self.after(200, self._poll_ui_events)
        self.after(self.auto_refresh_ms.get(), self._auto_refresh_tick)

    # ---------- Peer Monitor ----------
    def open_peer_window(self):
        if self.peer_window and self.peer_window.winfo_exists():
            self.peer_window.lift()
            return
        self.peer_window = PeerMonitor(self.master, self)
        # Node에 피어 이벤트 콜백 등록 → UI 큐로 밀어넣기
        if self.node:

            def on_peer_event(evt: dict):
                try:
                    # 스레드-세이프 큐로 전달
                    self.ui_event_q.put(("peer_update", evt.get("peers", [])))
                except Exception as e:
                    logger.warning(f"enqueue peer event failed: {e}")

            self.node.set_peer_event_callback(on_peer_event)
            # 현재 스냅샷 1회 강제 발행
            self.node.fire_peer_snapshot_threadsafe()

    # ---------- UI 이벤트/자동 갱신 ----------
    def _poll_ui_events(self):
        try:
            fired_refresh = False
            peers_payload = None
            while True:
                try:
                    kind, data = self.ui_event_q.get_nowait()
                except queue.Empty:
                    break
                if kind == "peer_update":
                    peers_payload = data  # 마지막만 반영
                else:
                    # DB 변경 등의 다른 타입도 있었으면 여기서 처리
                    fired_refresh = True
            if (
                peers_payload is not None
                and self.peer_window
                and self.peer_window.winfo_exists()
            ):
                self.peer_window.update_peers(peers_payload)
            if fired_refresh:
                # 필요 시 다른 뷰 리프레시
                pass
        except Exception as e:
            logger.exception(f"UI event poll error: {e}")
        finally:
            self.after(200, self._poll_ui_events)

    def _auto_refresh_tick(self):
        try:
            if self.auto_refresh_enabled.get():
                self.refresh_all()
                self.status.set(
                    f"Auto-refreshed every {self.auto_refresh_ms.get()}ms | DB: {self.db_path}"
                )
        except Exception as e:
            logger.exception(f"Auto refresh tick error: {e}")
        finally:
            self.after(self.auto_refresh_ms.get(), self._auto_refresh_tick)

    def refresh_all(self):
        try:
            self.wbs_frame.refresh()
            self.map_frame.refresh()
            self.rules_frame.refresh()
        except Exception as e:
            logger.exception(f"Refresh all error: {e}")

    # ---------- Import/Export ----------
    def on_export(self):
        project = self.project_var.get().strip() or "PROJECT"
        out_dir = filedialog.askdirectory(title="Select export directory")
        if not out_dir:
            return
        path = export_snapshot(self.db_path, project, Path(out_dir))
        self.status.set(f"Exported: {path}")
        messagebox.showinfo("Export", f"Exported to:\n{path}")

    def on_import(self):
        path = filedialog.askopenfilename(
            title="Select JSON file", filetypes=[("JSON files", "*.json")]
        )
        if not path:
            return
        w, m, r = import_snapshot(self.db_path, Path(path))
        self.refresh_all()
        self.status.set(f"Imported: WBS={w}, Mappings={m}, Rules={r}")
        messagebox.showinfo("Import", f"Imported: WBS={w}, Mappings={m}, Rules={r}")

    # ---------- P2P ----------
    def start_p2p(self):
        if self.node:
            messagebox.showinfo("P2P", "Already running")
            return
        node_id = self.node_id_var.get().strip() or _default_node_id()
        token = self.token_var.get().strip() or "SHARED-SECRET"
        port = int(self.port_var.get().strip() or "9000")

        self.node = Node(
            self.db_path, node_id=node_id, token=token, host="0.0.0.0", port=port
        )
        self.broadcaster = Broadcaster(
            self.db_path, self.node, node_id=node_id, token=token
        )

        # 자동 발견 초기화(안정 버전 Discovery 사용 중이라면)
        if self.autodiscover_enabled.get():
            self.discovery = Discovery(
                self.node,
                token=token,
                port=self.discovery_port.get(),
                interval=self.discovery_interval_ms.get() / 1000.0,
                iface_ip=(self.discovery_iface_ip.get().strip() or None),
                ttl=1,
                loopback=1,
            )
        else:
            self.discovery = None

        # 피어 이벤트 콜백 등록(피어 창이 열려있든 아니든 수신)
        def on_peer_event(evt: dict):
            try:
                self.ui_event_q.put(("peer_update", evt.get("peers", [])))
            except Exception as e:
                logger.warning(f"enqueue peer event failed: {e}")

        self.node.set_peer_event_callback(on_peer_event)

        async def runner():
            await self.node.start()
            self.broadcaster.attach()
            if self.discovery:
                await self.discovery.start()
                await self.discovery.send_discover_now()
            logger.info(
                f"P2P started in asyncio worker (node_id={node_id}, port={port})"
            )

        self.worker.submit(runner)
        self.status.set(f"P2P Started: node_id={node_id}, port={port}")

    def stop_p2p(self):
        if not self.node:
            return

        async def runner():
            try:
                if self.discovery:
                    await self.discovery.stop()
                if self.broadcaster:
                    self.broadcaster.detach()
                if self.node:
                    await self.node.stop()
            finally:
                logger.info("P2P stopped in asyncio worker")

        fut = self.worker.submit(runner)
        try:
            fut.result(timeout=5)
        except Exception:
            pass
        self.node = None
        self.broadcaster = None
        self.discovery = None
        self.status.set("P2P Stopped")

    def add_peer(self):
        if not self.node:
            messagebox.showwarning("P2P", "Start P2P first")
            return
        input_str = simpledialog.askstring(
            "피어 추가", "IP:PORT 형식으로 입력 (예: 192.168.0.10:9001)"
        )
        if not input_str:
            return
        try:
            host, port = input_str.split(":")
            port = int(port)
        except Exception:
            messagebox.showerror("입력 오류", "형식: IP:PORT")
            return

        async def runner():
            self.node.add_peer(host, port)

        self.worker.submit(runner)
        self.status.set(f"Peer added: {host}:{port}")

    def discover_now(self):
        if not self.discovery:
            messagebox.showwarning(
                "자동 발견", "먼저 P2P를 시작하거나 자동 발견을 켜세요"
            )
            return

        async def runner():
            await self.discovery.send_discover_now()

        self.worker.submit(runner)
        self.status.set("Discover sent now")

    # ---------- 공통 ----------
    def _on_close(self):
        try:
            self.stop_p2p()
        finally:
            self.master.destroy()


def run_app(db_path: Path):
    root = tk.Tk()
    app = App(root, db_path)
    root.geometry("1100x720")
    root.mainloop()
