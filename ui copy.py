# ui.py
import logging
import asyncio
import threading
import socket
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from pathlib import Path

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
    get_change_seq,  # ★ 자동 리프레시용
)
from io_json import export_snapshot, import_snapshot
from sync.node import Node
from sync.broadcaster import Broadcaster

logger = logging.getLogger("ui")


# ----------------------------
# Asyncio 백그라운드 워커 (크로스플랫폼)
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
        """
        코루틴 객체/함수 모두 허용.
        - submit(runner) / submit(runner()) 모두 OK
        """
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
# WBS Frame
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


# ----------------------------
# Mapping Frame
# ----------------------------
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


# ----------------------------
# Rules Frame
# ----------------------------
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
                values=(
                    r["wbs_code"],
                    r["rule_type"],
                    r["formula"],
                    r["unit"],
                    r["updated_ts"],
                ),
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

        self.node = None
        self.broadcaster = None
        self.worker = AsyncioWorker()  # 백그라운드 asyncio 루프

        self._last_seq_seen = -1  # ★ 자동 새로고침 상태
        self._build()

    def _build(self):
        self.master.title("Rules Collaboration - P2P LWW (B)")
        self.pack(fill="both", expand=True)

        # Top bar: project & import/export
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
        self.status = tk.StringVar(value="Ready.")
        status_bar = ttk.Label(self, textvariable=self.status, anchor="w")
        status_bar.pack(fill="x")

        # 안전 종료 훅
        self.master.protocol("WM_DELETE_WINDOW", self._on_close)

        # ★ 자동 리프레시 루프 시작
        self.after(700, self._auto_refresh_tick)

    def _on_close(self):
        try:
            self.stop_p2p()
        finally:
            self.master.destroy()

    # -------- Auto Refresh ----------
    def _auto_refresh_tick(self):
        try:
            seq = get_change_seq()
            if seq != self._last_seq_seen:
                self._last_seq_seen = seq
                # 간단하게 전체 탭 리프레시
                self.wbs_frame.refresh()
                self.map_frame.refresh()
                self.rules_frame.refresh()
                self.status.set(f"Auto-refreshed (seq={seq})")
        except Exception as e:
            # 예외가 있어도 루프가 끊기지 않도록 처리
            logger.exception(f"Auto refresh tick error: {e}")
        finally:
            # 다음 주기
            self.after(700, self._auto_refresh_tick)

    # -------- Import/Export ----------
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
        # 즉시 수동 갱신도 병행 (사용자 체감 향상)
        self.wbs_frame.refresh()
        self.map_frame.refresh()
        self.rules_frame.refresh()
        self.status.set(f"Imported: WBS={w}, Mappings={m}, Rules={r}")
        messagebox.showinfo("Import", f"Imported: WBS={w}, Mappings={m}, Rules={r}")

    # -------- P2P 제어 ----------
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

        async def runner():
            await self.node.start()
            self.broadcaster.attach()
            logger.info(
                f"P2P started in asyncio worker (node_id={node_id}, port={port})"
            )

        self.worker.submit(runner)  # 코루틴 함수 전달 OK (submit이 감싸줌)
        self.status.set(f"P2P Started: node_id={node_id}, port={port}")

    def stop_p2p(self):
        if not self.node:
            return

        async def runner():
            try:
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


def run_app(db_path: Path):
    root = tk.Tk()
    app = App(root, db_path)
    root.geometry("1000x650")
    root.mainloop()
