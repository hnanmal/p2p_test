# sync/node.py
import asyncio
import logging
import time
from typing import Dict, Any, Set, Tuple, Optional, List
from pathlib import Path

from .protocol import validate_message, loads_line, dumps_line
from .snapshot import build_snapshot, apply_snapshot
from .lww import apply_message  # 기존 변경 이벤트 처리

logger = logging.getLogger("sync.node")

ACKABLE_TYPES = {
    "upsert_wbs",
    "delete_wbs",
    "upsert_mapping",
    "delete_mapping",
    "upsert_rule",
    "delete_rule",
}


class Node:
    def __init__(
        self,
        db_path: Path,
        node_id: str,
        token: str,
        host: str = "0.0.0.0",
        port: int = 9000,
    ):
        self.db_path = db_path
        self.node_id = node_id
        self.token = token
        self.host = host
        self.port = int(port)

        self._server: asyncio.base_events.Server | None = None
        self._peers: Set[Tuple[str, int]] = set()
        self._writers: Set[asyncio.StreamWriter] = set()
        self._tasks: Set[asyncio.Task] = set()
        self._running = False
        self.loop: asyncio.AbstractEventLoop | None = None

        # ---- 피어 상태 추적 ----
        # key: (host, port) -> meta dict
        self._peers_meta: Dict[Tuple[str, int], Dict[str, Any]] = {}
        self._on_peer_event: Optional[callable] = (
            None  # UI에 알림 (루프 스레드에서 호출됨)
        )

    # ====== Peer state helpers ======
    def set_peer_event_callback(self, cb: Optional[callable]):
        """피어 상태 변경 콜백 등록/해제. 콜백은 이벤트 루프 스레드에서 호출됨."""
        self._on_peer_event = cb

    def _peer_key(self, host: str, port: int) -> Tuple[str, int]:
        return (host, int(port))

    def _peer_snapshot(self) -> List[Dict[str, Any]]:
        return [dict(v) for v in self._peers_meta.values()]

    def fire_peer_snapshot_threadsafe(self):
        """UI 스레드에서 호출해도 안전하게 전체 스냅샷 이벤트를 발행."""
        if not self.loop or not self._on_peer_event:
            return

        def _emit():
            try:
                self._on_peer_event(
                    {"event": "snapshot", "peers": self._peer_snapshot()}
                )
            except Exception as e:
                logger.warning(f"peer snapshot emit failed: {e}")

        try:
            self.loop.call_soon_threadsafe(_emit)
        except Exception as e:
            logger.warning(f"peer snapshot schedule failed: {e}")

    def _notify_peer_event(self, kind: str, meta: Dict[str, Any]):
        if self._on_peer_event:
            try:
                self._on_peer_event(
                    {"event": kind, "peer": dict(meta), "peers": self._peer_snapshot()}
                )
            except Exception as e:
                logger.warning(f"peer event callback failed: {e}")

    def _get_or_make_peer(self, host: str, port: int) -> Dict[str, Any]:
        k = self._peer_key(host, port)
        m = self._peers_meta.get(k)
        if not m:
            m = {
                "host": host,
                "port": int(port),
                "state": "disconnected",  # connecting/connected/disconnected
                "direction": "out",  # out/in
                "remote_node_id": "",
                "last_seen": 0.0,
                "connected_at": 0.0,
                "errors": 0,
            }
            self._peers_meta[k] = m
        return m

    def _mark_connecting(self, host: str, port: int, direction: str):
        m = self._get_or_make_peer(host, port)
        m["direction"] = direction
        m["state"] = "connecting"
        self._notify_peer_event("connecting", m)

    def _mark_connected(
        self, host: str, port: int, direction: str, remote_node_id: Optional[str] = None
    ):
        m = self._get_or_make_peer(host, port)
        m["direction"] = direction
        m["state"] = "connected"
        m["connected_at"] = time.time()
        if remote_node_id:
            m["remote_node_id"] = remote_node_id
        self._notify_peer_event("connected", m)

    def _mark_seen(self, host: str, port: int, remote_node_id: Optional[str] = None):
        m = self._get_or_make_peer(host, port)
        m["last_seen"] = time.time()
        if remote_node_id:
            m["remote_node_id"] = remote_node_id
        self._notify_peer_event("seen", m)

    def _mark_disconnected(self, host: str, port: int):
        m = self._get_or_make_peer(host, port)
        m["state"] = "disconnected"
        self._notify_peer_event("disconnected", m)

    # ====== Node lifecycle ======
    async def start(self):
        if self._running:
            return
        self.loop = asyncio.get_running_loop()
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        self._running = True
        logger.info(f"Node started at {self.host}:{self.port} (node_id={self.node_id})")
        for h, p in list(self._peers):
            self._tasks.add(self.loop.create_task(self._connect_to_peer(h, p)))

    async def stop(self):
        self._running = False

        for w in list(self._writers):
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass
        self._writers.clear()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()

        logger.info("Node stopped.")

    def add_peer(self, host: str, port: int):
        self._peers.add((host, int(port)))
        self._mark_connecting(host, port, direction="out")
        if self._running and self.loop:
            self._tasks.add(
                self.loop.create_task(self._connect_to_peer(host, int(port)))
            )
        logger.info(f"Peer added: {host}:{port}")

    async def _connect_to_peer(self, host: str, port: int):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            self._writers.add(writer)
            self._mark_connected(host, port, direction="out")

            # handshake
            hello = {
                "type": "hello",
                "payload": {},
                "ts": int(time.time()),
                "node_id": self.node_id,
                "token": self.token,
            }
            writer.write(dumps_line(hello).encode("utf-8"))
            await writer.drain()
            logger.info(f"Connected to peer {host}:{port}")

            # 읽기 루프 시작
            task = asyncio.create_task(self._read_loop(reader, writer))
            self._tasks.add(task)

            # 연결 직후 우리 스냅샷 1회 송신
            await self._send_snapshot(writer)
        except Exception as e:
            logger.warning(f"Failed to connect to peer {host}:{port}: {e}")
            # 실패 카운트 & 상태 갱신
            m = self._get_or_make_peer(host, port)
            m["errors"] = int(m.get("errors", 0)) + 1
            self._mark_disconnected(host, port)

    async def _send_snapshot(self, writer: asyncio.StreamWriter):
        snap = build_snapshot(self.db_path)
        msg = {
            "type": "sync_snapshot",
            "payload": snap,
            "ts": int(time.time()),
            "node_id": self.node_id,
            "token": self.token,
        }
        await self._send_line(writer, msg)
        logger.info("Initial sync_snapshot sent")

    async def _read_loop(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        host = addr[0] if addr else "?"
        port = addr[1] if addr else 0
        try:
            while self._running:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = loads_line(line.decode("utf-8").strip())
                except Exception:
                    logger.warning("Invalid JSON line received")
                    continue

                if not validate_message(msg):
                    logger.warning("Invalid message schema")
                    continue

                # 토큰 검증
                if msg.get("token") != self.token:
                    logger.warning("Rejected message due to invalid token")
                    continue

                mtype = msg["type"]
                remote_node_id = msg.get("node_id", "")

                # 매 메시지 수신 시 마지막 본 시각/원격 노드ID 갱신
                self._mark_seen(host, port, remote_node_id=remote_node_id)

                if mtype == "hello":
                    # hello_ack 응답 + 스냅샷 송신
                    hello_ack = {
                        "type": "hello_ack",
                        "payload": {"status": "ok"},
                        "ts": int(time.time()),
                        "node_id": self.node_id,
                        "token": self.token,
                    }
                    await self._send_line(writer, hello_ack)
                    await self._send_snapshot(writer)
                    # 상태를 inbound connected로 보정
                    self._mark_connected(
                        host, port, direction="in", remote_node_id=remote_node_id
                    )
                    continue

                if mtype in ("hello_ack", "ack"):
                    # 핸드셰이크/회신은 무시
                    continue

                if mtype == "sync_snapshot":
                    apply_snapshot(
                        self.db_path,
                        local_node_id=self.node_id,
                        remote_node_id=remote_node_id,
                        snapshot=msg["payload"],
                    )
                    done = {
                        "type": "ack",
                        "payload": {"applied_type": "sync_snapshot"},
                        "ts": int(time.time()),
                        "node_id": self.node_id,
                        "token": self.token,
                    }
                    await self._send_line(writer, done)
                    continue

                # 변이 이벤트 처리 + ack
                if mtype in ACKABLE_TYPES:
                    apply_message(self.db_path, self.node_id, msg)
                    ack = {
                        "type": "ack",
                        "payload": {"applied_type": mtype},
                        "ts": int(time.time()),
                        "node_id": self.node_id,
                        "token": self.token,
                    }
                    await self._send_line(writer, ack)
                else:
                    logger.debug(f"Ignored message type: {mtype}")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Read loop error from {addr}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            if writer in self._writers:
                self._writers.remove(writer)
            self._mark_disconnected(host, port)
            logger.info(f"Connection closed from {addr}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        host = addr[0] if addr else "?"
        port = addr[1] if addr else 0
        logger.info(f"Incoming connection from {addr}")
        self._writers.add(writer)
        self._mark_connected(host, port, direction="in")
        task = asyncio.create_task(self._read_loop(reader, writer))
        self._tasks.add(task)

    async def _send_line(self, writer: asyncio.StreamWriter, msg: Dict[str, Any]):
        try:
            data = dumps_line(msg).encode("utf-8")
            writer.write(data)
            await writer.drain()
        except Exception as e:
            logger.warning(f"Send failed: {e}")

    async def _broadcast_async(self, msg: Dict[str, Any]):
        dead = []
        for w in list(self._writers):
            try:
                await self._send_line(w, msg)
            except Exception:
                dead.append(w)
        for w in dead:
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass
            self._writers.discard(w)

    def broadcast(self, msg: Dict[str, Any]):
        if not self._running or not self.loop:
            return
        try:
            self.loop.call_soon_threadsafe(
                lambda: asyncio.create_task(self._broadcast_async(msg))
            )
        except Exception as e:
            logger.exception(f"Thread-safe broadcast schedule failed: {e}")
