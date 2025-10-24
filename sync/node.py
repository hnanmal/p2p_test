# sync/node.py
import asyncio
import logging
import time
from typing import Dict, Any, Set, Tuple, Optional
from pathlib import Path

from .protocol import validate_message, loads_line, dumps_line
from .lww import apply_message

logger = logging.getLogger("sync.node")


class Node:
    def __init__(
        self,
        db_path: Path,
        node_id: str,
        token: str,
        host: str = "0.0.0.0",
        port: int = 9000,
        on_applied: Optional[callable] = None,  # ★ UI 이벤트 큐로 신호 넣기 위한 콜백
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
        self.loop: asyncio.AbstractEventLoop | None = None  # 이벤트 루프 참조
        self.on_applied = on_applied  # ★ UI에 알림 (스레드 세이프 Callable 권장)

    async def start(self):
        if self._running:
            return
        self.loop = asyncio.get_running_loop()
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        self._running = True
        logger.info(f"Node started at {self.host}:{self.port} (node_id={self.node_id})")
        # 등록된 피어들에 접속 시도
        for h, p in list(self._peers):
            self._tasks.add(self.loop.create_task(self._connect_to_peer(h, p)))

    async def stop(self):
        self._running = False

        # writers 정리
        for w in list(self._writers):
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass
        self._writers.clear()

        # 서버 닫기
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # 태스크 취소
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()

        logger.info("Node stopped.")

    def add_peer(self, host: str, port: int):
        """피어 목록에 추가하고, 실행 중이면 즉시 연결"""
        self._peers.add((host, int(port)))
        if self._running and self.loop:
            self._tasks.add(
                self.loop.create_task(self._connect_to_peer(host, int(port)))
            )
        logger.info(f"Peer added: {host}:{port}")

    async def _connect_to_peer(self, host: str, port: int):
        """Outgoing connection: writer 등록 + reader 루프 시작"""
        try:
            reader, writer = await asyncio.open_connection(host, port)
            self._writers.add(writer)

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

            # Outgoing connection에서도 읽기 루프를 돌려 상대의 브로드캐스트 수신 처리
            task = asyncio.create_task(self._read_loop(reader, writer))
            self._tasks.add(task)
        except Exception as e:
            logger.warning(f"Failed to connect to peer {host}:{port}: {e}")

    async def _read_loop(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """공통 읽기 루프: incoming/outgoing 연결 모두 사용"""
        addr = writer.get_extra_info("peername")
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

                # Token check
                if msg.get("token") != self.token:
                    logger.warning("Rejected message due to invalid token")
                    continue

                mtype = msg["type"]
                if mtype == "hello":
                    # ack
                    ack = {
                        "type": "ack",
                        "payload": {"status": "ok"},
                        "ts": int(time.time()),
                        "node_id": self.node_id,
                        "token": self.token,
                    }
                    await self._send_line(writer, ack)
                    continue

                # Apply LWW
                apply_message(self.db_path, self.node_id, msg)

                # ★ UI에 즉시 알림 (스레드 세이프 콜백 권장: Queue.put 등)
                try:
                    if self.on_applied:
                        self.on_applied(msg)  # UI 측에서 Queue에 넣고 after로 처리
                except Exception as e:
                    logger.warning(f"on_applied notify failed: {e}")

                # Ack (선택)
                ack = {
                    "type": "ack",
                    "payload": {"applied_type": mtype},
                    "ts": int(time.time()),
                    "node_id": self.node_id,
                    "token": self.token,
                }
                await self._send_line(writer, ack)
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
            logger.info(f"Connection closed from {addr}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Incoming connection: 바로 읽기 루프로 넘김"""
        addr = writer.get_extra_info("peername")
        logger.info(f"Incoming connection from {addr}")
        self._writers.add(writer)
        task = asyncio.create_task(self._read_loop(reader, writer))
        self._tasks.add(task)

    async def _send_line(self, writer: asyncio.StreamWriter, msg: Dict[str, Any]):
        """단일 메시지 전송 (루프 스레드에서만 호출)"""
        try:
            data = dumps_line(msg).encode("utf-8")
            writer.write(data)
            await writer.drain()
        except Exception as e:
            logger.warning(f"Send failed: {e}")

    async def _broadcast_async(self, msg: Dict[str, Any]):
        """루프 스레드 내에서 안전하게 브로드캐스트"""
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
        """
        스레드 안전 브로드캐스트:
        - UI/DB 스레드 등 어떤 스레드에서 호출해도 안전.
        - 실제 전송은 이벤트 루프 스레드에서 수행.
        """
        if not self._running or not self.loop:
            return
        try:
            self.loop.call_soon_threadsafe(
                lambda: asyncio.create_task(self._broadcast_async(msg))
            )
        except Exception as e:
            logger.exception(f"Thread-safe broadcast schedule failed: {e}")
