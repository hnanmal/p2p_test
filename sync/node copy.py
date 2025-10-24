# sync/node.py
import asyncio
import logging
from typing import Dict, Any, Set, Tuple

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

    async def start(self):
        if self._running:
            return
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        self._running = True
        logger.info(f"Node started at {self.host}:{self.port} (node_id={self.node_id})")
        # 연결된 피어들에 접속 시도
        for h, p in list(self._peers):
            asyncio.create_task(self._connect_to_peer(h, p))

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
        if self._running:
            asyncio.create_task(self._connect_to_peer(host, int(port)))
        logger.info(f"Peer added: {host}:{port}")

    async def _connect_to_peer(self, host: str, port: int):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            self._writers.add(writer)
            # handshake
            hello = {
                "type": "hello",
                "payload": {},
                "ts": int(asyncio.get_event_loop().time()),
                "node_id": self.node_id,
                "token": self.token,
            }
            writer.write(dumps_line(hello).encode("utf-8"))
            await writer.drain()
            logger.info(f"Connected to peer {host}:{port}")
            # keep reader task for remote peer?
            # Outgoing connection: we won't read here; we only write. For simplicity, ignore inbound on outgoing.
        except Exception as e:
            logger.warning(f"Failed to connect to peer {host}:{port}: {e}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        logger.info(f"Incoming connection from {addr}")
        self._writers.add(writer)
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
                    logger.info(f"HELLO from {msg.get('node_id')}")
                    # reply ack
                    ack = {
                        "type": "ack",
                        "payload": {"status": "ok"},
                        "ts": int(asyncio.get_event_loop().time()),
                        "node_id": self.node_id,
                        "token": self.token,
                    }
                    writer.write(dumps_line(ack).encode("utf-8"))
                    await writer.drain()
                    continue

                # Apply LWW
                apply_message(self.db_path, self.node_id, msg)

                # Send ACK
                ack = {
                    "type": "ack",
                    "payload": {"applied_type": mtype},
                    "ts": int(asyncio.get_event_loop().time()),
                    "node_id": self.node_id,
                    "token": self.token,
                }
                writer.write(dumps_line(ack).encode("utf-8"))
                await writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Client handler error: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            if writer in self._writers:
                self._writers.remove(writer)
            logger.info(f"Connection closed from {addr}")

    def broadcast(self, msg: Dict[str, Any]):
        """현재 보유한 모든 writer로 메시지 송신"""
        if not self._running:
            return
        data = dumps_line(msg).encode("utf-8")
        dead = []
        for w in list(self._writers):
            try:
                w.write(data)
            except Exception:
                dead.append(w)
        for w in dead:
            try:
                w.close()
            except Exception:
                pass
            self._writers.discard(w)
