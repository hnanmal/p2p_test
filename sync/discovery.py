# sync/discovery.py
import asyncio
import json
import logging
import socket
import struct
import time
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger("sync.discovery")


class _DiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self, handler):
        super().__init__()
        self._handler = handler

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        try:
            msg = json.loads(data.decode("utf-8", errors="ignore"))
        except Exception:
            logger.debug("Discovery: invalid JSON datagram")
            return
        try:
            self._handler(msg, addr)
        except Exception as e:
            logger.exception(f"Discovery handler error: {e}")


class Discovery:
    """
    안정 버전:
    - 수신: 멀티캐스트 그룹(239.255.255.250:<port>) 가입 시도 → 실패하면 브로드캐스트 수신으로 폴백
    - 송신: 멀티캐스트 + 브로드캐스트 동시 송신 (둘 중 하나라도 네트워크에서 허용되면 작동)
    - 토큰 필터링 / 자기 자신 제외 / 중복 노화(60s) / node.add_peer 자동 호출
    """

    def __init__(
        self,
        node,
        token: str,
        port: int = 9999,
        interval: float = 3.0,
        group: str = "239.255.255.250",
        iface_ip: Optional[str] = None,  # 필요 시 특정 NIC IP 강제 (예: "192.168.0.21")
        ttl: int = 1,
        loopback: int = 1,  # 동일 호스트 내 프로세스 상호 발견 허용
    ):
        self.node = node
        self.token = token
        self.port = int(port)
        self.interval = float(interval)
        self.group = group
        self.iface_ip = iface_ip
        self.ttl = int(ttl)
        self.loopback = int(loopback)

        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.transport: Optional[asyncio.transports.DatagramTransport] = None
        self._announce_task: Optional[asyncio.Task] = None

        self._send_sock_mc: Optional[socket.socket] = None
        self._send_sock_bc: Optional[socket.socket] = None

        # (host, port) -> last_seen_ts
        self._known: Dict[Tuple[str, int], float] = {}

        # 수신 방식: "multicast" or "broadcast"
        self.recv_mode: str = "multicast"

    def _make_recv_sock_multicast(self) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, self.loopback)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.ttl)
        s.bind(("", self.port))
        mreq = struct.pack(
            "=4s4s",
            socket.inet_aton(self.group),
            socket.inet_aton(self.iface_ip or "0.0.0.0"),
        )
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        if self.iface_ip:
            try:
                s.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_MULTICAST_IF,
                    socket.inet_aton(self.iface_ip),
                )
            except Exception:
                pass
        s.setblocking(False)
        return s

    def _make_recv_sock_broadcast(self) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass
        # 브로드캐스트 수신은 별도 옵션 불필요
        s.bind(("", self.port))
        s.setblocking(False)
        return s

    def _make_send_sock_multicast(self) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, self.loopback)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.ttl)
        if self.iface_ip:
            try:
                s.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_MULTICAST_IF,
                    socket.inet_aton(self.iface_ip),
                )
            except Exception:
                pass
        s.setblocking(False)
        return s

    def _make_send_sock_broadcast(self) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass
        s.setblocking(False)
        return s

    async def start(self):
        self.loop = asyncio.get_running_loop()
        # 수신: 멀티캐스트 시도 → 실패 시 브로드캐스트로 폴백
        try:
            recv_sock = self._make_recv_sock_multicast()
            self.transport, _ = await self.loop.create_datagram_endpoint(
                lambda: _DiscoveryProtocol(self._handle_incoming),
                sock=recv_sock,
            )
            self.recv_mode = "multicast"
            logger.info(
                f"Discovery recv: multicast joined {self.group}:{self.port} (iface={self.iface_ip or 'ANY'})"
            )
        except Exception as e:
            logger.warning(
                f"Discovery multicast join failed, fallback to broadcast recv: {e}"
            )
            recv_sock = self._make_recv_sock_broadcast()
            self.transport, _ = await self.loop.create_datagram_endpoint(
                lambda: _DiscoveryProtocol(self._handle_incoming),
                sock=recv_sock,
            )
            self.recv_mode = "broadcast"
            logger.info(f"Discovery recv: broadcast bound 0.0.0.0:{self.port}")

        # 송신 소켓 준비 (둘 다)
        try:
            self._send_sock_mc = self._make_send_sock_multicast()
            logger.debug("Discovery send: multicast socket ready")
        except Exception as e:
            logger.warning(f"Discovery multicast send socket failed: {e}")
            self._send_sock_mc = None

        try:
            self._send_sock_bc = self._make_send_sock_broadcast()
            logger.debug("Discovery send: broadcast socket ready")
        except Exception as e:
            logger.warning(f"Discovery broadcast send socket failed: {e}")
            self._send_sock_bc = None

        self._announce_task = self.loop.create_task(self._announce_loop())
        logger.info(
            f"Discovery started (interval={self.interval}s, loopback={self.loopback}, ttl={self.ttl})"
        )

    async def stop(self):
        if self._announce_task:
            try:
                self._announce_task.cancel()
                await asyncio.gather(self._announce_task, return_exceptions=True)
            except Exception:
                pass
            self._announce_task = None

        try:
            if self._send_sock_mc:
                self._send_sock_mc.close()
        except Exception:
            pass
        self._send_sock_mc = None

        try:
            if self._send_sock_bc:
                self._send_sock_bc.close()
        except Exception:
            pass
        self._send_sock_bc = None

        if self.transport:
            try:
                self.transport.close()
            except Exception:
                pass
            self.transport = None

        logger.info("Discovery stopped")

    async def _announce_loop(self):
        while True:
            await self.send_discover_now()
            await asyncio.sleep(self.interval)

    async def send_discover_now(self):
        msg = {
            "type": "discover",
            "node_id": self.node.node_id,
            "port": self.node.port,
            "token": self.token,
            "ts": int(time.time()),
        }
        data = json.dumps(msg, ensure_ascii=False).encode("utf-8")

        # 멀티캐스트 송신
        if self._send_sock_mc:
            try:
                await self.loop.sock_sendto(
                    self._send_sock_mc, data, (self.group, self.port)
                )
                logger.debug("Discovery: sent DISCOVER (multicast)")
            except Exception as e:
                logger.warning(f"Discovery multicast send failed: {e}")

        # 브로드캐스트 송신
        if self._send_sock_bc:
            try:
                await self.loop.sock_sendto(
                    self._send_sock_bc, data, ("255.255.255.255", self.port)
                )
                logger.debug("Discovery: sent DISCOVER (broadcast)")
            except Exception as e:
                logger.warning(f"Discovery broadcast send failed: {e}")

    def _handle_incoming(self, msg: Dict[str, Any], addr: Tuple[str, int]):
        # 검증
        if not isinstance(msg, dict) or msg.get("type") != "discover":
            return
        if msg.get("token") != self.token:
            logger.debug("Discovery: token mismatch, ignored")
            return
        remote_node_id = str(msg.get("node_id", ""))
        remote_port = int(msg.get("port", 0))
        remote_host = addr[0]

        # 자기 자신 제외
        if remote_node_id == self.node.node_id and remote_port == self.node.port:
            return

        key = (remote_host, remote_port)
        now = time.time()

        # 60초 내 중복 발견 방지
        last = self._known.get(key, 0)
        if last and (now - last) < 60.0:
            return

        self._known[key] = now
        logger.info(
            f"Discovery: found peer {remote_host}:{remote_port} (node_id={remote_node_id}, via={self.recv_mode})"
        )
        self.node.add_peer(remote_host, remote_port)
