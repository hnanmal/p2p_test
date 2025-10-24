# sync/broadcaster.py
import logging
from typing import Dict, Any
from pathlib import Path

from .protocol import dumps_line  # not strictly needed; for formatting previews
from db import set_change_callback

logger = logging.getLogger("sync.broadcaster")


class Broadcaster:
    def __init__(self, db_path: Path, node, node_id: str, token: str):
        self.db_path = db_path
        self.node = node
        self.node_id = node_id
        self.token = token
        self._attached = False

    def attach(self):
        if self._attached:
            return

        def _on_change(event: Dict[str, Any]):
            msg = {
                "type": event["type"],
                "payload": event["payload"],
                "ts": int(event["ts"]),
                "node_id": self.node_id,
                "token": self.token,
            }
            self.node.broadcast(msg)
            logger.info(f"Broadcasted: {msg['type']} ts={msg['ts']}")

        set_change_callback(_on_change)
        self._attached = True
        logger.info("Broadcaster attached and listening for DB changes")

    def detach(self):
        if not self._attached:
            return
        set_change_callback(None)
        self._attached = False
        logger.info("Broadcaster detached")
