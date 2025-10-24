# sync/protocol.py
import json
from typing import Dict, Any

REQUIRED_COMMON = ["type", "payload", "ts", "node_id", "token"]


def validate_message(msg: Dict[str, Any]) -> bool:
    for k in REQUIRED_COMMON:
        if k not in msg:
            return False
    if not isinstance(msg["type"], str):
        return False
    if not isinstance(msg["payload"], dict):
        return False
    if not isinstance(msg["ts"], int):
        return False
    if not isinstance(msg["node_id"], str):
        return False
    if not isinstance(msg["token"], str):
        return False
    return True


def dumps_line(msg: Dict[str, Any]) -> str:
    """NLJSON: 한 줄당 하나의 JSON 메시지"""
    return json.dumps(msg, ensure_ascii=False) + "\n"


def loads_line(line: str) -> Dict[str, Any]:
    return json.loads(line)
