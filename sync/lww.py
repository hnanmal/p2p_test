# sync/lww.py
import logging
from pathlib import Path
from typing import Dict, Any

from db import (
    upsert_wbs,
    upsert_mapping,
    upsert_rule,
    delete_wbs,
    delete_mapping,
    delete_rule,
    get_session,
    suppress_events,
)
from models import WBSItem, RevitTypeMappingItem, CalculationRuleItem

logger = logging.getLogger("sync.lww")


def _current_ts_for_key(db_path: Path, entity: str, key: Dict[str, Any]) -> int:
    """현재 레코드의 updated_ts 조회 (없으면 0)"""
    with get_session(db_path) as s:
        if entity == "wbs_list":
            obj = s.get(WBSItem, key["wbs_code"])
            return int(obj.updated_ts) if obj else 0
        elif entity == "revit_type_mapping":
            obj = (
                s.query(RevitTypeMappingItem)
                .filter_by(type_name=key["type_name"], wbs_code=key["wbs_code"])
                .first()
            )
            return int(obj.updated_ts) if obj else 0
        elif entity == "calculation_rules":
            obj = s.get(CalculationRuleItem, key["wbs_code"])
            return int(obj.updated_ts) if obj else 0
        else:
            return 0


def should_apply(
    incoming_ts: int, existing_ts: int, incoming_node_id: str, local_node_id: str
) -> bool:
    if incoming_ts > existing_ts:
        return True
    if incoming_ts < existing_ts:
        return False
    # 동률: node_id 사전순 타이브레이커
    return incoming_node_id > local_node_id


def apply_message(db_path: Path, local_node_id: str, msg: Dict[str, Any]):
    """
    수신 NLJSON 메시지를 LWW 규칙으로 DB에 반영.
    - 동률 ts: incoming node_id > local_node_id 일 때 적용 (force_update)
    - 네트워크 루프 방지: suppress_events로 재브로드캐스트 차단
    """
    mtype = msg["type"]
    payload = msg["payload"]
    ts = int(msg["ts"])
    remote_node_id = msg["node_id"]

    # 키/엔티티 정의
    if mtype in ("upsert_wbs", "delete_wbs"):
        entity = "wbs_list"
        key = {"wbs_code": payload["wbs_code"]}
    elif mtype in ("upsert_mapping", "delete_mapping"):
        entity = "revit_type_mapping"
        key = {"type_name": payload["type_name"], "wbs_code": payload["wbs_code"]}
    elif mtype in ("upsert_rule", "delete_rule"):
        entity = "calculation_rules"
        key = {"wbs_code": payload["wbs_code"]}
    else:
        logger.warning(f"Unknown message type: {mtype}")
        return

    existing_ts = _current_ts_for_key(db_path, entity, key)
    force = should_apply(ts, existing_ts, remote_node_id, local_node_id)

    with suppress_events():
        if mtype == "upsert_wbs":
            upsert_wbs(
                db_path,
                payload["wbs_code"],
                payload.get("description", ""),
                updated_ts=ts,
                force_update=force,
            )
        elif mtype == "delete_wbs":
            # 삭제는 최신만 허용
            if force:
                delete_wbs(db_path, payload["wbs_code"])
        elif mtype == "upsert_mapping":
            upsert_mapping(
                db_path,
                payload["type_name"],
                payload["wbs_code"],
                updated_ts=ts,
                force_update=force,
            )
        elif mtype == "delete_mapping":
            if force:
                delete_mapping(db_path, payload["type_name"], payload["wbs_code"])
        elif mtype == "upsert_rule":
            upsert_rule(
                db_path,
                payload["wbs_code"],
                payload.get("rule_type", ""),
                payload.get("formula", ""),
                payload.get("unit", ""),
                updated_ts=ts,
                force_update=force,
            )
        elif mtype == "delete_rule":
            if force:
                delete_rule(db_path, payload["wbs_code"])

    logger.info(
        f"Applied [{mtype}] with ts={ts} (existing_ts={existing_ts}, force={force})"
    )
