# sync/snapshot.py
import logging
import time
from pathlib import Path
from typing import Dict, Any, List

from db import (
    list_wbs,
    list_mappings,
    list_rules,
    list_tombstones,
    upsert_wbs,
    upsert_mapping,
    upsert_rule,
    delete_wbs,
    delete_mapping,
    delete_rule,
    get_session,
)
from models import WBSItem, RevitTypeMappingItem, CalculationRuleItem

logger = logging.getLogger("sync.snapshot")


def build_snapshot(db_path: Path) -> Dict[str, Any]:
    """
    현재 DB 상태 스냅샷 구성.
    - entity 리스트 + tombstones(삭제 이벤트)
    """
    return {
        "wbs_list": list_wbs(db_path),
        "revit_type_mapping": list_mappings(db_path),
        "calculation_rules": list_rules(db_path),
        "tombstones": list_tombstones(db_path),  # [{entity, key, deleted_ts}]
    }


def _current_ts(db_path: Path, entity: str, key: Dict[str, Any]) -> int:
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


def _should_apply(in_ts: int, ex_ts: int, in_node: str, local_node: str) -> bool:
    if in_ts > ex_ts:
        return True
    if in_ts < ex_ts:
        return False
    # tie: node_id lexicographic
    return in_node > local_node


def apply_snapshot(
    db_path: Path, local_node_id: str, remote_node_id: str, snapshot: Dict[str, Any]
):
    """
    수신한 스냅샷을 LWW 규칙으로 병합.
    - insert/update: updated_ts 비교 + tie-breaker
    - tombstone(삭제): deleted_ts 비교 + tie-breaker → delete_*
    """
    # --- UPSERTS ---
    for item in snapshot.get("wbs_list", []):
        ts = int(item.get("updated_ts", int(time.time())))
        key = {"wbs_code": item["wbs_code"]}
        ex_ts = _current_ts(db_path, "wbs_list", key)
        force = _should_apply(ts, ex_ts, remote_node_id, local_node_id)
        upsert_wbs(
            db_path,
            item["wbs_code"],
            item.get("description", ""),
            updated_ts=ts,
            force_update=force,
        )

    for item in snapshot.get("revit_type_mapping", []):
        ts = int(item.get("updated_ts", int(time.time())))
        key = {"type_name": item["type_name"], "wbs_code": item["wbs_code"]}
        ex_ts = _current_ts(db_path, "revit_type_mapping", key)
        force = _should_apply(ts, ex_ts, remote_node_id, local_node_id)
        upsert_mapping(
            db_path,
            item["type_name"],
            item["wbs_code"],
            updated_ts=ts,
            force_update=force,
        )

    for item in snapshot.get("calculation_rules", []):
        ts = int(item.get("updated_ts", int(time.time())))
        key = {"wbs_code": item["wbs_code"]}
        ex_ts = _current_ts(db_path, "calculation_rules", key)
        force = _should_apply(ts, ex_ts, remote_node_id, local_node_id)
        upsert_rule(
            db_path,
            item["wbs_code"],
            item.get("rule_type", ""),
            item.get("formula", ""),
            item.get("unit", ""),
            updated_ts=ts,
            force_update=force,
        )

    # --- TOMBSTONES ---
    for t in snapshot.get("tombstones", []):
        entity = t.get("entity")
        key_str = t.get("key", "")
        del_ts = int(t.get("deleted_ts", int(time.time())))

        if entity == "wbs_list":
            key = {"wbs_code": key_str}
            ex_ts = _current_ts(db_path, "wbs_list", key)
            if _should_apply(del_ts, ex_ts, remote_node_id, local_node_id):
                delete_wbs(db_path, key_str)

        elif entity == "revit_type_mapping":
            # key format: 'type_name|wbs_code'
            try:
                type_name, wbs_code = key_str.split("|", 1)
            except ValueError:
                logger.warning(f"Invalid tombstone key format: {key_str}")
                continue
            key = {"type_name": type_name, "wbs_code": wbs_code}
            ex_ts = _current_ts(db_path, "revit_type_mapping", key)
            if _should_apply(del_ts, ex_ts, remote_node_id, local_node_id):
                delete_mapping(db_path, type_name, wbs_code)

        elif entity == "calculation_rules":
            key = {"wbs_code": key_str}
            ex_ts = _current_ts(db_path, "calculation_rules", key)
            if _should_apply(del_ts, ex_ts, remote_node_id, local_node_id):
                delete_rule(db_path, key_str)

    logger.info("Snapshot applied (LWW + tombstones)")
