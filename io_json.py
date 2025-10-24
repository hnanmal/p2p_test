# io_json.py
import json
import time
import logging
from pathlib import Path
from typing import Dict, Any, Tuple

from db import (
    list_wbs,
    list_mappings,
    list_rules,
    upsert_wbs,
    upsert_mapping,
    upsert_rule,
)

logger = logging.getLogger("io_json")

VERSION = 1


def _ts_iso(ts: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def export_snapshot(db_path: Path, project: str, out_dir: Path) -> Path:
    now_ts = int(time.time())
    exported_at = _ts_iso(now_ts)
    data = {
        "project": project,
        "version": VERSION,
        "exported_at": exported_at,
        "wbs_list": list_wbs(db_path),
        "revit_type_mapping": list_mappings(db_path),
        "calculation_rules": list_rules(db_path),
    }
    fname = (
        f"rules_{project}_{time.strftime('%Y%m%d-%H%M%S', time.localtime(now_ts))}.json"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / fname
    out_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"Exported snapshot: {out_path}")
    return out_path


def import_snapshot(db_path: Path, json_path: Path) -> Tuple[int, int, int]:
    raw = json_path.read_text(encoding="utf-8")
    data = json.loads(raw)

    if not isinstance(data, dict) or "version" not in data:
        raise ValueError("Invalid JSON: missing 'version'")

    now_ts = int(time.time())

    wbs_count = 0
    for item in data.get("wbs_list", []):
        wbs_code = item["wbs_code"]
        description = item.get("description", "")
        updated_ts = int(item.get("updated_ts", now_ts))
        upsert_wbs(db_path, wbs_code, description, updated_ts)
        wbs_count += 1

    map_count = 0
    for item in data.get("revit_type_mapping", []):
        type_name = item["type_name"]
        wbs_code = item.get("wbs_code", "")
        updated_ts = int(item.get("updated_ts", now_ts))
        upsert_mapping(db_path, type_name, wbs_code, updated_ts)
        map_count += 1

    rule_count = 0
    for item in data.get("calculation_rules", []):
        wbs_code = item["wbs_code"]
        rule_type = item.get("rule_type", "")
        formula = item.get("formula", "")
        unit = item.get("unit", "")
        updated_ts = int(item.get("updated_ts", now_ts))
        upsert_rule(db_path, wbs_code, rule_type, formula, unit, updated_ts)
        rule_count += 1

    logger.info(f"Imported: WBS={wbs_count}, Mappings={map_count}, Rules={rule_count}")
    return wbs_count, map_count, rule_count
