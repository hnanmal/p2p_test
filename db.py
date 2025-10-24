# db.py
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from contextlib import contextmanager

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

from models import Base, WBSItem, RevitTypeMappingItem, CalculationRuleItem

logger = logging.getLogger("db")

# Engine/Session 캐시
_engines: Dict[str, Any] = {}
_sessionmakers: Dict[str, sessionmaker] = {}

# --- 변경 감지 카운터 (UI 폴링용) ---
_change_seq = 0
_last_change_ts = 0


def _bump_change_seq(ts: int):
    """DB 커밋 성공 직후 호출: UI 자동 갱신 트리거용 카운터"""
    global _change_seq, _last_change_ts
    _change_seq += 1
    _last_change_ts = int(ts)


def get_change_seq() -> int:
    """UI가 주기적으로 폴링해 변경 여부를 감지"""
    return _change_seq


def get_last_change_ts() -> int:
    return _last_change_ts


# --- 네트워크 브로드캐스트 콜백 및 억제기 ---
_change_callback: Optional[Callable[[Dict[str, Any]], None]] = None
_suppress_events: int = 0  # 중첩 가능


def set_change_callback(cb: Optional[Callable[[Dict[str, Any]], None]]):
    global _change_callback
    _change_callback = cb
    logger.info("DB change callback registered" if cb else "DB change callback cleared")


@contextmanager
def suppress_events():
    global _suppress_events
    _suppress_events += 1
    try:
        yield
    finally:
        _suppress_events -= 1


def _trigger_change(msg_type: str, payload: Dict[str, Any], ts: int):
    """DB 변경 → 브로드캐스트. suppress_events 중엔 송신 안 함.
    (주의) UI 자동 리프레시는 _bump_change_seq로 항상 동작함."""
    if _suppress_events > 0:
        return
    if _change_callback:
        event = {"type": msg_type, "payload": payload, "ts": ts}
        try:
            _change_callback(event)
        except Exception as e:
            logger.exception(f"Change callback error: {e}")


# --- 세션/엔진 ---
def _get_sessionmaker(db_path: Path) -> sessionmaker:
    key = str(db_path.resolve())
    sm = _sessionmakers.get(key)
    if sm is None:
        engine = create_engine(f"sqlite:///{db_path}", echo=False, future=True)
        Base.metadata.create_all(engine)
        sm = sessionmaker(
            bind=engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            future=True,
        )
        _engines[key] = engine
        _sessionmakers[key] = sm
        logger.info(f"Initialized SQLAlchemy engine/session for {db_path}")
    return sm


def get_session(db_path: Path) -> Session:
    sm = _get_sessionmaker(db_path)
    return sm()


def init_db(db_path: Path, schema_path: Optional[Path] = None):
    _get_sessionmaker(db_path)
    logger.info("Database initialized with SQLAlchemy ORM.")


# --- 공통 유틸 ---
def _now_ts() -> int:
    return int(time.time())


def _wbs_dict(obj: WBSItem) -> Dict[str, Any]:
    return {
        "wbs_code": obj.wbs_code,
        "description": obj.description or "",
        "updated_ts": int(obj.updated_ts),
    }


def _map_dict(obj: RevitTypeMappingItem) -> Dict[str, Any]:
    return {
        "id": int(obj.id) if obj.id is not None else None,
        "type_name": obj.type_name,
        "wbs_code": obj.wbs_code,
        "updated_ts": int(obj.updated_ts),
    }


def _rule_dict(obj: CalculationRuleItem) -> Dict[str, Any]:
    return {
        "wbs_code": obj.wbs_code,
        "rule_type": obj.rule_type or "",
        "formula": obj.formula or "",
        "unit": obj.unit or "",
        "updated_ts": int(obj.updated_ts),
    }


# ---------- WBS ----------
def list_wbs(db_path: Path) -> List[Dict[str, Any]]:
    with get_session(db_path) as s:
        rows = s.execute(select(WBSItem).order_by(WBSItem.wbs_code)).scalars().all()
        return [_wbs_dict(r) for r in rows]


def upsert_wbs(
    db_path: Path,
    wbs_code: str,
    description: str,
    updated_ts: Optional[int] = None,
    force_update: bool = False,
):
    ts = int(updated_ts) if updated_ts is not None else _now_ts()
    with get_session(db_path) as s:
        obj = s.get(WBSItem, wbs_code)
        if obj:
            if force_update or ts > (obj.updated_ts or 0):
                obj.description = description
                obj.updated_ts = ts
                s.commit()
                logger.info(f"Upsert WBS(updated): {wbs_code} (ts={ts})")
                _bump_change_seq(ts)  # ★ 자동 리프레시 트리거
                _trigger_change(
                    "upsert_wbs", {"wbs_code": wbs_code, "description": description}, ts
                )
            else:
                logger.info(
                    f"Upsert WBS(skipped LWW): {wbs_code} (incoming ts={ts} <= existing ts={obj.updated_ts})"
                )
        else:
            s.add(WBSItem(wbs_code=wbs_code, description=description, updated_ts=ts))
            s.commit()
            logger.info(f"Upsert WBS(created): {wbs_code} (ts={ts})")
            _bump_change_seq(ts)  # ★
            _trigger_change(
                "upsert_wbs", {"wbs_code": wbs_code, "description": description}, ts
            )


def delete_wbs(db_path: Path, wbs_code: str):
    with get_session(db_path) as s:
        obj = s.get(WBSItem, wbs_code)
        if obj:
            ts = _now_ts()
            s.delete(obj)
            s.commit()
            logger.info(f"Delete WBS: {wbs_code}")
            _bump_change_seq(ts)  # ★
            _trigger_change("delete_wbs", {"wbs_code": wbs_code}, ts)
        else:
            logger.info(f"Delete WBS: not found ({wbs_code})")


# ---------- Revit Type Mapping ----------
def list_mappings(db_path: Path) -> List[Dict[str, Any]]:
    with get_session(db_path) as s:
        rows = (
            s.execute(
                select(RevitTypeMappingItem).order_by(
                    RevitTypeMappingItem.type_name, RevitTypeMappingItem.wbs_code
                )
            )
            .scalars()
            .all()
        )
        return [_map_dict(r) for r in rows]


def upsert_mapping(
    db_path: Path,
    type_name: str,
    wbs_code: str,
    updated_ts: Optional[int] = None,
    force_update: bool = False,
):
    ts = int(updated_ts) if updated_ts is not None else _now_ts()
    with get_session(db_path) as s:
        obj = (
            s.execute(
                select(RevitTypeMappingItem).where(
                    RevitTypeMappingItem.type_name == type_name,
                    RevitTypeMappingItem.wbs_code == wbs_code,
                )
            )
            .scalars()
            .first()
        )
        if obj:
            if force_update or ts > (obj.updated_ts or 0):
                obj.updated_ts = ts
                s.commit()
                logger.info(
                    f"Upsert Mapping(updated): {type_name} -> {wbs_code} (ts={ts})"
                )
                _bump_change_seq(ts)  # ★
                _trigger_change(
                    "upsert_mapping", {"type_name": type_name, "wbs_code": wbs_code}, ts
                )
            else:
                logger.info(
                    f"Upsert Mapping(skipped LWW): {type_name} -> {wbs_code} (incoming ts={ts} <= existing ts={obj.updated_ts})"
                )
        else:
            s.add(
                RevitTypeMappingItem(
                    type_name=type_name, wbs_code=wbs_code, updated_ts=ts
                )
            )
            s.commit()
            logger.info(f"Upsert Mapping(created): {type_name} -> {wbs_code} (ts={ts})")
            _bump_change_seq(ts)  # ★
            _trigger_change(
                "upsert_mapping", {"type_name": type_name, "wbs_code": wbs_code}, ts
            )


def delete_mapping(db_path: Path, type_name: str, wbs_code: str):
    with get_session(db_path) as s:
        obj = (
            s.execute(
                select(RevitTypeMappingItem).where(
                    RevitTypeMappingItem.type_name == type_name,
                    RevitTypeMappingItem.wbs_code == wbs_code,
                )
            )
            .scalars()
            .first()
        )
        if obj:
            ts = _now_ts()
            s.delete(obj)
            s.commit()
            logger.info(f"Delete Mapping: {type_name} | {wbs_code}")
            _bump_change_seq(ts)  # ★
            _trigger_change(
                "delete_mapping", {"type_name": type_name, "wbs_code": wbs_code}, ts
            )
        else:
            logger.info(f"Delete Mapping: not found ({type_name} | {wbs_code})")


# ---------- Calculation Rules ----------
def list_rules(db_path: Path) -> List[Dict[str, Any]]:
    with get_session(db_path) as s:
        rows = (
            s.execute(
                select(CalculationRuleItem).order_by(CalculationRuleItem.wbs_code)
            )
            .scalars()
            .all()
        )
        return [_rule_dict(r) for r in rows]


def upsert_rule(
    db_path: Path,
    wbs_code: str,
    rule_type: str,
    formula: str,
    unit: str,
    updated_ts: Optional[int] = None,
    force_update: bool = False,
):
    ts = int(updated_ts) if updated_ts is not None else _now_ts()
    with get_session(db_path) as s:
        obj = s.get(CalculationRuleItem, wbs_code)
        if obj:
            if force_update or ts > (obj.updated_ts or 0):
                obj.rule_type = rule_type
                obj.formula = formula
                obj.unit = unit
                obj.updated_ts = ts
                s.commit()
                logger.info(f"Upsert Rule(updated): {wbs_code} (ts={ts})")
                _bump_change_seq(ts)  # ★
                _trigger_change(
                    "upsert_rule",
                    {
                        "wbs_code": wbs_code,
                        "rule_type": rule_type,
                        "formula": formula,
                        "unit": unit,
                    },
                    ts,
                )
            else:
                logger.info(
                    f"Upsert Rule(skipped LWW): {wbs_code} (incoming ts={ts} <= existing ts={obj.updated_ts})"
                )
        else:
            s.add(
                CalculationRuleItem(
                    wbs_code=wbs_code,
                    rule_type=rule_type,
                    formula=formula,
                    unit=unit,
                    updated_ts=ts,
                )
            )
            s.commit()
            logger.info(f"Upsert Rule(created): {wbs_code} (ts={ts})")
            _bump_change_seq(ts)  # ★
            _trigger_change(
                "upsert_rule",
                {
                    "wbs_code": wbs_code,
                    "rule_type": rule_type,
                    "formula": formula,
                    "unit": unit,
                },
                ts,
            )


def delete_rule(db_path: Path, wbs_code: str):
    with get_session(db_path) as s:
        obj = s.get(CalculationRuleItem, wbs_code)
        if obj:
            ts = _now_ts()
            s.delete(obj)
            s.commit()
            logger.info(f"Delete Rule: {wbs_code}")
            _bump_change_seq(ts)  # ★
            _trigger_change("delete_rule", {"wbs_code": wbs_code}, ts)
        else:
            logger.info(f"Delete Rule: not found ({wbs_code})")
