# models.py
from sqlalchemy import Column, String, Integer, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class WBSItem(Base):
    __tablename__ = "wbs_list"
    wbs_code = Column(String, primary_key=True)
    description = Column(String)
    updated_ts = Column(Integer, nullable=False)


class RevitTypeMappingItem(Base):
    __tablename__ = "revit_type_mapping"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type_name = Column(String, nullable=False)
    wbs_code = Column(String, nullable=False)
    updated_ts = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint("type_name", "wbs_code", name="uq_type_wbs"),)


class CalculationRuleItem(Base):
    __tablename__ = "calculation_rules"
    wbs_code = Column(String, primary_key=True)
    rule_type = Column(String)
    formula = Column(String)
    unit = Column(String)
    updated_ts = Column(Integer, nullable=False)
