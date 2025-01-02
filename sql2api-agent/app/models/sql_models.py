from dataclasses import dataclass
from typing import List, Optional

@dataclass
class TableInfo:
    """表信息对象"""
    table: str  # 表名
    alias: str  # 表别名

@dataclass
class SelectField:
    """查询字段对象"""
    table: str       # 表名/别名
    column: str      # 字段名
    name: str        # 别名（AS后的名称）

@dataclass
class WhereCondition:
    """WHERE条件对象"""
    table: str       # 表名/别名
    column: str      # 字段名
    value: str       # 条件值
    operator: str    # 操作符类型 ('=', 'LIKE', 'IN', 'order by')

@dataclass
class JoinCondition:
    """JOIN条件对象"""
    leftTable: str    # 左表名/别名
    leftColumn: str   # 左表字段
    rightTable: str   # 右表名/别名
    rightColumn: str  # 右表字段
    sequence: int     # JOIN序号，标识JOIN的顺序关系


@dataclass
class OrderByCondition:
    """排序条件对象"""
    column: str      # 排序字段名
    type: str       # 排序类型(DESC/ASC)

@dataclass
class SQLParseResult:
    """SQL解析结果对象"""
    tables: List[TableInfo]
    fields: List[SelectField]
    where_conditions: List[WhereCondition]
    join_conditions: List[JoinCondition]
