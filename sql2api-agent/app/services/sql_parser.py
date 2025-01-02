from typing import Dict, List, Any, Optional
import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Token, Parenthesis
from sqlparse.tokens import Keyword, DML
from app.core.logger import logger
from app.models.sql_models import (
    TableInfo,
    SelectField,
    WhereCondition,
    JoinCondition,
    SQLParseResult
)

class SQLParser:
    def parse_sql(self, sql: str) -> Dict[str, Any]:
        """解析SQL语句，支持多表关联查询"""
        try:
            logger.debug(f"开始解析SQL: {sql}")
            
            # 格式化SQL
            formatted_sql = sqlparse.format(sql, strip_comments=True).strip()
            parsed = sqlparse.parse(formatted_sql)[0]
            
            # 解析表和别名
            tables = self._parse_tables_and_joins(parsed)
            
            # 解析SELECT字段
            fields = self._parse_select_fields(parsed)
            
            # 解析WHERE条件
            conditions = self._parse_where_conditions(parsed)
            
            # 解析JOIN条件
            joins = self._parse_join_conditions(parsed)
            logger.debug(f"JOIN条件: {joins}")

            parse_result = SQLParseResult(
                tables=tables,
                fields=fields,
                where_conditions=conditions,
                join_conditions=joins
            )
            
            # 组装结果
            tables_result = []
            for table_info in parse_result.tables:
                if "." in table_info.table:
                    table_name = table_info.table.split(".")[1]
                    alias = table_info.alias.split(".")[1]
                else:
                    table_name = table_info.table
                    alias = table_info.alias
                
                # 获取该表相关的字段
                table_fields = []
                for field in parse_result.fields:
                    if field.table == alias or field.table == '':
                        table_fields.append(field.name)
                
                # 获取该表相关的条件
                table_conditions = {}
                request_conditions = []
                in_conditions = []
                
                # 先收集所有条件
                for condition in parse_result.where_conditions:
                    if condition.operator == '=':
                        table_conditions = self._handle_equal_condition(
                            condition, alias, table_conditions, parse_result
                        )
                    elif condition.operator == 'IN':
                        in_conditions.append(condition)

                # 如果有IN条件，对每个IN条件都创建新的条件组合
                if in_conditions:
                    base_conditions = table_conditions.copy()
                    for in_condition in in_conditions:
                        new_conditions = self._handle_in_condition(in_condition, alias, base_conditions)
                        if not request_conditions:
                            request_conditions = new_conditions
                        else:
                            # 与已有的条件组合进行笛卡尔积
                            combined_conditions = []
                            for existing in request_conditions:
                                for new_cond in new_conditions:
                                    combined = existing.copy()
                                    combined.update(new_cond)
                                    combined_conditions.append(combined)
                            request_conditions = combined_conditions
                else:
                    request_conditions.append(table_conditions)
                    
                tables_result.append({
                    'table': table_name,
                    'alias': alias,
                    'request': request_conditions,
                    'result': table_fields
                })
            
            # 返回完整结果
            result = {
                'tables': tables_result,
                'where_conditions': conditions,
                'join_conditions': joins
            }
            
            logger.debug(f"SQL解析结果: {result}")
            return result
            
        except Exception as e:
            logger.error(f"SQL解析失败: {str(e)}", exc_info=True)
            raise

    def _parse_tables_and_joins(self, parsed) -> List[TableInfo]:
        """解析表名和别名"""
        tables = []
        from_seen = False
        current_token = ''
        
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
                
            # 标记 FROM 子句开始
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue
                
            # 处理 FROM 和 JOIN 子句
            if from_seen:
                token_upper = token.value.upper()
                
                # 跳过关键字
                if token.ttype is Keyword and token_upper in ('WHERE', 'GROUP', 'HAVING', 'ORDER'):
                    break
                if token.ttype is None and "WHERE" in token.value.upper():
                    break
                
                # 理表和别名
                if not token.is_whitespace and token.ttype is None:
                    # 获取完整的表达式（可能包含多个表和JOIN）
                    table_expr = str(token).strip()

                    # 关系语句不予处理
                    if "=" in table_expr:
                        continue

                    # 分割成单独的部分（处理JOIN）
                    parts = table_expr.split('JOIN')
                    
                    # 处理第一个表（FROM 子句中的）
                    if parts[0]:
                        first_table = parts[0].strip().split()
                        if len(first_table) >= 2:
                            tables.append(TableInfo(
                                table=first_table[0],
                                alias=first_table[-1]
                            ))
                        else:
                            tables.append(TableInfo(
                                table=first_table[0],
                                alias=first_table[0]
                            ))
                    
                    # 处理JOIN的表
                    for part in parts[1:]:
                        # 去掉ON及之后的内容
                        table_part = part.split('ON')[0].strip()
                        table_parts = table_part.split()
                        
                        if len(table_parts) >= 2:
                            tables.append(TableInfo(
                                table=table_parts[0],
                                alias=table_parts[-1]
                            ))
                        else:
                            tables.append(TableInfo(
                                table=table_parts[0],
                                alias=table_parts[0]
                            ))
        
        return tables

    def _parse_select_fields(self, parsed) -> List[SelectField]:
        """解析SELECT字段"""
        fields = []
        select_seen = False
        from_seen = False
        
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
                
            if token.ttype is Keyword.DML and token.value.upper() == 'SELECT':
                select_seen = True
                continue
                
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
                break
                
            if select_seen and not from_seen:
                if "," in token.value:
                    for field in token.value.split(','):
                        field = field.strip()
                        if ' AS ' in field.upper():
                            parts = field.split(' AS ')
                            table_field = parts[0].strip().split('.')
                            fields.append(SelectField(
                                table=table_field[0],
                                column=table_field[1],
                                name=parts[1].strip()
                            ))
                        else:
                            if '.' in field:
                                table_field = field.split('.')
                                fields.append(SelectField(
                                    table=table_field[0],
                                    column=table_field[1],
                                    name=table_field[1]
                                ))
                            else:
                                fields.append(SelectField(
                                    table='',
                                    column=field,
                                    name=field
                                ))
        
        return fields

    def _parse_where_conditions(self, parsed) -> List[WhereCondition]:
        """解析WHERE条件"""
        conditions = []
        
        # 初始化标记位
        where_seen = False
        limit_seen = False
        order_by_seen = False
        offset_seen = False
        # 遍历tokens检查是否存在WHERE、LIMIT、ORDER BY子句
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
                
            if isinstance(token, Where):
                where_seen = True
                logger.debug(f"WHERE条件: {token}")
            elif token.ttype is Keyword and token.value.upper() == 'LIMIT':
                limit_seen = True
                logger.debug(f"LIMIT条件: {token}")
            elif token.ttype is Keyword and token.value.upper() == 'OFFSET':
                offset_seen = True
                logger.debug(f"OFFSET条件: {token}")
            elif token.ttype is Keyword and token.value.upper() == 'ORDER BY':
                order_by_seen = True
                logger.debug(f"ORDER BY条件: {token}")

        if not (where_seen or limit_seen or order_by_seen):
            # 如果没有任何条件子句,直接返回空列表
            return []

        if where_seen:
            for token in parsed.tokens:
                if isinstance(token, Where):
                    where_conditions = self._parse_where_token(token)
                    conditions.extend(where_conditions)
        
        if limit_seen:
            # 解析LIMIT条件
            limit_conditions = self._parse_limit_conditions(parsed)
            conditions.extend(limit_conditions)

        if offset_seen:
            # 解析OFFSET条件
            offset_conditions = self._parse_offset_conditions(parsed)
            conditions.extend(offset_conditions)

        if order_by_seen:
            # 解析ORDER BY条件
            order_by_conditions = self._parse_order_by_conditions(parsed)
            conditions.extend(order_by_conditions)
        
        return conditions

    def _parse_where_token(self, where_token: Where) -> List[WhereCondition]:
        """解析WHERE token中的条件"""
        conditions = []
        
        for item in where_token.tokens:
            logger.debug(f"WHERE条件: {item}")
            if item.is_whitespace:
                continue
            if isinstance(item, Comparison):
                condition = self._parse_comparison(item)
                if condition:
                    conditions.append(condition)
            elif item.ttype is Keyword and 'IN' in item.value.upper():
                # 处理IN条件
                condition = self._parse_comparison_IN(where_token.tokens)
                if condition:
                    conditions.append(condition)
        
        return conditions

    def _parse_limit_conditions(self, parsed) -> List[WhereCondition]:
        """解析LIMIT条件"""
        conditions = []
        limit_seen = False
        
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
                
            if token.ttype is Keyword and token.value.upper() == 'LIMIT':
                limit_seen = True
                continue
                
            if limit_seen:
                if token.ttype is not None:
                    value = token.value
                    if ',' in value:
                        # 格式为: LIMIT offset, limit
                        offset, limit = value.split(',')
                        conditions.append(WhereCondition(
                            table="",
                            column="offset", 
                            value=offset.strip(),
                            operator="="
                        ))
                        conditions.append(WhereCondition(
                            table="",
                            column="limit",
                            value=limit.strip(),
                            operator="="
                        ))
                    else:
                        # 格式为: LIMIT limit
                        conditions.append(WhereCondition(
                            table="",
                            column="limit",
                            value=value,
                            operator="="
                        ))
                    break
        return conditions
    
    def _parse_offset_conditions(self, parsed) -> List[WhereCondition]:
        """解析OFFSET条件"""
        conditions = []
        offset_seen = False
        
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
            
            if token.ttype is Keyword and token.value.upper() == 'OFFSET':
                offset_seen = True
                continue
                
            if offset_seen:
                if token.ttype is not None:
                    value = token.value
                    conditions.append(WhereCondition(
                        table="",
                        column="offset",
                        value=value,
                        operator="="
                    ))
                    break
        return conditions

    def _parse_comparison(self, comparison) -> Optional[WhereCondition]:
        """解析比较表达式"""
        left = None
        operator = None
        right = None
        
        for token in comparison.tokens:
            if isinstance(token, Identifier):
                if left is None:
                    left = token.value
                continue

            if token.is_whitespace:
                continue
                
            if operator is not None:
                right = token.value.strip("'").strip('"')
                break
                
            # 扩展操作符支持
            if token.value.upper() == 'LIKE':
                operator = 'LIKE'
            elif token.value in ['=', '>', '<', '>=', '<=', '!=', '<>']:
                operator = '='
                
        if left and operator and right:
            # 处理 LIKE 条件的值，去掉 % 号
            if operator == 'LIKE':
                right = right.replace('%', '')
            
            if "." in left:
                left_table = left.split('.')
                return WhereCondition(
                    table=left_table[0],
                    column=left_table[1],
                    value=right,
                    operator=operator
                )
            else:
                return WhereCondition(
                    table="",
                    column=left,
                    value=right,
                    operator=operator
                )
        return None
    
    def _parse_comparison_on(self, comparison, sequence: int) -> Optional[JoinCondition]:
        """解析比较表达式"""
        left = None
        operator = None
        right = None
        
        for token in comparison.tokens:
            if isinstance(token, Identifier):
                if left is None:
                    left = token.value
                    continue

            if token.is_whitespace:
                continue
                
            if operator is not None:
                right = token.value
                break
                
            if token.value in ['=', '>', '<', '>=', '<=', '!=', '<>']:
                operator = token.value
                continue
                
        if left and operator and right and operator in ['=']:
            left_table = left.split('.')
            right_table = right.split('.')
            return JoinCondition(
                leftTable=left_table[0],
                leftColumn=left_table[1],
                rightTable=right_table[0],
                rightColumn=right_table[1],
                sequence=sequence  # 使用传入的序号
            )
        return None

    def _parse_join_conditions(self, parsed) -> List[JoinCondition]:
        """解析JOIN条件"""
        conditions = []
        join_seen = False
        on_seen = False
        sequence = 0  # JOIN序号计数器
        
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
                
            if 'JOIN' in str(token).upper():
                join_seen = True
                sequence += 1  # 每遇到一个JOIN就增加序号
                continue
                
            if join_seen and 'ON' in str(token).upper():
                on_seen = True
                continue
                
            if on_seen and isinstance(token, Comparison):
                condition = self._parse_comparison_on(token, sequence)  # 传递序号
                if condition:
                    conditions.append(condition)
                on_seen = False
                join_seen = False
                
        return conditions

    def _parse_order_by_conditions(self, parsed) -> List[WhereCondition]:
        """解析ORDER BY条件"""
        order_conditions = []
        order_by_seen = False
        
        for token in parsed.tokens:
            if token.is_whitespace:
                continue
            
            # 检测 ORDER BY 关键字
            if token.ttype is Keyword and token.value.upper() == 'ORDER BY':
                order_by_seen = True
                continue
            
            # 解析 ORDER BY 后的字段
            if order_by_seen and token.ttype is None and not token.is_whitespace:
                # 处理多个排序字段
                fields = token.value.split(',')
                for field in fields:
                    field = field.strip()
                    parts = field.split()
                    
                    # 默认排序方向为 ASC
                    direction = 'ASC'
                    if len(parts) > 1 and parts[1].upper() in ['ASC', 'DESC']:
                        field_name = parts[0]
                        direction = parts[1].upper()
                    else:
                        field_name = field
                    
                    # 处理表别名
                    if '.' in field_name:
                        table, column = field_name.split('.')
                        order_conditions.append(WhereCondition(
                            table=table,
                            column=column,
                            value=direction,
                            operator='order by'
                        ))
                    else:
                        order_conditions.append(WhereCondition(
                            table='',
                            column=field_name,
                            value=direction,
                            operator='order by'
                        ))
                break
        
        return order_conditions
    
    def _parse_comparison_IN(self, comparison) -> Optional[WhereCondition]:
        """解析比较表达式"""
        left = None
        operator = None
        right = None
        
        for token in comparison:
            if isinstance(token, Identifier):
                if left is None:
                    left = token.value
                continue

            if token.is_whitespace:
                continue
                
            if operator is not None:
                right = token.value.strip("'").strip('"')
                break
                
            # 扩展操作符支持
            if token.value.upper() == 'IN':
                operator = 'IN'
                continue
        if left and operator and right:
            
            # 处理 IN 条件的值
                # 去掉括号并分割值
            right = right.strip('()').split(',')
            right = [v.strip().strip("'").strip('"') for v in right]

            if "." in left:
                left_table = left.split('.')
                return WhereCondition(
                    table=left_table[0],
                    column=left_table[1],
                    value=right,
                    operator=operator
                )
            else:
                return WhereCondition(
                    table="",
                    column=left,
                    value=right,
                    operator=operator
                )
        return None

    def _handle_equal_condition(self, condition, alias, table_conditions, parse_result):
        """处理等于操作符的条件"""
        if condition.table.__len__() == 0 or condition.table == alias:
            table_conditions[condition.column] = condition.value
        # else:
        #     for join_cond in parse_result.join_conditions:
        #         if (join_cond.leftTable == alias and 
        #             join_cond.rightTable == condition.table and 
        #             join_cond.rightColumn == condition.column):
        #             table_conditions[join_cond.leftColumn] = condition.value
        #         elif (join_cond.rightTable == alias and 
        #               join_cond.leftTable == condition.table and 
        #               join_cond.leftColumn == condition.column):
        #             table_conditions[join_cond.rightColumn] = condition.value
        return table_conditions

    def _handle_in_condition(self, condition, alias, table_conditions):
        """处理IN操作符的条件"""
        result_conditions = []
        for value in condition.value:
            value = value.strip()  # 去除空格
            current_conditions = table_conditions.copy()  # 复制基础条件
            if condition.table.__len__() == 0 or condition.table == alias:
                current_conditions[condition.column] = value
            result_conditions.append(current_conditions)
        return result_conditions