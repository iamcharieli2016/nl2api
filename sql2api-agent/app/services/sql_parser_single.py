import sqlparse
from typing import Dict, List, Any
from sqlparse.sql import Where, Comparison, Identifier, Token
from sqlparse.tokens import Keyword, DML
from app.core.logger import logger

class SQLParser:
    def is_valid_sql(self, sql: str) -> bool:
        """
        验证SQL语句是否合法
        """
        try:
            # 格式化并解析SQL
            formatted_sql = sqlparse.format(sql, strip_comments=True).strip()
            parsed = sqlparse.parse(formatted_sql)[0]
            
            # 检查是否是SELECT语句
            if not parsed.get_type() == 'SELECT':
                return False
                
            # 检查是否包含FROM
            has_from = False
            for token in parsed.tokens:
                if token.ttype is Keyword and token.value.upper() == 'FROM':
                    has_from = True
                    break
            
            return has_from
        except Exception:
            return False

    def parse_sql(self, sql: str) -> Dict[str, Any]:
        """
        解析SQL语句，返回表名和where条件
        """
        try:
            logger.debug(f"开始解析SQL: {sql}")
            if not self.is_valid_sql(sql):
                raise ValueError("Invalid SQL statement")
            
            # 格式化SQL语句
            formatted_sql = sqlparse.format(sql, strip_comments=True).strip()
            # 解析SQL语句
            parsed = sqlparse.parse(formatted_sql)[0]
            
            result = {
                'tables': [],
                'where_conditions': []
            }
            
            # 提取表名和WHERE条件
            for token in parsed.tokens:
                # 提取表名
                if token.ttype is Keyword and token.value.upper() == 'FROM':
                    # 获取FROM后面的表名
                    next_token = self._get_next_token(parsed.tokens, token)
                    if next_token:
                        table_identifier = next_token.value.strip('`').strip('"').strip("'")
                        result['tables'].append(table_identifier)
                
                # 提取WHERE条件
                if isinstance(token, Where):
                    result['where_conditions'] = self._parse_where_conditions(token)
            
            logger.debug(f"SQL解析结果: {result}")
            return result
            
        except Exception as e:
            logger.error(f"SQL解析失败: {str(e)}", exc_info=True)
            raise
    
    def _get_next_token(self, tokens, current_token):
        """获取下一个非空白token"""
        found_current = False
        for token in tokens:
            if found_current and not token.is_whitespace:
                return token
            if token == current_token:
                found_current = True
        return None

    def _parse_where_conditions(self, where_clause) -> List[Dict]:
        """
        解析WHERE子句中的条件
        """
        conditions = []
        
        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                # 解析比较表达式
                left = None
                operator = None
                right = None
                
                # 存储所有token以便处理
                tokens = [t for t in token.tokens if not t.is_whitespace]
                
                for i, item in enumerate(tokens):
                    if isinstance(item, Identifier):
                        left = item.value
                    elif self._is_comparison_operator(item):
                        operator = item.value.strip()
                        # 获取操作符后的值（下一个非空token）
                        if i + 1 < len(tokens):
                            next_token = tokens[i + 1]
                            right = next_token.value.strip("'").strip('"')
                            break
                
                if left and operator and right:
                    conditions.append({
                        'column': left,
                        'operator': operator,
                        'value': right
                    })
        
        return conditions

    def _is_comparison_operator(self, token):
        """检查是否是比较运算符"""
        if token.value:
            return token.value.strip() in ['=', '>', '<', '>=', '<=', '!=', '<>']
        return False
