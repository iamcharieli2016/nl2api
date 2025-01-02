from typing import Dict, List, Any
from app.core.logger import logger
from datetime import datetime

class MergeService:
    @staticmethod
    def filter_by_like_conditions(data: List[Dict], where_conditions: List[Any]) -> List[Dict]:
        """
        根据LIKE条件过滤数据
        
        Args:
            data: 需要过滤的数据列表
            where_conditions: WHERE条件列表
            
        Returns:
            List[Dict]: 过滤后的数据列表
        """
        filtered_data = []
        for item in data:
            should_include = True
            for condition in where_conditions:
                if condition.operator != 'LIKE':
                    continue
                    
                column = condition.column
                if isinstance(column, str) and column.startswith('`') and column.endswith('`'):
                    column = column[1:-1]
                if column not in item:
                    continue
                    
                item_value = str(item[column]).lower()
                search_value = str(condition.value).lower()  # 转换为小写进行不区分大小写的比较
                
                if search_value not in item_value:
                    should_include = False
                    break
                    
            if should_include:
                filtered_data.append(item)
                
        return filtered_data

    @staticmethod
    async def merge_results(
        all_results: List[Dict],
        parsed_results: Dict[str, Any]
    ) -> List[Dict]:
        """
        异步合并多个表的查询结果
        当有多个表时，将每个表的数据行进行组合
        """
        if len(all_results) <= 1:
            # 单表查询，保持原有逻辑
            merged_data = []
            for result in all_results:
                merged_data.extend(result['data'])
        else:
            # 多表查询，需要进行数据行组合
            merged_data = []
            # 获取第一个表的数据作为基础
            base_data = all_results[0]['data']
            
            # 判断是否为单表多次查询
            is_single_table = True
            base_table = all_results[0].get('table_name', '')
            for result in all_results[1:]:
                if result.get('table_name', '') != base_table:
                    is_single_table = False
                    break
            
            if is_single_table:
                # 单表多次查询，直接合并结果
                for result in all_results[1:]:
                    merged_data.extend(result['data'])
            else:
                # 多表关联查询，需要进行数据行组合
                for base_row in base_data:
                    combined_row = base_row.copy()
                    for other_result in all_results[1:]:
                        for other_row in other_result['data']:
                            temp_row = {**combined_row, **other_row}
                            merged_data.append(temp_row)

        # 判断是否有where中是否有like条件，如果有的话，按照like条件过滤结果数据merged_data
        has_like_conditions = any(
            condition.operator == 'LIKE' 
            for condition in parsed_results['where_conditions']
        )
                
        # 如果有LIKE条件，使用filter_by_like_conditions函数进行过滤
        filtered_by_where = (
            MergeService.filter_by_like_conditions(merged_data, parsed_results['where_conditions'])
            if has_like_conditions
            else merged_data
        )
        
        merged_data = filtered_by_where

        # 根据order_by条件进行排序
        if parsed_results['where_conditions']:
            merged_data = MergeService.sort_results(merged_data, parsed_results['where_conditions'])

        # 根parsed_results中的字段筛选数据
        filtered_data = []
        for item in merged_data:
            filtered_item = {}
            for parsed_result in parsed_results['tables']:
                # 获取需要的字段
                result_fields = parsed_result.get('result', [])
                # 当result_fields为空时，返回所有字段
                if not result_fields:
                    filtered_item = item.copy()
                    break
                # 否则只返回指定字段
                for field in result_fields:
                    if field in item:
                        filtered_item[field] = item[field]
            if filtered_item:
                filtered_data.append(filtered_item)

        # 处理limit条件
        limit = None
        offset = 0
        for parsed_result in parsed_results['tables']:
            conditions = parsed_result.get('request', {})
            if 'limit' in conditions:
                limit = int(conditions['limit'])
            if 'offset' in conditions:
                offset = int(conditions['offset'])

        # 应用limit和offset
        if limit is not None:
            filtered_data = filtered_data[offset:offset + limit]

        return filtered_data

    @staticmethod
    def sort_results(data: List[Dict], where_conditions: List[Dict]) -> List[Dict]:
        """根据order_by条件对结果进行排序"""
        if not data or not where_conditions:
            return data
        
        # 从where_conditions中提取order by条件
        order_by = [
            condition for condition in where_conditions 
            if condition.operator.upper() == 'ORDER BY'
        ]
        
        # 如果没有排序条件，直接返回原数据
        if not order_by:
            return data
            
        def get_sort_key(item):
            keys = []
            for sort_condition in order_by:
                column = sort_condition.column
                value = item.get(column, None)
                
                # 处理字符串类型的值
                if isinstance(value, str):
                    # 尝试转换为日期
                    try:
                        value = datetime.strptime(value, '%Y-%m-%d')
                    except (ValueError, TypeError):
                        # 如果不是日期，尝试转换为数值
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            # 如果既不是日期也不是数值，使用字符串的ASCII码
                            value = ord(value[0]) if value else 0
                
                # 处理降序
                if sort_condition.value == 'DESC':
                    if isinstance(value, datetime):
                        # 对于日期类型，使用一个足够大的未来日期减去当前日期
                        max_date = datetime(9999, 12, 31)
                        value = max_date - value
                    elif isinstance(value, (int, float)):
                        value = -value
                    elif isinstance(value, str):
                        value = -ord(value[0]) if value else 0
                keys.append(value)
            return keys
        
        return sorted(data, key=get_sort_key) 