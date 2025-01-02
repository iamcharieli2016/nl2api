from typing import Dict, List, Any, Optional, Tuple
import asyncio
from app.core.logger import logger
from app.services.api_caller import APICaller
from app.db.models import APIMapping
from sqlalchemy.orm import Session

class APIService:
    def __init__(self):
        self.api_caller = APICaller()

    async def execute_api_calls(
        self,
        parsed_tables: List[Dict],
        parsed_joins: List[Dict],
        db: Session
    ) -> Tuple[List[Dict], Optional[Dict]]:
        """
        执行并行API调用
        返回: (结果列表, 错误信息(如果有))
        """
        api_tasks = []
        
        # 判断是单表查询还是多表关联查询
        if len(parsed_tables) == 1:
            logger.debug(f"单表查询: {parsed_tables[0]}")
            return await self._execute_single_table_query(parsed_tables[0], db)
        else:
            logger.debug(f"多表查询: {parsed_tables} {parsed_joins}")
            return await self._execute_multi_table_query(parsed_tables, parsed_joins, db)

    async def _execute_single_table_query(
        self,
        table_info: Dict,
        db: Session
    ) -> Tuple[List[Dict], Optional[Dict]]:
        """处理单表查询"""
        try:
            # 获取API映射
            api_mapping = await self.get_api_mapping(db, table_info['table'])
            if not api_mapping:
                return [], {
                    'status': 1001,
                    'message': f"未找到表 {table_info['table']} 的API映射"
                }

            # 准备API调用参数
            results = []
            for param in table_info['request']:
                # 处理limit和offset
                limit = param.pop('limit', None)
                offset = param.pop('offset', None)
                
                template = api_mapping.get_template_json()
                if limit is not None:
                    template['limit'] = limit
                if offset is not None:
                    template['offset'] = offset

                # 执行API调用
                response = await self.api_caller.call_api_async(
                    {
                        'method': api_mapping.method,
                        'url': api_mapping.api_url,
                        'template': template
                    },
                    param
                )
                
                response_data = response.get('data', []) if isinstance(response, dict) else response
                results.extend(response_data)

            return [{'table': table_info['table'], 'data': results}], None

        except Exception as e:
            logger.error(f"单表查询执行失败: {str(e)}", exc_info=True)
            return [], {
                'status': 1002,
                'message': f"API调用异常: {str(e)}"
            }

    async def _execute_multi_table_query(
        self,
        parsed_tables: List[Dict],
        parsed_joins: List[Dict],
        db: Session
    ) -> Tuple[List[Dict], Optional[Dict]]:
        """处理多表关联查询"""
        try:
            # 1. 首先执行所有表的独立查询
            table_results = {}
            for table_info in parsed_tables:
                results, error = await self._execute_single_table_query(table_info, db)
                if error:
                    return [], error
                table_results[table_info['alias']] = results[0]['data']  # 存储每个表的查询结果

            # 2. 根据JOIN条件合并数据
            merged_results = table_results[parsed_tables[0]['alias']]  # 从第一个表开始
            
            for join in parsed_joins:
                left_data = merged_results
                right_data = table_results[join.rightTable]
                
                # 基于JOIN条件合并数据，使用INNER JOIN逻辑
                new_merged_results = []
                for left_item in left_data:
                    for right_item in right_data:
                        if left_item.get(join.leftColumn) == right_item.get(join.rightColumn):
                            merged_item = {**left_item, **right_item}
                            new_merged_results.append(merged_item)
                
                merged_results = new_merged_results

            return [{'table': 'merged_results', 'data': merged_results}], None

        except Exception as e:
            logger.error(f"多表查询执行失败: {str(e)}", exc_info=True)
            return [], {
                'status': 1003,
                'message': f"多表查询异常: {str(e)}"
            }


    @staticmethod
    async def get_api_mapping(db: Session, table_name: str) -> Optional[APIMapping]:
        """异步获取API映射"""
        try:
            return db.query(APIMapping).filter(
                APIMapping.table_name == table_name
            ).first()
        except Exception as e:
            logger.error(f"获取API映射失败: {str(e)}")
            return None 