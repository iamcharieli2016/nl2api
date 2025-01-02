from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.services.sql_parser import SQLParser
from app.services.api_service import APIService
from app.services.cache_service import CacheService
from app.core.exceptions import DatabaseError
from app.db.models import APIMapping
from typing import Dict, List, Any, Optional
from pydantic import BaseModel
import asyncio
import hashlib
from app.core.logger import logger
from app.services.merge_service import MergeService

class SQLExecuteRequest(BaseModel):
    reportId: int
    sql: str
    question: str

class SQLExecuteResponse(BaseModel):
    status: int = 0
    message: str = "success"
    data: Any = None
    cache_hit: bool = False

router = APIRouter()
cache_service = CacheService()

def get_cache_key(sql: str) -> str:
    """生成缓存key"""
    return f"sql_result:{hashlib.md5(sql.encode()).hexdigest()}"

@router.post("/execute", response_model=SQLExecuteResponse)
async def execute_sql(
    request: SQLExecuteRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    try:
        logger.info(f"收到SQL执行请求: {request.sql}")
        
        # 检查缓存
        cache_key = get_cache_key(request.sql)
        cached_result = cache_service.get(cache_key)
        if cached_result:
            logger.info("命中缓存")
            return SQLExecuteResponse(
                status=0,
                message="success (cached)",
                data=cached_result,
                cache_hit=True
            )
        
        # 解析SQL
        parser = SQLParser()
        parsed_results = parser.parse_sql(request.sql)
        logger.debug(f"SQL解析结果: {parsed_results}")
        
        # 并行调用API
        api_service = APIService()
        all_results, error = await api_service.execute_api_calls(parsed_results['tables'],parsed_results['join_conditions'], db)
        
        if error:
            return SQLExecuteResponse(
                status=error['status'],
                message=error['message'],
                data=[]
            )
        
        # 合并结果
        merge_service = MergeService()
        final_result = await merge_service.merge_results(all_results, parsed_results)
        
        # 异步保存缓存
        background_tasks.add_task(
            cache_service.set,
            cache_key,
            final_result
        )
        
        logger.info("所有API调用成功完成")
        return SQLExecuteResponse(
            status=0,
            message="success",
            data=final_result
        )
            
    except Exception as e:
        logger.error(f"SQL解析异常: {str(e)}", exc_info=True)
        return SQLExecuteResponse(
            status=1001,
            message=f"SQL解析异常: {str(e)}",
            data=[]
        )
    finally:
        db.close()