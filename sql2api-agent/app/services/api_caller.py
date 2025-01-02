from typing import Dict, Any
import httpx
import asyncio
from app.core.logger import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import json

class APICaller:
    def __init__(self):
        self.timeout = 30  # 30秒超时
        self.max_retries = 3

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def call_api_async(self, api_config: Dict, params: Dict) -> Any:
        """异步调用API，带重试机制"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                if api_config['method'].upper() == 'GET':
                    response = await client.get(
                        api_config['url'],
                        params=params
                    )
                else:
                    template = dict(api_config['template'])
                    if 'params' in template:
                        template['params'].update(params)
                    else:
                        template.update(params)
                        
                    response = await client.post(
                        api_config['url'],
                        json=template
                    )
                
                response.raise_for_status()
                
                # 处理空响应的情况
                if not response.content:
                    logger.warning(f"API返回空响应: {api_config['url']}")
                    return []
                
                try:
                    # 先进行 UTF-8 解码
                    content_str = response.content.decode('utf-8')
                    logger.debug(f"API响应内容: {content_str}")
                    
                    # 如果解码后的内容为空，返回空列表
                    if not content_str.strip():
                        return []
                    
                    # 解析 JSON
                    return json.loads(content_str)
                except UnicodeDecodeError as e:
                    logger.error(f"响应内容解码失败: {response.content}", exc_info=True)
                    return []
                except json.JSONDecodeError as e:
                    logger.error(f"API响应解析失败: {content_str}", exc_info=True)
                    return []
                
        except httpx.TimeoutException:
            logger.error(f"API调用超时: {api_config['url']}")
            raise
        except Exception as e:
            logger.error(f"API调用失败: {str(e)}", exc_info=True)
            raise
