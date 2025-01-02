from typing import Any, Optional
import redis
from app.core.config import settings
from app.core.logger import logger

class CacheService:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True
        )
        self.default_expire = 300  # 5分钟默认过期时间

    def get(self, key: str) -> Optional[Any]:
        try:
            return self.redis_client.get(key)
        except Exception as e:
            logger.error(f"Redis获取缓存失败: {str(e)}")
            return None

    def set(self, key: str, value: Any, expire: int = None):
        try:
            self.redis_client.set(
                key,
                value,
                ex=expire or self.default_expire
            )
        except Exception as e:
            logger.error(f"Redis设置缓存失败: {str(e)}") 