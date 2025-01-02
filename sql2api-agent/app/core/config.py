from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # 数据库配置
    DATABASE_URL: str = "mysql://root:ik2jE42p@172.20.7.75:3307/data_agent_sql2api_meta"
    
    # Redis配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    
    # API调用配置
    API_TIMEOUT: int = 30
    API_MAX_RETRIES: int = 3
    
    # 缓存配置
    CACHE_EXPIRE: int = 300  # 5分钟
    
    class Config:
        env_file = ".env"

settings = Settings()
