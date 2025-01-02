from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

# 创建新的数据库URL
database_url = settings.DATABASE_URL
if 'mysql://' in database_url:
    database_url = database_url.replace('mysql://', 'mysql+pymysql://')

engine = create_engine(
    database_url,
    pool_size=20,
    max_overflow=10,
    pool_timeout=30
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
