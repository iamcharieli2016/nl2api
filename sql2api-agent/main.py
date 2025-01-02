from fastapi import FastAPI
from app.api.endpoints import router
from app.core.exceptions import setup_exception_handlers
import uvicorn

app = FastAPI(title="SQL to API Agent")

# 注册路由
app.include_router(router)

# 设置异常处理
setup_exception_handlers(app)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        workers=1
    )
