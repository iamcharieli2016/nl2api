from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

class SQLParseError(Exception):
    pass

class APICallError(Exception):
    pass

class DatabaseError(Exception):
    pass

def setup_exception_handlers(app: FastAPI):
    @app.exception_handler(SQLParseError)
    async def sql_parse_error_handler(request: Request, exc: SQLParseError):
        return JSONResponse(
            status_code=400,
            content={"message": str(exc)}
        )

    @app.exception_handler(APICallError)
    async def api_call_error_handler(request: Request, exc: APICallError):
        return JSONResponse(
            status_code=500,
            content={"message": str(exc)}
        )

    @app.exception_handler(DatabaseError)
    async def database_error_handler(request: Request, exc: DatabaseError):
        return JSONResponse(
            status_code=500,
            content={"message": str(exc)}
        )
