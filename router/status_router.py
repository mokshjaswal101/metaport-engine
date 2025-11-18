import http
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from database.db import db_engine

StatusRouter = APIRouter(tags=["health_checks"])


# normal status check
@StatusRouter.get("/status", status_code=http.HTTPStatus.OK)
async def status_check():
    return JSONResponse(status_code=http.HTTPStatus.OK, content={"status": "OK"})


# deep check with db connection
@StatusRouter.get("/deepstatus", status_code=http.HTTPStatus.OK)
async def deep_status_check():
    with db_engine.connect() as connection:
        result = connection.execute(text("SELECT 'true'")).fetchone()
        is_db_ok = result[0] == "true"

        if not is_db_ok:
            return JSONResponse(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                content={"error": "db not connected"},
            )

    return JSONResponse(status_code=http.HTTPStatus.OK, content={"db": is_db_ok})
