import uvicorn
import asyncio
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from context_manager.context import get_db_session
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import json
import os
from logger import logger
from pydantic import ValidationError
from utils.exception_handler import (
    handle_validation_error,
    custom_http_exception_handler,
)

from router import CommonRouter, DefaultRouter, StatusRouter, OpenRouter
from modules.authentication import auth_router

from database.db import init_models  # sync DB init

app = FastAPI()

# Routers
app.include_router(CommonRouter)
app.include_router(auth_router)
app.include_router(StatusRouter)
app.include_router(DefaultRouter)
app.include_router(OpenRouter)

# Exception handlers
app.add_exception_handler(ValidationError, handle_validation_error)
app.add_exception_handler(HTTPException, custom_http_exception_handler)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Queue + worker config
NUM_WORKERS = 20  # Number of parallel workers
QUEUE_MAX_SIZE = 1000  # Maximum queue size
TIMEOUT_SECONDS = 30  # Wait timeout before returning 503

request_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

# Upload folder
os.makedirs("uploads", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")


# -------------------------------
# Worker to process requests
# -------------------------------
async def worker(worker_id: int):
    while True:
        request, call_next, response_future = await request_queue.get()
        try:
            # Process the request
            response = await call_next(request)
            response_future.set_result(response)
        except Exception as e:
            logger.error(f"Worker-{worker_id} error: {str(e)}")
            response_future.set_result(
                Response(content="Internal Server Error", status_code=500)
            )
        finally:
            request_queue.task_done()


# -------------------------------
# Startup event
# -------------------------------
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    # Initialize DB safely in executor
    await loop.run_in_executor(None, init_models)

    # Start workers
    for i in range(NUM_WORKERS):
        asyncio.create_task(worker(i))
    logger.info(f"Started {NUM_WORKERS} workers for request queue")


# -------------------------------
# Queue middleware
# -------------------------------
@app.middleware("http")
async def queue_middleware(request: Request, call_next):
    response_future = asyncio.Future()
    try:
        await asyncio.wait_for(
            request_queue.put((request, call_next, response_future)),
            timeout=TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        return Response(content="Server overloaded, try again later", status_code=503)

    return await response_future


# -------------------------------
# Close DB connections properly
# -------------------------------
@app.middleware("http")
async def close_idle_connections(request: Request, call_next):
    loop = asyncio.get_running_loop()
    db = await loop.run_in_executor(None, get_db_session)
    try:
        request.state.db = db
        response = await call_next(request)
    finally:
        if db:
            await loop.run_in_executor(None, db.close)
    return response


# -------------------------------
# Validation error handler
# -------------------------------
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    raw = (await request.body()).decode("utf-8", "ignore")
    logger.error("422 on %s\nBody: %s\nErrors: %s", request.url, raw, exc.errors())
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        parsed = raw
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": parsed},
    )


# -------------------------------
# Optional: Global rate limiter
# -------------------------------
import time

MAX_REQUESTS_PER_SECOND = 200
WINDOW_SECONDS = 1
request_timestamps = []


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    global request_timestamps
    now = time.time()
    # keep timestamps within 1 second
    request_timestamps = [t for t in request_timestamps if now - t < WINDOW_SECONDS]
    if len(request_timestamps) >= MAX_REQUESTS_PER_SECOND:
        return JSONResponse({"detail": "Too many requests, slow down"}, status_code=429)
    request_timestamps.append(now)
    return await call_next(request)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
