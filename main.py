import uvicorn
import asyncio
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from context_manager.context import get_db_session
from fastapi.staticfiles import StaticFiles
import os
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import json, logging
from logger import logger


from pydantic import ValidationError
from utils.exception_handler import (
    handle_validation_error,
    custom_http_exception_handler,
)

from router import CommonRouter
from router import DefaultRouter
from router import StatusRouter
from router import OpenRouter
from modules.authentication import auth_router
from modules.user import otp_router  # OTP endpoints without full auth

from database import DBBase, db_engine


# sync all the models in the database
DBBase.metadata.create_all(db_engine)

app = FastAPI()

# Include the routers
app.include_router(CommonRouter)  # Authenticated endpoints (requires OTP verification)
app.include_router(auth_router)  # Login/Signup (no auth required)
app.include_router(otp_router)  # OTP verification (token required but no OTP verification check)
app.include_router(StatusRouter)
app.include_router(DefaultRouter)
app.include_router(OpenRouter)


# Register the exception handler for pydantic validation errors
app.add_exception_handler(ValidationError, handle_validation_error)
app.add_exception_handler(HTTPException, custom_http_exception_handler)


# CORS Configuration
# Configure allowed origins from environment variable for security
# For development, use: ALLOWED_ORIGINS="http://localhost:3000,http://localhost:5173"
# For production, use specific domains: ALLOWED_ORIGINS="https://app.metaport.com"
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "").split(",")

# Fallback to allow all in development if not configured (NOT RECOMMENDED for production)
if not ALLOWED_ORIGINS or ALLOWED_ORIGINS == [""]:
    logger.warning(
        "CORS: No ALLOWED_ORIGINS configured. Allowing all origins (INSECURE - not recommended for production)"
    )
    ALLOWED_ORIGINS = ["*"]
else:
    # Clean up whitespace
    ALLOWED_ORIGINS = [origin.strip() for origin in ALLOWED_ORIGINS if origin.strip()]
    logger.info(f"CORS: Allowed origins configured: {ALLOWED_ORIGINS}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Length", "X-Request-ID"],
    max_age=600,  # Cache preflight requests for 10 minutes
)


# Define queue and number of workers
NUM_WORKERS = 14
request_queue = asyncio.Queue()  # Global queue for task handling
TIMEOUT_SECONDS = 30  # Max time a request should wait in queue


if not os.path.exists("uploads"):
    os.makedirs("uploads")

app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")


async def worker(worker_id: int):
    """
    Worker that continuously processes requests from the queue.
    """
    while True:
        request, call_next, response_future = await request_queue.get()  # Fetch task

        try:
            print(f"Worker-{worker_id} processing: {request.url}")
            response = await call_next(request)  # Execute original request
            response_future.set_result(response)  # Return response
        except Exception as e:
            print(f"Worker-{worker_id} error: {e}")
            response_future.set_result(
                Response(content="Internal Server Error", status_code=500)
            )
        finally:
            request_queue.task_done()  # Mark task as done


@app.on_event("startup")
async def startup_event():
    """
    Starts workers on FastAPI startup.
    """
    for i in range(NUM_WORKERS):
        asyncio.create_task(worker(i))


@app.middleware("http")
async def queue_middleware(request: Request, call_next):
    """
    Middleware that queues all requests for processing and returns correct API response.
    """
    response_future = asyncio.Future()  # Future to store response

    try:
        await asyncio.wait_for(
            request_queue.put((request, call_next, response_future)),
            timeout=TIMEOUT_SECONDS,
        )  # Add request to queue with a timeout
    except asyncio.TimeoutError:
        return Response(content="Server overloaded, try again later", status_code=503)

    return await response_future  # Wait and return the correct API response


@app.middleware("http")
async def close_idle_connections(request: Request, call_next):
    response = None
    db = get_db_session()
    try:
        request.state.db = db
        response = await call_next(request)
    finally:
        if db:  # Check if the connection is idle
            db.close()  # Close the idle connection
    return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    raw = (await request.body()).decode("utf-8", "ignore")
    logger.error("422 on %s\nBody: %s\nErrors: %s", request.url, raw, exc.errors())
    print(f"422 on {request.url}\nBody: {raw}\nErrors: {exc.errors()}")
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        parsed = raw
    return JSONResponse(
        status_code=422, content={"detail": exc.errors(), "body": parsed}
    )


if __name__ == "__main__":
    uvicorn.run("app:main", host="0.0.0.0", port=8000, reload=True)
