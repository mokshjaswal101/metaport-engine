from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse

limiter = Limiter(key_func=get_remote_address)


def rate_limit_handler(request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429, content={"detail": "Too many requests. Slow down!"}
    )
