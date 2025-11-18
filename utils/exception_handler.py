from typing import List, Dict
from pydantic import ValidationError
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse

from logger import logger


# format the validation errors into our desired output
def format_validation_errors(errors: List[dict]) -> Dict:
    formatted_errors = {}

    for error in errors:
        # Extract the field name and error message
        field = error["loc"][-1] if len(error["loc"]) > 1 else "Unknown"
        message = error["msg"]

        # Add to the formatted_errors dictionary
        if field in formatted_errors:
            formatted_errors[field].append(message)
        else:
            formatted_errors[field] = [message]

    # Construct the final response format
    return {
        "data": {"fields": formatted_errors},
        "message": "Validation error occurred.",
        "status": False,
    }


def handle_validation_error(exc: ValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content=format_validation_errors(exc.errors()))


# Custom internal server error handler
async def custom_http_exception_handler(
    request: Request, exc: HTTPException
) -> JSONResponse:
    if exc.status_code == 500:
        logger.error(f"Internal server error: {exc.detail}")
        return JSONResponse(
            status_code=500,
            content={
                "detail": "An internal server error occurred. Please try again later."
            },
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )
