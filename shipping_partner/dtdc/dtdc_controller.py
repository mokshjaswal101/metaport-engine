import http
from fastapi import APIRouter, Request, Depends
from fastapi import APIRouter, Depends
import ast
from fastapi.security import HTTPBearer
from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response

# service
from .dtdc import Dtdc

from logger import logger

# creating a client router
dtdc_router = APIRouter(tags=["Dtdc"])
security = HTTPBearer()


@dtdc_router.post(
    "/webhook/courier/dtdc/track-order",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def tracking_webhook(request: Request):

    track_req = await request.json()

    print("track_req", track_req)

    # return build_api_response(
    #     GenericResponseModel(status=True, status_code=200, message="success")
    # )

    response: GenericResponseModel = Dtdc.tracking_webhook(response_data=track_req)
    return build_api_response(response)
