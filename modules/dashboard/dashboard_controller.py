import http
from fastapi import APIRouter, Depends
from schema.base import GenericResponseModel

from context_manager.context import build_request_context


# utils
from utils.response_handler import build_api_response

# service
from .dashboard_service import DashboardService

# shchema
from .dashboard_schema import dashboard_filters

# creating an auth router
dashboard_router = APIRouter(tags=["dashboard"], prefix="/dashboard")


@dashboard_router.post(
    "/performance",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_performance_data(filters: dashboard_filters):
    print("inside")
    response: GenericResponseModel = DashboardService.get_performance_data(
        filters=filters
    )

    return build_api_response(response)
