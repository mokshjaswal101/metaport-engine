import http
import asyncio
from fastapi import APIRouter


# schema
from schema.base import GenericResponseModel
from modules.serviceability.serviceability_schema import ServiceabilityParamsModel
from modules.serviceability.serviceability_schema import (
    RateCalculatorParamsModel,
    RateCalculatorResponseModel,
)

# utils
from utils.response_handler import build_api_response

# services
from .serviceability_service import ServiceabilityService


serviceability_router = APIRouter(tags=["serviceability"])


# @serviceability_router.post(
#     "/courier/serviceability",
#     status_code=http.HTTPStatus.CREATED,
#     response_model=GenericResponseModel,
# )
# async def get_available_couriers(serviceability_params: ServiceabilityParamsModel):
#     try:
#         response: GenericResponseModel = ServiceabilityService.get_available_couriers(
#             serviceability_params=serviceability_params
#         )
#         return build_api_response(response)


#     except Exception as e:
#         return build_api_response(
#             GenericResponseModel(
#                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
#                 data=str(e),
#                 message="An error occurred while getting the available shipment partners.",
#             )
#         )
@serviceability_router.post(
    "/courier/serviceability",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_available_couriers(serviceability_params: ServiceabilityParamsModel):
    try:
        response = await ServiceabilityService.get_available_couriers(
            serviceability_params
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while getting the available shipment partners.",
            )
        )


@serviceability_router.get(
    "/rate-card",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_rate_card():
    try:
        response: GenericResponseModel = ServiceabilityService.get_rate_card()
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while getting the available shipment partners.",
            )
        )


# @serviceability_router.get(
#     "/pincode/details",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
# )
# async def get_pincode_details(pincode: int):
#     try:
#         response: GenericResponseModel = ServiceabilityService.get_pincode_details(
#             pincode=pincode
#         )
#         return build_api_response(response)

#     except Exception as e:
#         return build_api_response(
#             GenericResponseModel(
#                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
#                 data=str(e),
#                 message="An error occurred while getting the pincode details.",
#             )
#         )


@serviceability_router.get(
    "/pincode/details",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_pincode_details(pincode: int):
    try:
        # Await async service call if your service is async
        response: GenericResponseModel = (
            await ServiceabilityService.get_pincode_details(pincode=pincode)
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while getting the pincode details.",
            )
        )


@serviceability_router.post(
    "/ratecalculator",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def calculate_rate(rate_calculator_params: RateCalculatorParamsModel):
    try:
        print("📥 API Hit: /ratecalculator with payload:", rate_calculator_params)
        response: GenericResponseModel = ServiceabilityService.get_contracts_calculator(
            rate_calculator_params
        )
        return build_api_response(response)
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while calculating the rate.",
            )
        )
