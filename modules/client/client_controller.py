import http
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from .client_schema import (
    ClientInsertModel,
    OnBoardingForm,
    getClientFiltersModel,
    OnBoardingForm,
    OtpVerified,
    OnBoardingPreviousSchema,
    CompleteClientDetailsModel,
)

# utils
from utils.response_handler import build_api_response

# service
from .client_service import ClientService
from .client_onboarding_service import ClientOnboardingService

# creating a client router
client_router = APIRouter(tags=["client"], prefix="/client")


@client_router.post(
    "/create", status_code=http.HTTPStatus.CREATED, response_model=GenericResponseModel
)
async def create_new_client(
    client_data: ClientInsertModel,
):
    response: GenericResponseModel = ClientService.create_client(
        client_data=client_data
    )
    return build_api_response(response)


@client_router.post(
    "/", status_code=http.HTTPStatus.CREATED, response_model=GenericResponseModel
)
async def get_all_clients(clientFilters: getClientFiltersModel):
    response: GenericResponseModel = ClientService.get_all_clients(
        clientFilters=clientFilters
    )
    return build_api_response(response)


# @client_router.post(
#     "/on-boarding",
#     status_code=http.HTTPStatus.CREATED,
#     response_model=GenericResponseModel,
# )
# async def onboarding_client(
#     onBoardingForm: OnBoardingForm,
# ):

#     response: GenericResponseModel = ClientOnboardingService.onboarding_create(
#         onboarding_form=onBoardingForm
#     )
#     return build_api_response(response)


@client_router.post(
    "/on-boarding",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def onboarding_client(onBoardingForm: OnBoardingForm):
    # Await the async service method
    response: GenericResponseModel = await ClientOnboardingService.onboarding_create(
        onboarding_form=onBoardingForm
    )
    return build_api_response(response)


# @client_router.post(
#     "/otp-verify",
#     status_code=http.HTTPStatus.CREATED,
#     response_model=GenericResponseModel,
# )
# async def otp_verified(
#     otpVerified: OtpVerified,
# ):


#     response: GenericResponseModel = ClientOnboardingService.otp_verified(
#         otpVerified=otpVerified
#     )
#     return build_api_response(response)
@client_router.post(
    "/otp-verify",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def otp_verified(
    otpVerified: OtpVerified,
):
    try:
        # Call the async service method
        response: GenericResponseModel = await ClientOnboardingService.otp_verified(
            otpVerified=otpVerified
        )
        return build_api_response(response)

    except Exception as e:
        # Handle unexpected errors
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An internal server error occurred during OTP verification.",
            )
        )


@client_router.post(
    "/resend-otp",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def resend_otp():

    response: GenericResponseModel = ClientOnboardingService.resend_otp()
    return build_api_response(response)


# FOR ADMIN
@client_router.post(
    "/on-boarding-previous",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def onboarding_client(
    onboarding_back: OnBoardingPreviousSchema,
):
    response: GenericResponseModel = ClientService.onboarding_previous(
        onboarding_back=onboarding_back
    )
    return build_api_response(response)


# Get onboarding forms data
# @client_router.get(
#     "/on-boarding/{stepper}",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
# )
# async def get_onboarding(stepper: str):
#     try:
#         response: GenericResponseModel = ClientOnboardingService.get_onboarding(
#             stepper=stepper
#         )
#         return build_api_response(response)

#     except Exception as e:
#         return build_api_response(
#             GenericResponseModel(
#                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
#                 data=str(e),
#                 message="An error occurred while tracking the shipment.",
#             )
#         )


@client_router.get(
    "/on-boarding/{stepper}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_onboarding(stepper: str):
    try:
        # Await the async service method
        response: GenericResponseModel = await ClientOnboardingService.get_onboarding(
            stepper=stepper
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching onboarding data.",
            )
        )


@client_router.post(
    "/doc/upload",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def onboarding_client(
    name: str = Form(...),
    file: UploadFile = File(...),
):
    try:
        response: GenericResponseModel = (
            await ClientOnboardingService.onboarding_doc_upload(name, file)
        )
        return build_api_response(response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @client_router.get(
#     "/complete-details",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
# )
# async def get_complete_client_details():
#     """
#     Get complete client details including client data, onboarding details, and bank details
#     """
#     response: GenericResponseModel = ClientService.get_complete_client_details()
#     return build_api_response(response)


@client_router.get(
    "/complete-details",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_complete_client_details():
    """
    Get complete client details including client data, onboarding details, and bank details
    """
    response: GenericResponseModel = await ClientService.get_complete_client_details()
    return build_api_response(response)
