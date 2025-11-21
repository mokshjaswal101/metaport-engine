import http
from fastapi import APIRouter, File, UploadFile
from schema.base import GenericResponseModel


# schema
from .shipping_label_schema import LabelSettingUpdateModel, generateLabelRequest

# utils
from utils.response_handler import build_api_response

# service
from .shipping_label_service import ShippingLabelService

# creating an auth router
label_router = APIRouter(
    tags=["shipping_label"],
)


@label_router.post("/shipment/generate-label/", status_code=http.HTTPStatus.OK)
async def generate_label(generateLabelRequest: generateLabelRequest):
    try:
        response: GenericResponseModel = ShippingLabelService.generate_label(
            order_ids=generateLabelRequest.order_ids
        )
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while generating labels.",
            )
        )


@label_router.post("/shipment/generate-invoice/", status_code=http.HTTPStatus.OK)
async def generate_invoice(generateLabelRequest: generateLabelRequest):
    try:
        response: GenericResponseModel = ShippingLabelService.generate_invoice(
            order_ids=generateLabelRequest.order_ids
        )
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while generating labels.",
            )
        )


@label_router.get(
    "/settings/shipping-label",
    status_code=http.HTTPStatus.OK,
)
async def get_shipping_label_settings():
    # Call async service method
    response: GenericResponseModel = await ShippingLabelService.get_label_settings()
    return build_api_response(response)


@label_router.post(
    "/settings/shipping-label",
    status_code=http.HTTPStatus.OK,
)
async def update_shipping_label_settings(label_parameters: LabelSettingUpdateModel):
    # Call async service method
    response: GenericResponseModel = await ShippingLabelService.update_label_settings(
        label_parameters=label_parameters
    )
    return build_api_response(response)


@label_router.post(
    "/settings/shipping-label/upload-logo",
    status_code=http.HTTPStatus.OK,
)
async def upload_logo(file: UploadFile = File(...)):
    # Call async service method
    response: GenericResponseModel = await ShippingLabelService.upload_logo(file)
    return build_api_response(response)
