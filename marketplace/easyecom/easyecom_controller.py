import http
from fastapi import APIRouter, Depends, HTTPException
from fastapi import APIRouter, Depends
from typing import Any
from fastapi import Request
import json
from fastapi.responses import Response
from schema.base import GenericResponseModel
from .easyecom_schema import (
    AuthInsertModel,
    ShippingInsertModel,
    CancelShipmentModel,
    EasyEcomAccessToken,
    UpdateTrackingStatus,
)
from .easyecom_service import EasyEcomService
from modules.documents.shipping_label.shipping_label_service import ShippingLabelService

# from utils.helper import build_api_response

easyecom_router = APIRouter(prefix="/easyecom", tags=["easyEcom"])


@easyecom_router.post(
    "/authenticate",
    status_code=http.HTTPStatus.CREATED,
    #   response_model=GenericResponseModel
)
#
async def authenticate(auth_data: AuthInsertModel):
    response = EasyEcomService.authenticate(auth_data=auth_data)
    # Extract the correct status code from the response body
    status_code = response.get("code", 200)  # Default to 200 if not present

    return Response(
        content=json.dumps(response),
        status_code=status_code,
        media_type="application/json",
    )


@easyecom_router.post("/createShipment", status_code=http.HTTPStatus.OK)
async def createShipment(shipment_data: Request):

    shipment_data = await shipment_data.json()
    # print(shipment_data)
    response = EasyEcomService.createShipment(shipment_data=shipment_data)
    return response


@easyecom_router.post("/cancelShipment", status_code=http.HTTPStatus.CREATED)
#
async def cancelShipment(shipment_cancel_data: CancelShipmentModel):
    response = EasyEcomService.cancelShipment(shipment_cancel_data=shipment_cancel_data)
    return response


@easyecom_router.get("/label/{awb}", status_code=http.HTTPStatus.CREATED)
#
async def generate_label(awb: str):

    # generate the thermal lable and directly download it
    response = EasyEcomService.generate_label(awb=awb)
    return response


@easyecom_router.post("/update_status", status_code=http.HTTPStatus.OK)
async def update_order_status(request: Request):
    """
    Manual endpoint to update order status back to EasyEcom.
    Expected payload: {"order_id": "string", "client_id": int}
    """
    try:
        request_data = await request.json()
        order_id = request_data.get("order_id")
        client_id = request_data.get("client_id")

        if not order_id or not client_id:
            return {
                "code": http.HTTPStatus.BAD_REQUEST,
                "message": "order_id and client_id are required",
            }

        from models import Order
        from context_manager.context import get_db_session

        with get_db_session() as db:
            order = (
                db.query(Order)
                .filter(Order.order_id == order_id, Order.client_id == client_id)
                .first()
            )

            if not order:
                return {"code": http.HTTPStatus.NOT_FOUND, "message": "Order not found"}

            # Only update EasyEcom orders
            if order.source != "easyecom":
                return {
                    "code": http.HTTPStatus.BAD_REQUEST,
                    "message": "Order is not from EasyEcom",
                }

        response = EasyEcomService.update_order_status_to_easyecom(order)
        return response

    except Exception as e:
        return {
            "code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
            "message": f"Error: {str(e)}",
        }
