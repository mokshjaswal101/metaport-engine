import http
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from fastapi import APIRouter, Depends

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response

# service
from .billing_invoice_service import BillingInvoiceService

# creating a client router
billing_invoice_router = APIRouter(tags=["client"], prefix="/wallet/billing-invoice")


@billing_invoice_router.get(
    "/", status_code=http.HTTPStatus.OK, response_model=GenericResponseModel
)
async def get_invoice():
    response: GenericResponseModel = BillingInvoiceService.get_invoice()
    return build_api_response(response)


@billing_invoice_router.get("/download", status_code=http.HTTPStatus.OK)
async def get_invoice(id: str):
    response: GenericResponseModel = BillingInvoiceService.download_invoice(
        invoice_id=id
    )
    return response
