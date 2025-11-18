import http
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from fastapi import APIRouter, Depends

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from .company_schema import CompanyInsertModel

# utils
from utils.response_handler import build_api_response

# service
from .company_service import CompanyService

# creating a company router
company_router = APIRouter(tags=["company"], prefix="/company")


@company_router.post(
    "/create", status_code=http.HTTPStatus.CREATED, response_model=GenericResponseModel
)
async def create_new_company(
    company_data: CompanyInsertModel,
):
    response: GenericResponseModel = CompanyService.create_company(
        company_data=company_data
    )
    return build_api_response(response)
