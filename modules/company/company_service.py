import http
from psycopg2 import DatabaseError
from typing import Optional, Any

from context_manager.context import context_user_data

# models
from models import Company

# schema
from schema.base import GenericResponseModel
from .company_schema import CompanyInsertModel

from logger import logger

from models.shipping_partner import Shipping_Partner
from models.company_contract import Company_Contract


class CompanyService:

    @staticmethod
    def create_company(
        company_data: CompanyInsertModel,
    ) -> GenericResponseModel:
        try:

            # convert the received object into an instance of the company model
            company_entity = Company.create_db_entity(company_data)

            # add user to database
            created_company = Company.create_company(company_entity)

            logger.info(
                msg="Company created successfully with id {}".format(
                    created_company.uuid
                ),
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Company created successfully",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating company: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Company.",
            )
