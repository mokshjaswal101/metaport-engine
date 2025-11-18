from sqlalchemy.orm import Session, relationship
from sqlalchemy import Column, String, ForeignKey, Integer, Numeric
from sqlalchemy.dialects.postgresql import JSONB

from database import DBBaseClass, DBBase

import uuid as uuid


class Company_To_Client_Rates(DBBaseClass, DBBase):
    __tablename__ = "company_to_client_rates"

    contract_id = Column(
        Integer, ForeignKey("company_to_client_contract.id"), nullable=False
    )
    zone = Column(String, nullable=False)

    base_rate = Column(Numeric(10, 2), nullable=False)
    additional_rate = Column(Numeric(10, 2), nullable=False)

    rto_base_rate = Column(Numeric(10, 2), nullable=False)
    rto_additional_rate = Column(Numeric(10, 2), nullable=False)

    # def __to_model(self):
    #     from modules.company_contract.company_contract_schema import (
    #         CompanyContractModel,
    #     )

    #     return CompanyContractModel.model_validate(self)
