from sqlalchemy.orm import Session, relationship
from sqlalchemy import Column, String, ForeignKey, Integer, Numeric
from sqlalchemy.dialects.postgresql import JSONB

from database import DBBaseClass, DBBase

import uuid as uuid


class Company_To_Client_COD_Rates(DBBaseClass, DBBase):
    __tablename__ = "company_to_client_cod_rates"

    contract_id = Column(
        Integer, ForeignKey("company_to_client_contract.id"), nullable=False
    )

    percentage_rate = Column(Numeric(5, 2), nullable=False)  # COD percentage rate
    absolute_rate = Column(Numeric(10, 2), nullable=False)  # COD absolute rate

    def __to_model(self):
        from modules.company_contract.company_contract_schema import (
            CompanyContractModel,
        )

        return CompanyContractModel.model_validate(self)
