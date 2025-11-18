from sqlalchemy.orm import Session, relationship, selectinload
from sqlalchemy import Column, String, Boolean, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import JSONB

from database import DBBaseClass, DBBase

import uuid as uuid


class Company_To_Client_Contract(DBBase, DBBaseClass):
    __tablename__ = "company_to_client_contract"

    company_contract_id = Column(
        Integer, ForeignKey("company_contract.id"), nullable=False
    )

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    aggregator_courier_id = Column(
        Integer, ForeignKey("aggregator_courier.id"), nullable=True
    )

    company_contract = relationship("Company_Contract", lazy="noload")
    aggregator_courier = relationship("Aggregator_Courier", lazy="noload")

    isActive = Column(Boolean, nullable=False, default=True)
    
    rate_type = Column(String(255), nullable=False, default='forward')

    rates = relationship("Company_To_Client_Rates", lazy="noload")
    cod_rates = relationship("Company_To_Client_COD_Rates", lazy="noload")

    def to_model(self):
        from modules.client_contract.client_contract_schema import (
            CompanyToClientContractModel,
        )

        return CompanyToClientContractModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Company_To_Client_Contract(**entity)
