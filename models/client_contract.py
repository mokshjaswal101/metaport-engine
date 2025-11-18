from sqlalchemy.orm import Session, relationship
from sqlalchemy import Column, String, Boolean, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import JSON

from database import DBBaseClass, DBBase

import uuid as uuid


class Client_Contract(DBBase, DBBaseClass):
    __tablename__ = "client_contract"

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    shipping_partner_id = Column(
        Integer, ForeignKey("shipping_partner.id"), nullable=False
    )

    company = relationship("Client", lazy="noload")
    shipping_partner = relationship("Shipping_Partner", lazy="noload")

    credentials = Column(JSON, nullable=False)

    tracking_series = Column(Integer)

    isActive = Column(Boolean, nullable=False, default=True)

    def __to_model(self):
        from modules.company_contract.company_contract_schema import (
            CompanyContractModel,
        )

        return CompanyContractModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Client_Contract(**entity)

    @classmethod
    def create_courier(cls, courier_data):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        db.add(courier_data)
        db.flush()

        return courier_data.__to_model()
