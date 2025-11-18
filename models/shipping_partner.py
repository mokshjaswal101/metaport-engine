from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Boolean
from sqlalchemy.dialects.postgresql import JSON

from database import DBBaseClass, DBBase

import uuid as uuid


class Shipping_Partner(DBBase, DBBaseClass):
    __tablename__ = "shipping_partner"

    name = Column(String(255), nullable=False)
    is_aggregator = Column(Boolean, nullable=False, default=False)
    slug = Column(String(255), nullable=False)

    mode = Column(String(255), nullable=True)
    credentials_schema = Column(JSON, nullable=False, default={})
    logo = Column(String(255), nullable=True)

    def __to_model(self):
        from modules.shipping_partner.shipping_partner_schema import (
            ShippingPartnerModel,
        )

        return ShippingPartnerModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Shipping_Partner(**entity)

    @classmethod
    def create_courier(cls, partner_data):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        db.add(partner_data)
        db.flush()

        return partner_data.__to_model()

    @classmethod
    def get_by_id(cls, id):
        partner = super().get_by_id(id)
        return partner.__to_model() if partner else None

    @classmethod
    def get_by_uuid(cls, uuid):
        partner = super().get_by_uuid(uuid)
        return partner.__to_model() if partner else None
