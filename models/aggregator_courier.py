from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Boolean, Integer, ForeignKey, Numeric
from sqlalchemy.dialects.postgresql import JSONB

from database import DBBaseClass, DBBase

import uuid as uuid


class Aggregator_Courier(DBBase, DBBaseClass):
    __tablename__ = "aggregator_courier"

    aggregator_id = Column(Integer, ForeignKey("shipping_partner.id"), nullable=False)

    name = Column(String(255), nullable=False)
    slug = Column(String(255), nullable=False)
    aggregator_slug = Column(String(255), nullable=False)

    mode = Column(String(255), nullable=True)
    logo = Column(String(255), nullable=True)

    min_chargeable_weight = Column(Numeric(10, 2), nullable=False)
    additional_weight_bracket = Column(Numeric(10, 2), nullable=False)

    def __to_model(self):
        from modules.shipping_partner.shipping_partner_schema import (
            AggregatorCourierModel,
        )

        return AggregatorCourierModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Aggregator_Courier(**entity)

    @classmethod
    def get_by_id(cls, id):
        partner = super().get_by_id(id)
        return partner.__to_model() if partner else None

    @classmethod
    def get_by_uuid(cls, uuid):
        partner = super().get_by_uuid(uuid)
        return partner.__to_model() if partner else None
