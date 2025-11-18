from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Boolean, Integer, ForeignKey

from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase

import uuid as uuid


class Shipping_Label_Setting(DBBase, DBBaseClass):
    __tablename__ = "shipping_label_setting"

    logo_url = Column(String(255), nullable=True)

    label_format = Column(String, nullable=False)
    order_id_barcode_enabled = Column(Boolean, nullable=False, default=False)
    barcode_format = Column(String, nullable=False)

    logo_shown = Column(Boolean, nullable=False, default=False)
    consignee_phone = Column(Boolean, nullable=False, default=True)
    package_dimensions = Column(Boolean, nullable=False, default=True)
    weight = Column(Boolean, nullable=False, default=True)
    order_date = Column(Boolean, nullable=False, default=True)
    payment_type = Column(Boolean, nullable=False, default=True)
    company_name = Column(Boolean, nullable=False, default=True)
    pickup_address = Column(Boolean, nullable=False, default=True)
    SKU = Column(Boolean, nullable=False, default=False)
    prepaid_amount = Column(Boolean, nullable=False, default=True)
    COD_amount = Column(Boolean, nullable=False, default=True)
    message = Column(Boolean, nullable=False, default=True)
    product_name = Column(Boolean, nullable=False, default=True)

    branding = Column(Boolean, nullable=False, default=True)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    client = relationship("Client", lazy="noload")

    def to_model(self):
        from modules.documents.shipping_label.shipping_label_schema import (
            LabelSettingModel,
        )

        return LabelSettingModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Shipping_Label_Setting(**entity)
