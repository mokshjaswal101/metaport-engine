from sqlalchemy import Column, Integer, ForeignKey, Numeric
from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase


class ShippingNotificationsRate(DBBase, DBBaseClass):
    __tablename__ = "shipping_notifications_rate"

    shipping_notifications = Column(Numeric(5, 2), nullable=True)
    cod_confirmation = Column(Numeric(5, 2), nullable=True)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    client = relationship("Client")

    def to_model(self):
        from modules.shipping_notifications.shipping_notifications_schema import (
            ShippingNotificationsRateModel,
        )

        return ShippingNotificationsRateModel.model_validate(self)
