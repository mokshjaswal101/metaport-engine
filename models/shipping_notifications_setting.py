from sqlalchemy import Column, Integer, ForeignKey, Numeric, Boolean
from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase


class ShippingNotificationsSetting(DBBase, DBBaseClass):
    __tablename__ = "shipping_notifications_setting"

    order_processed = Column(Boolean, nullable=False, default=False)
    order_shipped = Column(Boolean, nullable=False, default=False)
    order_out_for_delivery = Column(Boolean, nullable=False, default=False)
    order_delivered = Column(Boolean, nullable=False, default=False)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    client = relationship("Client")

    def to_model(self):
        from modules.shipping_notifications.shipping_notifications_schema import (
            ShippingNotificationSettingModel,
        )

        return ShippingNotificationSettingModel.model_validate(self)
