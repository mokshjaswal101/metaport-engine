import json
from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    TIMESTAMP,
    Enum,
    Text,
    Numeric,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship
from datetime import datetime
from pytz import timezone

from database import DBBaseClass, DBBase


UTC = timezone("UTC")


def time_now():
    return datetime.now(UTC)


class ShippingNotificationLogs(DBBase, DBBaseClass):

    __tablename__ = "shipping_notification_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("order.id"), nullable=False)
    direction = Column(
        Enum("sent", "received", name="message_direction"), nullable=False
    )
    sent_at = Column(TIMESTAMP(timezone=True), default=time_now, nullable=False)
    content = Column(String, nullable=True)

    message_type = Column(
        String, nullable=True
    )  # e.g. "order_confirmation", "follow_up"

    cost = Column(Numeric(10, 2), default=0.00, nullable=False)
    status = Column(String, nullable=True)

    def to_model(self):
        from modules.shipping_notifications.shipping_notifications_schema import (
            ShippingNotificationsLogsModel,
        )

        return ShippingNotificationsLogsModel.model_validate(self)
