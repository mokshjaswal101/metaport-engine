from pydantic import BaseModel, Json, Field
from typing import Optional, Literal
from datetime import datetime
from decimal import Decimal

# schema
from schema.base import DBBaseModel


class ShippingNotificationsRateBaseModel(BaseModel):
    shipping_notifications: float
    cod_confirmation: float


class ShippingNotificationsRateModel(DBBaseModel, ShippingNotificationsRateBaseModel):
    pass


class ShippingNotificationSettingBaseModel(BaseModel):
    order_processed: bool
    order_shipped: bool
    order_out_for_delivery: bool
    order_delivered: bool


class ShippingNotificationSettingModel(
    DBBaseModel, ShippingNotificationSettingBaseModel
):
    pass


class UpdateShippingNotificationSetting(BaseModel):
    notification: type
    status: bool


class ShippingNotificationsLogsModel(DBBaseModel):
    order_id: Optional[int] = None
    shipment_id: Optional[int] = None
    direction: Literal["sent", "received"]
    sent_at: datetime
    message_type: str
    cost: Decimal
    content: Optional[str] = None
    status: Optional[str] = None
