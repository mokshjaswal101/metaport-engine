from .default_label import generate_default_label
from .thermal_label import generate_thermal_label
from modules.orders.order_schema import Order_Model
from modules.documents.shipping_label.shipping_label_schema import (
    LabelSettingResponseModel,
)


class LabelTemplateFactory:
    """Factory class for creating shipping label templates"""

    @staticmethod
    def create_label(
        order: Order_Model,
        settings: LabelSettingResponseModel,
        client_name: str,
        client_id: int,
    ) -> str:

        if settings.label_format == "thermal":
            return generate_thermal_label(order, settings, client_name, client_id)
        else:
            return generate_default_label(order, settings, client_name, client_id)
