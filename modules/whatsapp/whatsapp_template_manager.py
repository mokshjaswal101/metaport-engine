"""
WhatsApp Template Manager
Manages WhatsApp message templates for different use cases.
"""

from typing import Dict, List, Any, Optional
from enum import Enum


class WhatsAppTemplateType(Enum):
    """Enum for different WhatsApp template types"""

    OTP_VERIFICATION = "metaport_otp"
    ORDER_PROCESSING = "order_processing_3"
    ORDER_SHIPPED = "order_shipped_2"
    ORDER_CONFIRMATION = "order_confirmation_url"


class WhatsAppTemplateManager:
    """
    Centralized manager for WhatsApp message templates.
    Makes it easy to add, modify, and manage templates.
    """

    @staticmethod
    def get_otp_template(otp_code: str, phone_number: str) -> Dict[str, Any]:
        """
        Build OTP verification template payload.

        Args:
            otp_code: 6-digit OTP code
            phone_number: Recipient phone number (with country code)

        Returns:
            dict: Complete WhatsApp message payload
        """
        return {
            "messaging_product": "whatsapp",
            "to": phone_number,
            "type": "template",
            "template": {
                "name": WhatsAppTemplateType.OTP_VERIFICATION.value,
                "language": {"code": "en"},
                "components": [
                    {
                        "type": "body",
                        "parameters": [{"type": "text", "text": otp_code}],
                    },
                    {
                        "type": "button",
                        "sub_type": "url",
                        "index": "0",
                        "parameters": [{"type": "text", "text": otp_code}],
                    },
                ],
            },
        }

    @staticmethod
    def get_order_processing_template(
        customer_name: str, client_name: str, order_id: str, phone_number: str
    ) -> Dict[str, Any]:
        """
        Build order processing notification template.

        Args:
            customer_name: Customer's full name
            client_name: Client/brand name
            order_id: Order ID
            phone_number: Recipient phone number (with country code)

        Returns:
            dict: Complete WhatsApp message payload
        """
        return {
            "messaging_product": "whatsapp",
            "to": phone_number,
            "type": "template",
            "template": {
                "name": WhatsAppTemplateType.ORDER_PROCESSING.value,
                "language": {"code": "en"},
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": customer_name},
                            {"type": "text", "text": client_name},
                            {"type": "text", "text": order_id},
                        ],
                    }
                ],
            },
        }

    @staticmethod
    def get_order_shipped_template(
        customer_name: str,
        client_name: str,
        order_id: str,
        awb_number: str,
        phone_number: str,
    ) -> Dict[str, Any]:
        """
        Build order shipped notification template.

        Args:
            customer_name: Customer's full name
            client_name: Client/brand name
            order_id: Order ID
            awb_number: AWB tracking number
            phone_number: Recipient phone number (with country code)

        Returns:
            dict: Complete WhatsApp message payload
        """
        return {
            "messaging_product": "whatsapp",
            "to": phone_number,
            "type": "template",
            "template": {
                "name": WhatsAppTemplateType.ORDER_SHIPPED.value,
                "language": {"code": "en"},
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": customer_name},
                            {"type": "text", "text": client_name},
                            {"type": "text", "text": order_id},
                        ],
                    },
                    {
                        "type": "button",
                        "sub_type": "url",
                        "index": "0",
                        "parameters": [{"type": "text", "text": awb_number}],
                    },
                ],
            },
        }

    @staticmethod
    def get_order_confirmation_template(
        customer_name: str,
        client_name: str,
        product_summary: str,
        total_amount: str,
        payment_mode: str,
        order_uuid: str,
        phone_number: str,
    ) -> Dict[str, Any]:
        """
        Build order confirmation template.

        Args:
            customer_name: Customer's full name
            client_name: Client/brand name
            product_summary: Summary of products
            total_amount: Total order amount
            payment_mode: Payment mode (COD, Prepaid, etc.)
            order_uuid: Order UUID for tracking links
            phone_number: Recipient phone number (with country code)

        Returns:
            dict: Complete WhatsApp message payload
        """
        return {
            "messaging_product": "whatsapp",
            "to": phone_number,
            "type": "template",
            "template": {
                "name": WhatsAppTemplateType.ORDER_CONFIRMATION.value,
                "language": {"code": "en"},
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": customer_name},
                            {"type": "text", "text": client_name},
                            {"type": "text", "text": product_summary},
                            {"type": "text", "text": total_amount},
                            {"type": "text", "text": payment_mode},
                        ],
                    },
                    {
                        "type": "button",
                        "sub_type": "url",
                        "index": "0",
                        "parameters": [{"type": "text", "text": order_uuid}],
                    },
                    {
                        "type": "button",
                        "sub_type": "url",
                        "index": "1",
                        "parameters": [{"type": "text", "text": order_uuid}],
                    },
                ],
            },
        }

    @staticmethod
    def validate_template(template_type: WhatsAppTemplateType) -> bool:
        """
        Validate if a template type is supported.

        Args:
            template_type: Template type to validate

        Returns:
            bool: True if valid, False otherwise
        """
        return template_type in WhatsAppTemplateType

    @staticmethod
    def get_all_templates() -> List[str]:
        """
        Get list of all available template names.

        Returns:
            list: List of template names
        """
        return [template.value for template in WhatsAppTemplateType]
