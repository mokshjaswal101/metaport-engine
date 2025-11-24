"""
WhatsApp Module
Handles WhatsApp messaging via Facebook Graph API
"""

from .whatsapp_service import WhatsappService
from .whatsapp_template_manager import WhatsAppTemplateManager, WhatsAppTemplateType

__all__ = [
    "WhatsappService",
    "WhatsAppTemplateManager",
    "WhatsAppTemplateType",
]
