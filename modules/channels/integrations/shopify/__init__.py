"""
Shopify OAuth Integration Module
This is a completely separate implementation from marketplace/shopify
Uses OAuth 2.0 flow and Channel Master system
"""

from .shopify_oauth_controller import (
    shopify_oauth_router,
    shopify_management_router,
    shopify_oauth_public_router,
)
from .shopify_webhook_controller import webhook_router
from .shopify_connector import ShopifyConnector
from .shopify_order_sync import ShopifyOrderSync
from .shopify_fulfillment_service import ShopifyFulfillmentService

# Combine all routers
from fastapi import APIRouter

# OAuth-specific endpoints (require user login) - for OAuth initiate only
shopify_oauth_integration_router = APIRouter(prefix="/channels/shopify-oauth", tags=["Shopify OAuth"])
shopify_oauth_integration_router.include_router(shopify_oauth_router)

# Shopify management endpoints (require user login) - for integration operations
shopify_management_integration_router = APIRouter(prefix="/channels/shopify", tags=["Shopify Management"])
shopify_management_integration_router.include_router(shopify_management_router)

# Public endpoints (no authentication - for Shopify OAuth callbacks and webhooks)
shopify_public_router = APIRouter(prefix="/api/v1/channels", tags=["Shopify Public"])
shopify_public_router.include_router(shopify_oauth_public_router)
shopify_public_router.include_router(webhook_router, prefix="/shopify")

__all__ = [
    "shopify_oauth_integration_router",  # OAuth initiate endpoint
    "shopify_management_integration_router",  # Management operations
    "shopify_public_router",  # Public webhooks and OAuth callback
    "shopify_oauth_router",
    "shopify_management_router",
    "webhook_router",
    "ShopifyConnector",
    "ShopifyOrderSync",
    "ShopifyFulfillmentService",
]

