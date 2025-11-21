"""
Shopify OAuth Integration Schemas
Pydantic models for request/response validation
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime


class ShopifyOAuthInitiateRequest(BaseModel):
    """Request to initiate Shopify OAuth flow"""
    shop_domain: str = Field(..., description="Shopify store domain (e.g., mystore.myshopify.com)")
    
    @validator('shop_domain')
    def validate_shop_domain(cls, v):
        import re
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-]*\.myshopify\.com$', v):
            raise ValueError('Invalid Shopify shop domain format')
        return v.lower()


class ShopifyOAuthInitiateResponse(BaseModel):
    """Response with OAuth authorization URL"""
    auth_url: str
    state: str
    redirect_uri: Optional[str] = None  # For debugging
    message: str = "Redirect user to auth_url to authorize"


class ShopifyOAuthCallbackRequest(BaseModel):
    """Shopify OAuth callback parameters"""
    code: str
    shop: str
    state: str
    hmac: str
    timestamp: str
    host: Optional[str] = None


class ChannelInfo(BaseModel):
    """Channel master information including logo and type"""
    id: int
    name: str
    slug: str
    channel_type: str  # marketplace, wms, erp, pos
    logo_url: Optional[str] = None

class ShopifyIntegrationInfo(BaseModel):
    """
    Channel integration information (channel-independent)
    Used for listing all integrations across different channels
    """
    integration_id: int
    shop_domain: str  # Store domain or URL
    shop_name: Optional[str] = None  # Custom integration name
    shop_email: Optional[str] = None
    shop_owner: Optional[str] = None
    connection_status: str  # connected, failed, pending, token_expired, uninstalled
    is_active: bool  # Whether integration is active
    auto_sync_enabled: bool  # Whether auto-sync is enabled
    sync_interval_minutes: int
    last_order_sync_at: Optional[datetime] = None
    total_orders_synced: int
    created_at: datetime
    webhook_status: Optional[str] = None  # active, inactive, pending
    last_synced_at: Optional[datetime] = None
    last_webhook_received_at: Optional[datetime] = None
    channel: Optional[ChannelInfo] = None  # Channel details including logo


class ShopifyOrderSyncRequest(BaseModel):
    """Request to sync orders from Shopify"""
    integration_id: int
    since_date: Optional[datetime] = None
    limit: Optional[int] = Field(250, ge=1, le=250)
    status: Optional[str] = None  # "any", "open", "closed", "cancelled"


class ShopifyTestConnectionRequest(BaseModel):
    """Request to test Shopify connection"""
    integration_id: int


class ShopifyTestConnectionResponse(BaseModel):
    """Response from connection test"""
    success: bool
    shop_domain: str
    shop_name: Optional[str] = None
    message: str
    details: Optional[Dict[str, Any]] = None


class ShopifyFulfillmentRequest(BaseModel):
    """Request to create fulfillment in Shopify"""
    order_id: str  # Internal order ID
    awb_number: str
    courier_name: str
    tracking_url: Optional[str] = None
    notify_customer: bool = True


class ShopifyWebhookPayload(BaseModel):
    """Base webhook payload structure"""
    topic: str
    shop_domain: str
    payload: Dict[str, Any]


class ShopifyOrderWebhook(BaseModel):
    """Shopify order webhook data"""
    id: int
    order_number: int
    email: Optional[str] = None
    created_at: str
    updated_at: str
    cancelled_at: Optional[str] = None
    financial_status: str
    fulfillment_status: Optional[str] = None
    total_price: str
    subtotal_price: str
    total_tax: str
    currency: str
    customer: Dict[str, Any]
    billing_address: Optional[Dict[str, Any]] = None
    shipping_address: Optional[Dict[str, Any]] = None
    line_items: List[Dict[str, Any]]
    shipping_lines: List[Dict[str, Any]]
    tags: Optional[str] = None


class ShopifyStoreInfo(BaseModel):
    """Shopify store information"""
    id: int
    name: str
    email: str
    domain: str
    myshopify_domain: str
    shop_owner: str
    phone: Optional[str] = None
    address1: Optional[str] = None
    city: Optional[str] = None
    province: Optional[str] = None
    country: Optional[str] = None
    zip: Optional[str] = None
    currency: str
    timezone: str
    plan_name: str


class WebhookRegistrationStatus(BaseModel):
    """Status of webhook registration"""
    topic: str
    registered: bool
    webhook_id: Optional[int] = None
    error: Optional[str] = None


class ShopifyIntegrationSettings(BaseModel):
    """Configurable settings for Shopify integration"""
    auto_sync_enabled: bool = True
    sync_interval_minutes: int = Field(30, ge=15, le=1440)
    order_statuses_to_fetch: List[str] = ["paid", "unfulfilled"]
    webhook_enabled: bool = True
    auto_fulfill_on_label_creation: bool = True
    sync_fulfilled_orders: bool = False
    auto_create_pickup_location: bool = True

