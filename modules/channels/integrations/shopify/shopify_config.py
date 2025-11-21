"""
Shopify OAuth Integration Configuration
Separate from the existing marketplace/shopify implementation
"""

import os
from typing import List

# Shopify OAuth Configuration
SHOPIFY_API_KEY = os.getenv("SHOPIFY_OAUTH_API_KEY", "")
SHOPIFY_API_SECRET = os.getenv("SHOPIFY_OAUTH_API_SECRET", "")
SHOPIFY_API_VERSION = (
    "2025-07"  # Latest stable version (July 2025) - Shopify 2025 Q3 Release
)

# Validate required configuration
if not SHOPIFY_API_KEY:
    raise ValueError(
        "SHOPIFY_OAUTH_API_KEY environment variable is required but not set. "
        "Get it from: Shopify Partner Dashboard → Your App → Configuration"
    )
if not SHOPIFY_API_SECRET:
    raise ValueError(
        "SHOPIFY_OAUTH_API_SECRET environment variable is required but not set. "
        "Get it from: Shopify Partner Dashboard → Your App → Configuration"
    )

# OAuth Redirect URI (must match what's configured in Shopify Partner Dashboard)
SHOPIFY_OAUTH_REDIRECT_URI = os.getenv("SHOPIFY_OAUTH_REDIRECT_URI")

# OAuth Scopes - Compliant with Shopify requirements
SHOPIFY_SCOPES: List[str] = [
    "read_orders",
    "write_orders",
    "read_fulfillments",
    "write_fulfillments",
    "read_customers",
    "read_products",
    "read_shipping",
]

# Webhook Topics - Only operational webhooks
# Note: GDPR webhooks (customers/redact, shop/redact, customers/data_request)
# are declared in shopify.app.toml but NOT auto-registered via code
WEBHOOK_TOPICS = [
    "orders/create",
    "orders/updated",
    "orders/cancelled",
    "app/uninstalled",
]

# Rate Limiting
SHOPIFY_RATE_LIMIT_MAX_REQUESTS = 2  # 2 requests per second for REST API
SHOPIFY_RATE_LIMIT_WINDOW = 1  # 1 second


# OAuth URLs
def get_shopify_oauth_url(shop_domain: str, state: str, redirect_uri: str) -> str:
    """Generate Shopify OAuth authorization URL"""
    from urllib.parse import urlencode

    params = {
        "client_id": SHOPIFY_API_KEY,
        "scope": ",".join(SHOPIFY_SCOPES),
        "redirect_uri": redirect_uri,
        "state": state,
    }

    base_url = f"https://{shop_domain}/admin/oauth/authorize"
    return f"{base_url}?{urlencode(params)}"


def get_shopify_token_url(shop_domain: str) -> str:
    """Get Shopify OAuth token exchange URL"""
    return f"https://{shop_domain}/admin/oauth/access_token"


# API URL Builder
def get_shopify_api_url(shop_domain: str, endpoint: str) -> str:
    """Build Shopify Admin API URL"""
    return f"https://{shop_domain}/admin/api/{SHOPIFY_API_VERSION}/{endpoint}"


def get_shopify_graphql_url(shop_domain: str) -> str:
    """Get Shopify GraphQL API URL"""
    return f"https://{shop_domain}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"


# Validation
def validate_shop_domain(shop: str) -> bool:
    """Validate Shopify shop domain format"""
    import re

    pattern = r"^[a-zA-Z0-9][a-zA-Z0-9\-]*\.myshopify\.com$"
    return bool(re.match(pattern, shop))


# Webhook validation
def verify_webhook_hmac(data: bytes, hmac_header: str) -> bool:
    """Verify Shopify webhook HMAC signature"""
    import hmac as hmac_lib
    import hashlib
    import base64

    # Validate secret is set
    if not SHOPIFY_API_SECRET:
        from logger import logger

        logger.error(
            "SHOPIFY_API_SECRET not set - webhook HMAC verification will fail!"
        )
        return False

    computed_hmac = base64.b64encode(
        hmac_lib.new(SHOPIFY_API_SECRET.encode("utf-8"), data, hashlib.sha256).digest()
    ).decode()

    return hmac_lib.compare_digest(computed_hmac, hmac_header)


def verify_oauth_hmac(query_params: dict) -> bool:
    """Verify Shopify OAuth callback HMAC"""
    import hmac as hmac_lib
    import hashlib
    from urllib.parse import urlencode

    # Validate secret is set
    if not SHOPIFY_API_SECRET:
        from logger import logger

        logger.error("SHOPIFY_API_SECRET not set - OAuth HMAC verification will fail!")
        return False

    params = {k: v for k, v in query_params.items() if k != "hmac"}
    message = urlencode(sorted(params.items()))

    computed_hmac = hmac_lib.new(
        SHOPIFY_API_SECRET.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    return hmac_lib.compare_digest(computed_hmac, query_params.get("hmac", ""))
