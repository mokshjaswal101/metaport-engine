from fastapi import APIRouter, Depends, HTTPException, Header, Request, Query, Body
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Any
import json
from datetime import datetime
import secrets
import hmac
import hashlib
import base64

from context_manager.context import context_user_data, get_db_session
from fastapi.encoders import jsonable_encoder

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response

# service
from .magento_service import Magento

# models
from models import Market_Place, Order

# creating a router
magento_router = APIRouter(tags=["magento"])

# Configuration
API_SECRET = "last_miles_magento_secret"  # Use a proper secret in production
WEBHOOK_BASE_URL = "https://api.lastmiles.co"  # Update with your actual base URL


def verify_webhook_signature(data: bytes, signature: str) -> bool:
    """
    Verify webhook signature from Magento

    Args:
        data: Raw request body
        signature: HMAC signature from headers

    Returns:
        bool: True if signature is valid
    """
    computed_hmac = hmac.new(
        API_SECRET.encode("utf-8"), data, hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(computed_hmac, signature)


@magento_router.post(
    "/market-place/magento/webhook", response_model=GenericResponseModel
)
async def magento_webhook(
    request: Request, x_magento_hmac_sha256: Optional[str] = Header(None)
):
    """
    Endpoint to receive webhooks from Magento

    Args:
        request: FastAPI request object
        x_magento_hmac_sha256: HMAC signature from Magento

    Returns:
        JSONResponse: Processing result
    """
    try:
        # Get raw request body
        body = await request.body()

        # Verify webhook signature if available
        if x_magento_hmac_sha256:
            if not verify_webhook_signature(body, x_magento_hmac_sha256):
                raise HTTPException(status_code=401, detail="Invalid webhook signature")

        # Parse event data
        event_data = json.loads(body)

        # Get client_id and marketplace_id from the event data or query params
        # This might need adaptation based on how you structure your webhooks
        client_id = request.query_params.get("client_id")
        marketplace_id = request.query_params.get("marketplace_id")

        if not client_id or not marketplace_id:
            # Try to extract from the event data
            store_id = event_data.get("store_id")

            if store_id:
                # Look up the client_id and marketplace_id from the database
                with get_db_session() as db:
                    marketplace = (
                        db.query(Market_Place)
                        .filter(Market_Place.store_id == store_id)
                        .first()
                    )

                    if marketplace:
                        client_id = marketplace.client_id
                        marketplace_id = marketplace.id
                    else:
                        raise HTTPException(
                            status_code=404,
                            detail=f"No marketplace found for store_id: {store_id}",
                        )
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Missing client_id and marketplace_id in request",
                )

        # Initialize Magento service
        magento_service = Magento(int(client_id), int(marketplace_id))

        # Process the webhook event
        result = magento_service.handle_webhook_event(event_data)

        return build_api_response(data=result, message="Webhook processed successfully")

    except HTTPException as he:
        # Re-raise HTTP exceptions
        raise he
    except Exception as e:
        # Log the error and return a 500 response
        from logger import logger

        logger.error(f"Error processing Magento webhook: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error processing webhook: {str(e)}"
        )


@magento_router.post(
    "/market-place/magento/sync-orders", response_model=GenericResponseModel
)
async def sync_magento_orders(
    client_id: int = 2,
    marketplace_id: int = 1,
    days_back: int = Query(7, description="Number of days to look back for orders"),
):
    """
    Sync orders from Magento to LastMiles

    Args:
        client_id: Client ID
        marketplace_id: Marketplace ID
        days_back: Number of days to look back for orders

    Returns:
        JSONResponse: Synced orders
    """
    try:
        # Initialize Magento service
        magento_service = Magento(client_id, marketplace_id)

        print(magento_service)

        # Fetch and sync orders
        synced_orders = magento_service.fetch_and_sync_orders(days_back)

        print(synced_orders)

        return build_api_response(
            data={"orders": synced_orders, "count": len(synced_orders)},
            message=f"Successfully synced {len(synced_orders)} orders from Magento",
        )

    except Exception as e:
        # Log the error and return a 500 response
        from logger import logger

        logger.error(f"Error syncing Magento orders: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error syncing orders: {str(e)}")


@magento_router.post(
    "/market-place/magento/test-connection", response_model=GenericResponseModel
)
async def test_magento_connection(
    client_id: int = 2,
    marketplace_id: int = 1,
):
    """
    Test connection to Magento store

    Args:
        client_id: Client ID
        marketplace_id: Marketplace ID

    Returns:
        JSONResponse: Connection test result
    """
    try:
        # Initialize Magento service
        magento_service = Magento(client_id, marketplace_id)

        # Test connection
        connection_result = magento_service.test_connection()

        if connection_result.get("status") == "success":
            return build_api_response(
                data=connection_result,
                message="Magento connection test successful",
            )
        else:
            return build_api_response(
                data=connection_result,
                message="Magento connection test failed",
                status_code=400,
            )

    except Exception as e:
        # Log the error and return a 500 response
        from logger import logger

        logger.error(f"Error testing Magento connection: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Connection test error: {str(e)}")


@magento_router.get(
    "/market-place/magento/check-store-type", response_model=GenericResponseModel
)
async def check_magento_store_type(
    client_id: int = 2,
    marketplace_id: int = 1,
):
    """
    Check what type of e-commerce platform is running on the given URL

    Args:
        client_id: Client ID
        marketplace_id: Marketplace ID

    Returns:
        JSONResponse: Store type detection result
    """
    try:
        import requests

        # Get the base URL from credentials
        magento_service = Magento(client_id, marketplace_id)
        base_url = magento_service.base_url

        logger.info(f"Checking store type for: {base_url}")

        # Check different endpoints to determine platform type
        checks = []

        try:
            # Check main page
            main_response = requests.get(base_url, timeout=10)
            main_content = main_response.text.lower()

            checks.append(
                {
                    "endpoint": "/",
                    "status_code": main_response.status_code,
                    "detected_platforms": [],
                }
            )

            if "magento" in main_content or "mage" in main_content:
                checks[-1]["detected_platforms"].append("Magento")
            if "shopify" in main_content:
                checks[-1]["detected_platforms"].append("Shopify")
            if "woocommerce" in main_content or "wordpress" in main_content:
                checks[-1]["detected_platforms"].append("WooCommerce")
            if "opencart" in main_content:
                checks[-1]["detected_platforms"].append("OpenCart")

        except Exception as e:
            checks.append({"endpoint": "/", "error": str(e)})

        # Check specific API endpoints
        api_endpoints = [
            "/rest/V1",
            "/api/rest",
            "/rest",
            "/api",
            "/admin",
            "/index.php",
        ]

        for endpoint in api_endpoints:
            try:
                url = f"{base_url}{endpoint}"
                response = requests.get(url, timeout=5)
                checks.append(
                    {
                        "endpoint": endpoint,
                        "status_code": response.status_code,
                        "content_type": response.headers.get("content-type", "unknown"),
                        "content_preview": (
                            response.text[:200] if response.status_code == 200 else None
                        ),
                    }
                )
            except Exception as e:
                checks.append({"endpoint": endpoint, "error": str(e)})

        return build_api_response(
            data={
                "base_url": base_url,
                "checks": checks,
                "credentials": {
                    "username": magento_service.username,
                    "base_url": base_url,
                },
            },
            message="Store type check completed",
        )

    except Exception as e:
        from logger import logger

        logger.error(f"Error checking store type: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Store type check error: {str(e)}")


@magento_router.post(
    "/market-place/magento/register-webhook", response_model=GenericResponseModel
)
async def register_magento_webhook(
    client_id: int,
    marketplace_id: int,
    webhook_type: str = Query(
        "orders", description="Type of webhook to register (orders, products, etc.)"
    ),
):
    """
    Register a webhook in Magento

    Args:
        client_id: Client ID
        marketplace_id: Marketplace ID
        webhook_type: Type of webhook to register

    Returns:
        JSONResponse: Registration result
    """
    try:
        # Initialize Magento service
        magento_service = Magento(client_id, marketplace_id)

        # Generate a random token for webhook verification
        webhook_token = secrets.token_hex(16)

        # Store the webhook token in the database
        with get_db_session() as db:
            marketplace = (
                db.query(Market_Place)
                .filter(
                    Market_Place.client_id == client_id,
                    Market_Place.id == marketplace_id,
                )
                .first()
            )

            if not marketplace:
                raise HTTPException(
                    status_code=404,
                    detail=f"No marketplace found for client_id: {client_id}, marketplace_id: {marketplace_id}",
                )

            # Store the token in webhook_token field or similar
            # This assumes such a field exists in your Market_Place model
            marketplace.webhook_token = webhook_token
            db.commit()

        # Create the webhook URL with the token
        webhook_url = f"{WEBHOOK_BASE_URL}/api/v1/market-place/magento/webhook?client_id={client_id}&marketplace_id={marketplace_id}&token={webhook_token}"

        # Register the webhook in Magento
        result = magento_service.create_webhook(webhook_url, webhook_type)

        return build_api_response(
            data=result,
            message=f"Successfully registered {webhook_type} webhook in Magento",
        )

    except Exception as e:
        # Log the error and return a 500 response
        from logger import logger

        logger.error(f"Error registering Magento webhook: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error registering webhook: {str(e)}"
        )


@magento_router.post(
    "/market-place/magento/update-order-status", response_model=GenericResponseModel
)
async def update_magento_order_status(
    client_id: int,
    marketplace_id: int,
    order_id: str,
    status: str,
    comment: Optional[str] = None,
):
    """
    Update order status in Magento

    Args:
        client_id: Client ID
        marketplace_id: Marketplace ID
        order_id: Magento order ID
        status: New order status
        comment: Optional comment for the status change

    Returns:
        JSONResponse: Update result
    """
    try:
        # Initialize Magento service
        magento_service = Magento(client_id, marketplace_id)

        # Update order status
        result = magento_service.update_order_status(order_id, status, comment)

        return build_api_response(
            data={"success": result},
            message=f"Successfully updated order status to {status} in Magento",
        )

    except Exception as e:
        # Log the error and return a 500 response
        from logger import logger

        logger.error(f"Error updating Magento order status: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error updating order status: {str(e)}"
        )


@magento_router.post(
    "/market-place/magento/create-shipment", response_model=GenericResponseModel
)
async def create_magento_shipment(
    client_id: int, marketplace_id: int, order_id: str, tracking_info: Dict = Body(...)
):
    """
    Create a shipment for an order in Magento

    Args:
        client_id: Client ID
        marketplace_id: Marketplace ID
        order_id: Magento order ID
        tracking_info: Dict with tracking information

    Returns:
        JSONResponse: Shipment creation result
    """
    try:
        # Initialize Magento service
        magento_service = Magento(client_id, marketplace_id)

        # Create shipment
        result = magento_service.create_shipment(order_id, tracking_info)

        return build_api_response(
            data=result,
            message=f"Successfully created shipment for order {order_id} in Magento",
        )

    except Exception as e:
        # Log the error and return a 500 response
        from logger import logger

        logger.error(f"Error creating Magento shipment: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error creating shipment: {str(e)}"
        )


@magento_router.get(
    "/market-place/magento/get-order/{order_id}", response_model=GenericResponseModel
)
async def get_magento_order(client_id: int, marketplace_id: int, order_id: str):
    """
    Get order details from Magento

    Args:
        client_id: Client ID
        marketplace_id: Marketplace ID
        order_id: Magento order ID

    Returns:
        JSONResponse: Order details
    """
    try:
        # Initialize Magento service
        magento_service = Magento(client_id, marketplace_id)

        # Get order
        magento_order = magento_service.get_order_by_id(order_id)

        # Map to LastMiles format
        lastmiles_order = magento_service.map_magento_order_to_lastmiles(magento_order)

        return build_api_response(
            data={"magento_order": magento_order, "lastmiles_order": lastmiles_order},
            message=f"Successfully retrieved order {order_id} from Magento",
        )

    except Exception as e:
        # Log the error and return a 500 response
        from logger import logger

        logger.error(f"Error getting Magento order: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting order: {str(e)}")
