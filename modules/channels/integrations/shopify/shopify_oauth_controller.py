"""
Shopify OAuth Controller
Handles OAuth flow, webhooks, and integrations
Completely separate from marketplace/shopify
"""

import http
import httpx
import jwt
from fastapi import APIRouter, Request, Header, Query, Depends
from sqlalchemy.orm import Session
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.security import HTTPBearer
from typing import Optional
from datetime import datetime, timedelta

from context_manager.context import get_db_session, context_user_data
from database.db import get_db
from schema.base import GenericResponseModel
from utils.response_handler import build_api_response
from utils.credential_encryption import encrypt_credentials, decrypt_credentials
from logger import logger

from models import (
    ClientChannelIntegration,
    ChannelMaster,
    IntegrationAuditLog,
    IntegrationSyncLog,
)

from .shopify_config import (
    SHOPIFY_API_KEY,
    SHOPIFY_API_SECRET,
    SHOPIFY_OAUTH_REDIRECT_URI,
    get_shopify_oauth_url,
    get_shopify_token_url,
    verify_oauth_hmac,
    verify_webhook_hmac,
    validate_shop_domain,
)
from .shopify_oauth_schema import (
    ShopifyOAuthInitiateRequest,
    ShopifyOAuthInitiateResponse,
    ShopifyTestConnectionRequest,
    ShopifyIntegrationInfo,
    ChannelInfo,
)
from .shopify_connector import ShopifyConnector


# OAuth-specific endpoints (initiate OAuth flow)
shopify_oauth_router = APIRouter(tags=["Shopify OAuth"])

# Shopify management endpoints (pause, resume, sync, etc.)
shopify_management_router = APIRouter(tags=["Shopify Management"])

# Public endpoints (no authentication required - for Shopify OAuth callbacks)
shopify_oauth_public_router = APIRouter(
    prefix="/shopify-oauth", tags=["Shopify OAuth Public"]
)

security = HTTPBearer()


# JWT-based OAuth state management (no database storage needed)
def generate_oauth_state_token(client_id: int, shop_domain: str) -> str:
    """
    Generate a signed JWT token as OAuth state
    Contains client_id and shop_domain, expires in 15 minutes
    """
    payload = {
        "client_id": client_id,
        "shop_domain": shop_domain,
        "exp": datetime.utcnow() + timedelta(minutes=15),
        "iat": datetime.utcnow(),
        "type": "shopify_oauth",
    }
    # Sign with Shopify API secret
    state_token = jwt.encode(payload, SHOPIFY_API_SECRET, algorithm="HS256")
    return state_token


def verify_oauth_state_token(state: str) -> dict:
    """
    Verify and decode the JWT state token
    Returns the payload if valid, raises exception if invalid/expired
    """
    try:
        payload = jwt.decode(state, SHOPIFY_API_SECRET, algorithms=["HS256"])

        # Verify it's the correct token type
        if payload.get("type") != "shopify_oauth":
            raise ValueError("Invalid token type")

        return payload
    except jwt.ExpiredSignatureError:
        raise ValueError("State token expired (15 minutes)")
    except jwt.InvalidTokenError as e:
        raise ValueError(f"Invalid state token: {str(e)}")


@shopify_oauth_router.post("/initiate", response_model=GenericResponseModel)
async def initiate_oauth(
    request: Request,
    request_data: ShopifyOAuthInitiateRequest,
):
    """
    Initiate Shopify OAuth flow
    Step 1: Generate OAuth URL and redirect user to Shopify
    """
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        shop_domain = request_data.shop_domain.lower()

        # Validate shop domain format
        if not validate_shop_domain(shop_domain):
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.BAD_REQUEST,
                message="Invalid Shopify shop domain format",
                data={},
            )

        # Generate JWT-based state token (self-contained, no DB storage needed)
        state = generate_oauth_state_token(client_id, shop_domain)

        # Use configured redirect URI (from environment variable or config)
        redirect_uri = SHOPIFY_OAUTH_REDIRECT_URI

        # Log for debugging
        logger.info(
            f"OAuth initiated for shop: {shop_domain}, "
            f"client_id: {client_id}, "
            f"state token generated (JWT-based)"
        )

        # Generate OAuth authorization URL
        auth_url = get_shopify_oauth_url(shop_domain, state, redirect_uri)

        logger.info(f"Generated auth_url: {auth_url}")

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="OAuth URL generated successfully",
            data=ShopifyOAuthInitiateResponse(
                auth_url=auth_url,
                state=state,
                redirect_uri=redirect_uri,  # Return this for debugging
            ),
        )

    except Exception as e:
        logger.error(f"Error initiating Shopify OAuth: {str(e)}", exc_info=True)
        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Failed to initiate Shopify connection. Please check your store domain and try again.",
            data={},
        )


@shopify_oauth_public_router.get("/callback", name="shopify_oauth_callback")
async def oauth_callback(
    request: Request,
    code: str,
    shop: str,
    state: str,
    hmac: str,
    timestamp: str,
    host: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Shopify OAuth callback
    Step 2: Exchange code for access token and save integration
    """
    try:
        # Check for OAuth error parameters (user cancelled or error occurred)
        error = request.query_params.get("error")
        error_description = request.query_params.get("error_description")

        if error:
            logger.warning(f"OAuth error from Shopify: {error} - {error_description}")

            # Handle specific error types
            if error == "access_denied":
                error_msg = "Authorization was cancelled. Please try again if you'd like to connect your store."
            elif error == "invalid_request":
                error_msg = (
                    "Invalid authorization request. Please try connecting again."
                )
            else:
                error_msg = f"Authorization failed: {error_description or error}"

            # Redirect to frontend with error
            frontend_url = f"https://app.lastmiles.co/channels/all?error={error}&message={error_msg}"
            return RedirectResponse(url=frontend_url)

        # Verify JWT state token (self-contained, no DB lookup needed)
        try:
            state_data = verify_oauth_state_token(state)
            client_id = state_data["client_id"]
            shop_domain_from_state = state_data["shop_domain"]

            logger.info(
                f"State token verified successfully for shop: {shop}, "
                f"client_id: {client_id}"
            )
        except ValueError as e:
            logger.error(f"State token verification failed: {str(e)}")
            return JSONResponse(
                status_code=http.HTTPStatus.BAD_REQUEST,
                content={
                    "error": f"Invalid or expired session: {str(e)}. Please try connecting your store again."
                },
            )

        # Verify HMAC
        query_params = {
            "code": code,
            "shop": shop,
            "state": state,
            "hmac": hmac,
            "timestamp": timestamp,
        }
        if host:
            query_params["host"] = host

        if not verify_oauth_hmac(query_params):
            logger.error(f"HMAC verification failed for shop: {shop}")
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={
                    "error": "Security verification failed. Please try connecting your store again."
                },
            )

        # Exchange code for access token
        token_url = get_shopify_token_url(shop)

        logger.info(
            f"Exchanging code for access token: shop={shop}, token_url={token_url}"
        )
        logger.info(f"Using client_id: {SHOPIFY_API_KEY[:10]}...{SHOPIFY_API_KEY[-4:]}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    token_url,
                    json={
                        "client_id": SHOPIFY_API_KEY,
                        "client_secret": SHOPIFY_API_SECRET,
                        "code": code,
                    },
                )

                logger.info(f"Token exchange response: status={response.status_code}")

                if response.status_code != 200:
                    response_text = response.text
                    logger.error(
                        f"Token exchange failed: {response.status_code} - {response_text}"
                    )

                    # Try to parse Shopify error
                    try:
                        error_data = response.json()
                        shopify_error = error_data.get("error", "")
                        shopify_error_description = error_data.get(
                            "error_description", ""
                        )
                        logger.error(
                            f"Shopify error: {shopify_error} - {shopify_error_description}"
                        )
                    except:
                        pass

                    error_msg = "Failed to connect to Shopify. "
                    if response.status_code == 401:
                        error_msg += "Invalid API credentials. Check SHOPIFY_OAUTH_API_KEY and SHOPIFY_OAUTH_API_SECRET."
                    elif response.status_code == 400:
                        error_msg += f"Bad request: {response_text[:200]}"
                    elif response.status_code == 403:
                        error_msg += "Access denied by Shopify."
                    else:
                        error_msg += f"Error: {response_text[:200]}"

                    return JSONResponse(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        content={"error": error_msg},
                    )
            except httpx.TimeoutException:
                logger.error("Timeout while connecting to Shopify")
                return JSONResponse(
                    status_code=http.HTTPStatus.REQUEST_TIMEOUT,
                    content={
                        "error": "Connection to Shopify timed out. Please try again."
                    },
                )
            except Exception as ex:
                logger.error(
                    f"HTTP error during token exchange: {str(ex)}", exc_info=True
                )
                return JSONResponse(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    content={"error": f"Network error: {str(ex)}"},
                )

            token_data = response.json()
            access_token = token_data.get("access_token")

        # Create Shopify connector to get shop info
        connector = ShopifyConnector(shop, access_token)
        shop_info_result = await connector.test_connection()

        if not shop_info_result["success"]:
            error_message = shop_info_result.get("message", "Unknown error")
            logger.error(f"Failed to get shop info for {shop}: {error_message}")
            return JSONResponse(
                status_code=http.HTTPStatus.BAD_REQUEST,
                content={
                    "error": f"Failed to retrieve store information. {error_message}"
                },
            )

        shop_info = shop_info_result["data"]  # Changed from ["shop_info"] to ["data"]

        # Encrypt credentials
        credentials = {
            "access_token": access_token,
            "shop_domain": shop,
            "scope": token_data.get("scope", ""),
        }
        encrypted_credentials = encrypt_credentials(credentials)

        # Get Shopify channel
        shopify_channel = (
            db.query(ChannelMaster).filter(ChannelMaster.slug == "shopify").first()
        )

        if not shopify_channel:
            logger.error("Shopify channel not found in ChannelMaster table")
            return JSONResponse(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                content={
                    "error": "Shopify integration is not configured in the system. Please contact support."
                },
            )

        # Check if this shop is already connected by ANY client (prevent same store multiple times)
        all_integrations = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.channel_id == shopify_channel.id,
                ClientChannelIntegration.is_active == True,
            )
            .all()
        )

        existing_for_current_client = None
        shop_connected_by_another_client = False

        for integ in all_integrations:
            try:
                creds = decrypt_credentials(integ.credentials)
                if creds.get("shop_domain") == shop:
                    if integ.client_id == client_id:
                        existing_for_current_client = integ
                    else:
                        shop_connected_by_another_client = True
                        logger.warning(
                            f"Shop {shop} already connected to client {integ.client_id}"
                        )
            except:
                continue

        # Prevent same shop being connected by multiple clients
        if shop_connected_by_another_client:
            logger.error(
                f"Shop {shop} already connected to another client, blocking connection for client {client_id}"
            )
            return JSONResponse(
                status_code=http.HTTPStatus.BAD_REQUEST,
                content={
                    "error": "This Shopify store is already connected to another account. Please disconnect it from the other account first, or contact support if you need assistance."
                },
            )

        if existing_for_current_client:
            # Update existing integration (reconnecting same store)
            logger.info(
                f"Updating existing integration {existing_for_current_client.id} for shop {shop}"
            )
            existing_for_current_client.credentials = encrypted_credentials
            existing_for_current_client.integration_name = (
                shop_info.get("shop_name") or shop
            )
            existing_for_current_client.connection_status = "connected"
            existing_for_current_client.last_connection_test_at = datetime.utcnow()
            existing_for_current_client.is_active = True
            existing_for_current_client.auto_sync_enabled = True
            existing_for_current_client.additional_metadata = {
                "store_url": shop_info.get("shop_domain", shop),
                "shop_email": shop_info.get("shop_email"),
                "shop_owner": shop_info.get("shop_owner"),
                "currency": shop_info.get("currency"),
                "timezone": shop_info.get("timezone"),
                "plan_name": shop_info.get("plan_name"),
            }
            db.commit()
            integration_id = existing_for_current_client.id
            is_reconnection = True
        else:
            # Create new integration
            logger.info(f"Creating new integration for shop {shop}")
            new_integration = ClientChannelIntegration(
                client_id=client_id,
                channel_id=shopify_channel.id,
                integration_name=shop_info.get("shop_name") or shop,
                credentials=encrypted_credentials,
                connection_status="connected",
                last_connection_test_at=datetime.utcnow(),
                is_active=True,
                auto_sync_enabled=True,
                sync_interval_minutes=30,
                webhook_enabled=True,
                additional_metadata={
                    "store_url": shop_info.get("shop_domain", shop),
                    "shop_email": shop_info.get("shop_email"),
                    "shop_owner": shop_info.get("shop_owner"),
                    "currency": shop_info.get("currency"),
                    "timezone": shop_info.get("timezone"),
                    "plan_name": shop_info.get("plan_name"),
                },
            )
            db.add(new_integration)
            db.commit()
            db.refresh(new_integration)
            integration_id = new_integration.id
            is_reconnection = False

        # Register webhooks
        base_url = str(request.base_url).rstrip("/")
        webhook_base_url = f"{base_url}/api/v1/channels/shopify/webhooks"
        webhook_results = await connector.register_all_webhooks(
            webhook_base_url, integration_id
        )

        logger.info(
            f"Webhooks registered for integration {integration_id}: {webhook_results}"
        )

        # Create audit log for successful connection
        try:
            IntegrationAuditLog.create_audit_log(
                db_session=db,
                integration_id=integration_id,
                event_type=(
                    "integration_connected"
                    if not is_reconnection
                    else "integration_reconnected"
                ),
                trigger="user_action",
                status="success",
                client_id=client_id,
                event_data={
                    "shop_domain": shop,
                    "shop_name": shop_info.get("shop_name"),
                    "shop_email": shop_info.get("shop_email"),
                    "shop_owner": shop_info.get("shop_owner"),
                    "store_url": shop_info.get("shop_domain"),
                    "currency": shop_info.get("currency"),
                    "timezone": shop_info.get("timezone"),
                    "plan_name": shop_info.get("plan_name"),
                    "is_reconnection": is_reconnection,
                    "webhooks_registered": len(
                        [r for r in webhook_results if r.get("success")]
                    ),
                },
            )
            logger.info(
                f"✅ Logged integration {'reconnection' if is_reconnection else 'connection'} to database: integration_id={integration_id}, shop={shop}"
            )
        except Exception as log_error:
            logger.error(
                f"Failed to log integration connection to database: {str(log_error)}",
                exc_info=True,
            )

        # JWT tokens are stateless - no database cleanup needed
        db.commit()

        # Redirect to frontend integration manage page with success toast
        frontend_url = f"https://app.lastmiles.co/channels/manage/{integration_id}?status=success"
        return RedirectResponse(url=frontend_url)

    except Exception as e:
        logger.error(f"OAuth callback error: {str(e)}", exc_info=True)

        # Provide user-friendly error message
        error_msg = "Connection failed. "
        if "database" in str(e).lower() or "db" in str(e).lower():
            error_msg += "Database error occurred. Please try again."
        elif "httpx" in str(e).lower() or "timeout" in str(e).lower():
            error_msg += (
                "Network error occurred. Please check your connection and try again."
            )
        elif "encrypt" in str(e).lower() or "decrypt" in str(e).lower():
            error_msg += "Security error occurred. Please contact support."
        else:
            error_msg += (
                "An unexpected error occurred. Please try again or contact support."
            )

        return JSONResponse(
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            content={"error": error_msg},
        )


@shopify_management_router.get(
    "/sync-history/{integration_id}", response_model=GenericResponseModel
)
async def get_sync_history(integration_id: int, limit: int = 50):
    """Get sync history for an integration"""
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        # Verify integration belongs to client
        integration = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.id == integration_id,
                ClientChannelIntegration.client_id == client_id,
            )
            .first()
        )

        if not integration:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.NOT_FOUND,
                message="Integration not found",
                data={},
            )

        # Get recent sync logs
        sync_logs = IntegrationSyncLog.get_recent_logs(db, integration_id, limit)

        # Convert to dict
        logs_data = [log.to_dict() for log in sync_logs]

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message=f"Retrieved {len(logs_data)} sync logs",
            data={"logs": logs_data, "integration_id": integration_id},
        )

    except Exception as e:
        logger.error(f"Error retrieving sync history: {str(e)}", exc_info=True)
        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Failed to retrieve sync history",
            data={},
        )


@shopify_management_router.get("/integrations/my", response_model=GenericResponseModel)
async def get_my_shopify_integrations():
    """
    Get all channel integrations for current client (channel-independent)
    Returns integrations from all channels with their respective channel details
    """
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        # Fetch all integrations with channel details via join
        integrations = (
            db.query(ClientChannelIntegration, ChannelMaster)
            .join(
                ChannelMaster, ClientChannelIntegration.channel_id == ChannelMaster.id
            )
            .filter(ClientChannelIntegration.client_id == client_id)
            .all()
        )

        if not integrations:
            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="No integrations found",
                data=[],
            )

        result = []
        for integration, channel in integrations:
            # Extract store domain from encrypted credentials
            shop_domain = ""
            try:
                creds = decrypt_credentials(integration.credentials)
                shop_domain = creds.get("shop_domain", creds.get("store_url", ""))
            except Exception:
                pass

            # Calculate webhook health status based on last received time
            webhook_status = "inactive"
            if integration.webhook_enabled and integration.is_active:
                if integration.last_webhook_received_at:
                    time_since_webhook = (
                        datetime.utcnow() - integration.last_webhook_received_at
                    )
                    webhook_status = (
                        "active"
                        if time_since_webhook < timedelta(hours=24)
                        else "inactive"
                    )
                else:
                    webhook_status = "pending"

            # Build channel information object
            channel_info = ChannelInfo(
                id=channel.id,
                name=channel.name,
                slug=channel.slug,
                channel_type=channel.channel_type,
                logo_url=channel.logo_url,
            )

            # Build integration response object
            result.append(
                ShopifyIntegrationInfo(
                    integration_id=integration.id,
                    shop_domain=shop_domain,
                    shop_name=integration.integration_name,
                    shop_email=(
                        integration.additional_metadata.get("shop_email")
                        if integration.additional_metadata
                        else None
                    ),
                    shop_owner=(
                        integration.additional_metadata.get("shop_owner")
                        if integration.additional_metadata
                        else None
                    ),
                    connection_status=integration.connection_status,
                    is_active=integration.is_active,
                    auto_sync_enabled=integration.auto_sync_enabled,
                    sync_interval_minutes=integration.sync_interval_minutes,
                    last_order_sync_at=integration.last_order_sync_at,
                    total_orders_synced=integration.total_orders_synced,
                    created_at=integration.created_at,
                    webhook_status=webhook_status,
                    last_synced_at=integration.last_successful_sync_at,
                    last_webhook_received_at=integration.last_webhook_received_at,
                    channel=channel_info,
                )
            )

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="Integrations fetched successfully",
            data=result,
        )

    except Exception as e:
        logger.error(f"Error fetching integrations: {str(e)}")
        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message=f"Failed to fetch integrations: {str(e)}",
            data=[],
        )


@shopify_management_router.post(
    "/test-connection/{integration_id}", response_model=GenericResponseModel
)
async def test_shopify_connection(integration_id: int):
    """Test connection to Shopify store"""
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        integration = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.id == integration_id,
                ClientChannelIntegration.client_id == client_id,
            )
            .first()
        )

        if not integration:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.NOT_FOUND,
                message="Integration not found. Please check the integration ID.",
                data={},
            )

        # Check if credentials exist
        if not integration.credentials:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.BAD_REQUEST,
                message="Integration credentials not found. Please reconnect your store.",
                data={},
            )

        # Decrypt credentials
        try:
            credentials = decrypt_credentials(integration.credentials)
            shop_domain = credentials.get("shop_domain")
            access_token = credentials.get("access_token")

            if not shop_domain or not access_token:
                raise ValueError("Missing shop domain or access token")

        except Exception as cred_error:
            logger.error(f"Credentials decryption failed: {str(cred_error)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.BAD_REQUEST,
                message="Failed to decrypt credentials. Please reconnect your store.",
                data={},
            )

        # Test connection
        connector = ShopifyConnector(shop_domain, access_token)

        result = await connector.test_connection()

        # Update connection status
        if result["success"]:
            integration.connection_status = "connected"
            integration.last_connection_test_at = datetime.utcnow()
            integration.connection_error_message = None
        else:
            integration.connection_status = "failed"
            integration.connection_error_message = result["message"]

        integration.updated_at = datetime.utcnow()
        db.commit()

        # Create audit log for test connection
        try:
            IntegrationAuditLog.create_audit_log(
                db_session=db,
                integration_id=integration_id,
                event_type="test_connection",
                trigger="user_action",
                status="success" if result["success"] else "failed",
                client_id=client_id,
                event_data={
                    "shop_domain": shop_domain,
                    "integration_name": integration.integration_name,
                    "test_result": result.get("shop_info", {}),
                },
                error_message=result.get("message") if not result["success"] else None,
            )
        except Exception as log_error:
            logger.error(
                f"Failed to log test connection to database: {str(log_error)}",
                exc_info=True,
            )

        return GenericResponseModel(
            status=result["success"],
            status_code=(
                http.HTTPStatus.OK if result["success"] else http.HTTPStatus.BAD_REQUEST
            ),
            message=result["message"],
            data=result,
        )

    except Exception as e:
        logger.error(f"Error testing Shopify connection: {str(e)}", exc_info=True)

        # Provide user-friendly error messages
        error_message = "Connection test failed. "
        if "401" in str(e) or "Unauthorized" in str(e):
            error_message += "Your Shopify access token may have expired. Please reconnect your store."
        elif "403" in str(e) or "Forbidden" in str(e):
            error_message += (
                "Access forbidden. Please check your Shopify app permissions."
            )
        elif "404" in str(e):
            error_message += "Store not found. Please verify your store domain."
        elif "429" in str(e) or "rate" in str(e).lower():
            error_message += "Rate limit exceeded. Please try again in a few moments."
        elif "timeout" in str(e).lower():
            error_message += "Request timed out. Please try again."
        else:
            error_message += (
                "Please try again or contact support if the issue persists."
            )

        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message=error_message,
            data={},
        )


@shopify_management_router.post(
    "/pause/{integration_id}", response_model=GenericResponseModel
)
async def pause_integration(integration_id: int):
    """Pause Shopify integration (stops syncing but keeps connection)"""
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        integration = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.id == integration_id,
                ClientChannelIntegration.client_id == client_id,
            )
            .first()
        )

        if not integration:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.NOT_FOUND,
                message="Integration not found",
                data={},
            )

        # Get shop domain for logging
        shop_domain = ""
        try:
            creds = decrypt_credentials(integration.credentials)
            shop_domain = creds.get("shop_domain", "")
        except:
            pass

        # Pause syncing
        integration.auto_sync_enabled = False
        integration.connection_status = "paused"
        integration.updated_at = datetime.utcnow()
        db.commit()

        logger.info(f"Integration {integration_id} paused for client {client_id}")

        # Create audit log
        try:
            IntegrationAuditLog.create_audit_log(
                db_session=db,
                integration_id=integration_id,
                event_type="integration_paused",
                trigger="user_action",
                status="success",
                client_id=client_id,
                event_data={
                    "shop_domain": shop_domain,
                    "integration_name": integration.integration_name,
                },
            )
        except Exception as log_error:
            logger.error(
                f"Failed to log integration pause to database: {str(log_error)}",
                exc_info=True,
            )

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="Integration paused successfully. Syncing stopped.",
            data={"integration_id": integration_id, "status": "paused"},
        )

    except Exception as e:
        logger.error(f"Error pausing integration: {str(e)}", exc_info=True)
        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Failed to pause integration. Please try again or contact support if the issue persists.",
            data={},
        )


@shopify_management_router.post(
    "/resume/{integration_id}", response_model=GenericResponseModel
)
async def resume_integration(integration_id: int):
    """Resume paused Shopify integration"""
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        integration = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.id == integration_id,
                ClientChannelIntegration.client_id == client_id,
            )
            .first()
        )

        if not integration:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.NOT_FOUND,
                message="Integration not found",
                data={},
            )

        # Get shop domain for logging
        shop_domain = ""
        try:
            creds = decrypt_credentials(integration.credentials)
            shop_domain = creds.get("shop_domain", "")
        except:
            pass

        # Resume syncing
        integration.auto_sync_enabled = True
        integration.connection_status = "connected"
        integration.updated_at = datetime.utcnow()
        db.commit()

        logger.info(f"Integration {integration_id} resumed for client {client_id}")

        # Create audit log
        try:
            IntegrationAuditLog.create_audit_log(
                db_session=db,
                integration_id=integration_id,
                event_type="integration_resumed",
                trigger="user_action",
                status="success",
                client_id=client_id,
                event_data={
                    "shop_domain": shop_domain,
                    "integration_name": integration.integration_name,
                },
            )
        except Exception as log_error:
            logger.error(
                f"Failed to log integration resume to database: {str(log_error)}",
                exc_info=True,
            )

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="Integration resumed successfully. Syncing restarted.",
            data={"integration_id": integration_id, "status": "connected"},
        )

    except Exception as e:
        logger.error(f"Error resuming integration: {str(e)}", exc_info=True)
        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Failed to resume integration. Please try again or contact support if the issue persists.",
            data={},
        )


@shopify_management_router.delete(
    "/delete/{integration_id}", response_model=GenericResponseModel
)
async def delete_integration(integration_id: int):
    """Delete Shopify integration completely (uninstall from Shopify + delete from DB)"""
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        integration = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.id == integration_id,
                ClientChannelIntegration.client_id == client_id,
            )
            .first()
        )

        if not integration:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.NOT_FOUND,
                message="Integration not found",
                data={},
            )

        # Decrypt credentials to get shop details
        credentials = decrypt_credentials(integration.credentials)
        shop_domain = credentials.get("shop_domain")
        access_token = credentials.get("access_token")

        # Try to uninstall app from Shopify (delete API access)
        try:
            connector = ShopifyConnector(shop_domain, access_token)
            # Note: Shopify doesn't have a direct API to uninstall apps
            # The app will be marked as uninstalled when user uninstalls from their admin
            # We just clean up our side
            logger.info(f"Preparing to delete integration for {shop_domain}")
        except Exception as uninstall_error:
            logger.warning(
                f"Could not notify Shopify of uninstall: {str(uninstall_error)}"
            )

        # Mark as deleted (soft delete)
        integration.is_active = False
        integration.connection_status = "deleted"
        integration.auto_sync_enabled = False
        integration.updated_at = datetime.utcnow()
        db.commit()

        logger.info(f"Integration {integration_id} deleted for client {client_id}")

        # Create audit log
        try:
            IntegrationAuditLog.create_audit_log(
                db_session=db,
                integration_id=integration_id,
                event_type="integration_deleted",
                trigger="user_action",
                status="success",
                client_id=client_id,
                event_data={
                    "shop_domain": shop_domain,
                    "integration_name": integration.integration_name,
                },
            )
        except Exception as log_error:
            logger.error(
                f"Failed to log integration deletion to database: {str(log_error)}",
                exc_info=True,
            )

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="Integration deleted successfully. Please uninstall the app from your Shopify admin.",
            data={"integration_id": integration_id, "shop_domain": shop_domain},
        )

    except Exception as e:
        logger.error(f"Error deleting integration: {str(e)}", exc_info=True)
        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Failed to delete integration. Please try again or contact support if the issue persists.",
            data={},
        )


@shopify_management_router.post(
    "/cleanup-webhooks/{integration_id}", response_model=GenericResponseModel
)
async def cleanup_webhooks(integration_id: int):
    """
    Cleanup and re-register webhooks for an integration
    Useful when webhooks are misconfigured or orphaned
    """
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        integration = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.id == integration_id,
                ClientChannelIntegration.client_id == client_id,
            )
            .first()
        )

        if not integration:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.NOT_FOUND,
                message="Integration not found.",
                data={},
            )

        # Decrypt credentials
        try:
            credentials = decrypt_credentials(integration.credentials)
            shop_domain = credentials.get("shop_domain")
            access_token = credentials.get("access_token")

            if not shop_domain or not access_token:
                raise ValueError("Missing credentials")
        except Exception as e:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.BAD_REQUEST,
                message="Failed to decrypt credentials. Please reconnect your store.",
                data={},
            )

        # Get connector
        connector = ShopifyConnector(shop_domain, access_token)

        # Get all existing webhooks
        existing_webhooks = await connector.get_webhooks()
        logger.info(
            f"Found {len(existing_webhooks)} existing webhooks for {shop_domain}"
        )

        # Delete all existing webhooks
        deleted_count = 0
        for webhook in existing_webhooks:
            webhook_id = webhook.get("id")
            if await connector.delete_webhook(webhook_id):
                deleted_count += 1

        # Re-register all webhooks
        from fastapi import Request

        base_url = "https://api.lastmiles.co"  # Use production URL
        webhook_base_url = f"{base_url}/api/v1/channels/shopify/webhooks"
        webhook_results = await connector.register_all_webhooks(
            webhook_base_url, integration_id
        )

        success_count = sum(1 for r in webhook_results if r.get("success"))

        logger.info(
            f"Webhook cleanup for integration {integration_id}: "
            f"Deleted {deleted_count}, Re-registered {success_count}"
        )

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message=f"Cleaned up {deleted_count} old webhooks and registered {success_count} new webhooks",
            data={
                "deleted_count": deleted_count,
                "registered_count": success_count,
                "results": webhook_results,
            },
        )

    except Exception as e:
        logger.error(f"Error cleaning up webhooks: {str(e)}", exc_info=True)
        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Failed to cleanup webhooks. Please try again or contact support.",
            data={},
        )


@shopify_management_router.post(
    "/sync-orders/{integration_id}", response_model=GenericResponseModel
)
async def manual_sync_orders(integration_id: int):
    """Manually sync unfulfilled orders from last 14 days"""
    try:
        user_data = context_user_data.get()
        client_id = user_data.client_id

        db = get_db_session()

        integration = (
            db.query(ClientChannelIntegration)
            .filter(
                ClientChannelIntegration.id == integration_id,
                ClientChannelIntegration.client_id == client_id,
            )
            .first()
        )

        if not integration:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.NOT_FOUND,
                message="Integration not found",
                data={},
            )

        # Check if integration is active
        if not integration.is_active:
            logger.warning(
                f"Manual sync attempted for inactive integration {integration_id} by client {client_id}"
            )
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.BAD_REQUEST,
                message="Integration is inactive. Please reactivate your store connection to sync orders.",
                data={"integration_id": integration_id, "is_active": False},
            )

        # Check if sync is already in progress (simple concurrent prevention)
        if integration.sync_in_progress:
            # Check if sync is stuck (older than 1 hour)
            if integration.sync_started_at:
                from datetime import timedelta

                time_since_start = datetime.utcnow() - integration.sync_started_at
                if time_since_start > timedelta(hours=1):
                    # Sync is stuck, reset the flag
                    logger.warning(
                        f"Sync stuck for integration {integration_id}, resetting flag"
                    )
                    integration.sync_in_progress = False
                    integration.sync_started_at = None
                    db.commit()
                else:
                    # Sync is genuinely in progress
                    return GenericResponseModel(
                        status=False,
                        status_code=http.HTTPStatus.CONFLICT,
                        message="A sync is already in progress. Please wait for it to complete.",
                        data={
                            "integration_id": integration_id,
                            "sync_started_at": integration.sync_started_at.isoformat(),
                        },
                    )

        # Set sync in progress flag
        integration.sync_in_progress = True
        integration.sync_started_at = datetime.utcnow()
        db.commit()

        # Decrypt credentials
        credentials = decrypt_credentials(integration.credentials)
        shop_domain = credentials.get("shop_domain")
        access_token = credentials.get("access_token")

        # Create connector and sync orders
        connector = ShopifyConnector(shop_domain, access_token)

        # Calculate date 14 days ago
        from datetime import timedelta

        fourteen_days_ago = datetime.utcnow() - timedelta(days=14)
        since_date = fourteen_days_ago.isoformat()

        # Fetch unfulfilled orders from last 14 days
        logger.info(
            f"Starting manual sync for integration {integration_id}, since {since_date}"
        )

        orders = await connector.fetch_orders(
            since_date=fourteen_days_ago, fulfillment_status="unfulfilled", limit=250
        )

        print(orders)

        orders_count = len(orders)

        # Import orders using sync service
        from .shopify_order_sync import ShopifyOrderSync
        import uuid

        sync_service = ShopifyOrderSync(integration)
        started_at = datetime.utcnow()

        imported_count = 0
        failed_count = 0
        for order in orders:
            try:
                result = await sync_service.import_single_order(order)
                if result.status:
                    imported_count += 1
                else:
                    failed_count += 1
            except Exception as import_error:
                logger.error(
                    f"Error importing order {order.get('order_number')}: {str(import_error)}"
                )
                failed_count += 1

        completed_at = datetime.utcnow()
        duration = int((completed_at - started_at).total_seconds())

        # Update sync timestamp
        # Note: total_orders_synced is already incremented in import_single_order method
        # so we don't need to increment it again here to avoid double counting
        integration.last_order_sync_at = datetime.utcnow()
        integration.last_successful_sync_at = datetime.utcnow()
        integration.updated_at = datetime.utcnow()
        db.commit()

        logger.info(
            f"Manual sync completed for integration {integration_id}: {orders_count} orders fetched, {imported_count} imported, {failed_count} failed"
        )

        # Create sync log in database
        try:
            job_id = (
                f"manual_sync_{uuid.uuid4().hex[:16]}_{int(started_at.timestamp())}"
            )
            sync_log = IntegrationSyncLog(
                integration_id=integration_id,
                job_id=job_id,
                sync_type="order_sync",
                sync_trigger="manual",
                status=(
                    "success"
                    if failed_count == 0
                    else ("partial_success" if imported_count > 0 else "failed")
                ),
                records_processed=orders_count,
                records_successful=imported_count,
                records_failed=failed_count,
                sync_data={
                    "shop_domain": shop_domain,
                    "since_date": since_date,
                    "fulfillment_status": "unfulfilled",
                    "sync_time": completed_at.isoformat(),
                },
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                error_message=(
                    f"{failed_count} orders failed to import"
                    if failed_count > 0
                    else None
                ),
            )
            db.add(sync_log)
            db.commit()
            logger.info(
                f"✅ Logged manual sync to database: job_id={job_id}, integration_id={integration_id}"
            )
        except Exception as log_error:
            logger.error(
                f"Failed to log manual sync to database: {str(log_error)}",
                exc_info=True,
            )

        # Clear sync in progress flag
        finally:
            try:
                integration.sync_in_progress = False
                integration.sync_started_at = None
                db.commit()
            except:
                pass

        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message=f"Successfully synced {imported_count} orders ({failed_count} failed) from last 14 days",
            data={
                "integration_id": integration_id,
                "orders_fetched": orders_count,
                "orders_imported": imported_count,
                "orders_failed": failed_count,
                "since_date": since_date,
                "sync_time": completed_at.isoformat(),
                "duration_seconds": duration,
            },
        )

    except Exception as e:
        # Clear sync in progress flag on error
        try:
            db = get_db_session()
            integration = (
                db.query(ClientChannelIntegration)
                .filter(
                    ClientChannelIntegration.id == integration_id,
                    ClientChannelIntegration.client_id == client_id,
                )
                .first()
            )
            if integration:
                integration.sync_in_progress = False
                integration.sync_started_at = None
                db.commit()
        except:
            pass

        logger.error(f"Error syncing orders: {str(e)}", exc_info=True)

        # Provide user-friendly error messages
        error_message = "Failed to sync orders. "
        if "401" in str(e) or "Unauthorized" in str(e):
            error_message += "Your Shopify access token may have expired. Please reconnect your store."
        elif "403" in str(e) or "Forbidden" in str(e):
            error_message += (
                "Access forbidden. Please check your Shopify app permissions."
            )
        elif "404" in str(e):
            error_message += "Store not found. Please verify your store domain."
        elif "429" in str(e) or "rate" in str(e).lower():
            error_message += "Rate limit exceeded. Please try again in a few moments."
        elif "timeout" in str(e).lower():
            error_message += "Request timed out. Please try again."
        else:
            error_message += (
                "Please try again or contact support if the issue persists."
            )

        return GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message=error_message,
            data={},
        )
