"""
Shopify Webhook Handlers
Handles incoming webhooks from Shopify (orders, GDPR, app uninstall)
"""

import http
from fastapi import APIRouter, Request, Header, Query, Depends
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import datetime
from sqlalchemy.orm import Session

from logger import logger
from database.db import get_db
from models import ClientChannelIntegration, IntegrationSyncLog, IntegrationAuditLog
from utils.credential_encryption import decrypt_credentials

from .shopify_config import verify_webhook_hmac
from .shopify_order_sync import ShopifyOrderSync


webhook_router = APIRouter(prefix="/webhooks", tags=["Shopify Webhooks"])


async def verify_shopify_webhook(request: Request, x_shopify_hmac_sha256: str) -> bool:
    """Verify Shopify webhook HMAC signature"""
    raw_body = await request.body()
    return verify_webhook_hmac(raw_body, x_shopify_hmac_sha256)


@webhook_router.post("/orders_create")
async def webhook_order_created(
    request: Request,
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    db: Session = Depends(get_db),
    integration_id: Optional[int] = None,
):
    """
    Handle orders/create webhook from Shopify
    Triggered when a new order is created
    
    Args:
        integration_id: Integration ID from query parameter (for optimized lookup)
    """
    try:
        # Verify webhook
        if not await verify_shopify_webhook(request, x_shopify_hmac_sha256):
            logger.warning(f"Invalid webhook HMAC from {x_shopify_shop_domain}")
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={"error": "Invalid HMAC"},
            )

        order_data = await request.json()

        logger.info(
            f"Order created webhook from {x_shopify_shop_domain}: Order #{order_data.get('order_number')}"
        )

        # Get integration_id from query params if not provided in function signature
        if integration_id is None:
            integration_id = request.query_params.get("integration_id")
            if integration_id:
                try:
                    integration_id = int(integration_id)
                except ValueError:
                    integration_id = None

        integration = None

        # Optimized lookup: Use integration_id if provided
        if integration_id:
            integration = (
                db.query(ClientChannelIntegration)
                .filter(
                    ClientChannelIntegration.id == integration_id,
                    ClientChannelIntegration.is_active == True,
                )
                .first()
            )
            
            # Verify shop_domain matches for security
            if integration:
                try:
                    creds = decrypt_credentials(integration.credentials)
                    if creds.get("shop_domain") != x_shopify_shop_domain:
                        logger.warning(
                            f"Shop domain mismatch for integration {integration_id}: "
                            f"expected {creds.get('shop_domain')}, got {x_shopify_shop_domain}"
                        )
                        integration = None
                except Exception as e:
                    logger.error(f"Error decrypting credentials for integration {integration_id}: {str(e)}")
                    integration = None

        # Fallback: Query by shop_domain if integration_id not provided or not found
        if not integration:
            from models import ChannelMaster
            shopify_channel = (
                db.query(ChannelMaster).filter(ChannelMaster.slug == "shopify").first()
            )

            if not shopify_channel:
                return JSONResponse(status_code=200, content={"ok": True})

            integrations = (
                db.query(ClientChannelIntegration)
                .filter(
                    ClientChannelIntegration.channel_id == shopify_channel.id,
                    ClientChannelIntegration.is_active == True,
                )
                .all()
            )

            for integ in integrations:
                try:
                    creds = decrypt_credentials(integ.credentials)
                    if creds.get("shop_domain") == x_shopify_shop_domain:
                        integration = integ
                        break
                except:
                    continue

        if not integration:
            logger.warning(f"No integration found for shop {x_shopify_shop_domain}")
            return JSONResponse(status_code=200, content={"ok": True})

        # Check if integration is active and auto-sync is enabled
        if not integration.is_active or not integration.auto_sync_enabled:
            logger.warning(f"Integration {integration.id} is inactive or auto-sync disabled, skipping webhook order sync")
            return JSONResponse(status_code=200, content={"ok": True})

        # Update last webhook received timestamp
        integration.last_webhook_received_at = datetime.utcnow()
        db.commit()

        # Process order
        started_at = datetime.utcnow()
        sync_service = ShopifyOrderSync(integration)
        result = await sync_service.import_single_order(order_data)
        completed_at = datetime.utcnow()
        duration = int((completed_at - started_at).total_seconds())
        
        # Log sync event
        try:
            import uuid
            job_id = f"webhook_order_create_{uuid.uuid4().hex[:16]}_{int(started_at.timestamp())}"
            sync_log = IntegrationSyncLog(
                integration_id=integration.id,
                job_id=job_id,
                sync_type="order_sync",
                sync_trigger="webhook",
                status="success" if result.status else "failed",
                records_processed=1,
                records_successful=1 if result.status else 0,
                records_failed=0 if result.status else 1,
                sync_data={
                    "shop_domain": x_shopify_shop_domain,
                    "order_number": order_data.get("order_number"),
                    "order_id": order_data.get("id"),
                    "webhook_type": "orders/create",
                },
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                error_message=result.message if not result.status else None,
            )
            db.add(sync_log)
            db.commit()
        except Exception as log_error:
            logger.error(f"Failed to log webhook sync to database: {str(log_error)}", exc_info=True)

        return JSONResponse(status_code=200, content={"ok": True})

    except Exception as e:
        logger.error(f"Error processing orders/create webhook: {str(e)}")
        return JSONResponse(
            status_code=200, content={"ok": True}
        )  # Always return 200 to Shopify


@webhook_router.post("/orders_updated")
async def webhook_order_updated(
    request: Request,
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    db: Session = Depends(get_db),
    integration_id: Optional[int] = None,
):
    """
    Handle orders/updated webhook from Shopify
    Triggered when an order is updated
    
    Args:
        integration_id: Integration ID from query parameter (for optimized lookup)
    """
    try:
        if not await verify_shopify_webhook(request, x_shopify_hmac_sha256):
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={"error": "Invalid HMAC"},
            )

        order_data = await request.json()
        logger.info(
            f"Order updated webhook from {x_shopify_shop_domain}: Order #{order_data.get('order_number')}"
        )

        # Get integration_id from query params if not provided in function signature
        if integration_id is None:
            integration_id = request.query_params.get("integration_id")
            if integration_id:
                try:
                    integration_id = int(integration_id)
                except ValueError:
                    integration_id = None

        integration = None

        # Optimized lookup: Use integration_id if provided
        if integration_id:
            integration = (
                db.query(ClientChannelIntegration)
                .filter(
                    ClientChannelIntegration.id == integration_id,
                    ClientChannelIntegration.is_active == True,
                )
                .first()
            )
            
            # Verify shop_domain matches for security
            if integration:
                try:
                    creds = decrypt_credentials(integration.credentials)
                    if creds.get("shop_domain") != x_shopify_shop_domain:
                        logger.warning(
                            f"Shop domain mismatch for integration {integration_id}: "
                            f"expected {creds.get('shop_domain')}, got {x_shopify_shop_domain}"
                        )
                        integration = None
                except Exception as e:
                    logger.error(f"Error decrypting credentials for integration {integration_id}: {str(e)}")
                    integration = None

        # Fallback: Query by shop_domain if integration_id not provided or not found
        if not integration:
            from models import ChannelMaster
            shopify_channel = db.query(ChannelMaster).filter(ChannelMaster.slug == "shopify").first()
            
            if not shopify_channel:
                return JSONResponse(status_code=200, content={"ok": True})
            
            integrations = db.query(ClientChannelIntegration).filter(
                ClientChannelIntegration.channel_id == shopify_channel.id,
                ClientChannelIntegration.is_active == True,
            ).all()
            
            for integ in integrations:
                try:
                    creds = decrypt_credentials(integ.credentials)
                    if creds.get("shop_domain") == x_shopify_shop_domain:
                        integration = integ
                        break
                except:
                    continue
        
        if not integration:
            logger.warning(f"No integration found for shop {x_shopify_shop_domain}")
            return JSONResponse(status_code=200, content={"ok": True})
        
        # Check if integration is active and auto-sync is enabled
        if not integration.is_active or not integration.auto_sync_enabled:
            logger.warning(f"Integration {integration.id} is inactive or auto-sync disabled, skipping webhook order sync")
            return JSONResponse(status_code=200, content={"ok": True})
        
        # Update last webhook received timestamp
        integration.last_webhook_received_at = datetime.utcnow()
        db.commit()
        
        # Process order update
        started_at = datetime.utcnow()
        sync_service = ShopifyOrderSync(integration)
        result = await sync_service.import_single_order(order_data)
        completed_at = datetime.utcnow()
        duration = int((completed_at - started_at).total_seconds())
        
        # Log sync event
        try:
            import uuid
            job_id = f"webhook_order_update_{uuid.uuid4().hex[:16]}_{int(started_at.timestamp())}"
            sync_log = IntegrationSyncLog(
                integration_id=integration.id,
                job_id=job_id,
                sync_type="order_sync",
                sync_trigger="webhook",
                status="success" if result.status else "failed",
                records_processed=1,
                records_successful=1 if result.status else 0,
                records_failed=0 if result.status else 1,
                sync_data={
                    "shop_domain": x_shopify_shop_domain,
                    "order_number": order_data.get("order_number"),
                    "order_id": order_data.get("id"),
                    "webhook_type": "orders/updated",
                },
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                error_message=result.message if not result.status else None,
            )
            db.add(sync_log)
            db.commit()
        except Exception as log_error:
            logger.error(f"Failed to log webhook sync to database: {str(log_error)}", exc_info=True)
        
        return JSONResponse(status_code=200, content={"ok": True})

    except Exception as e:
        logger.error(f"Error processing orders/updated webhook: {str(e)}")
        return JSONResponse(status_code=200, content={"ok": True})


@webhook_router.post("/orders_cancelled")
async def webhook_order_cancelled(
    request: Request,
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    db: Session = Depends(get_db),
):
    """
    Handle orders/cancelled webhook from Shopify
    Triggered when an order is cancelled
    """
    try:
        if not await verify_shopify_webhook(request, x_shopify_hmac_sha256):
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={"error": "Invalid HMAC"},
            )

        order_data = await request.json()
        logger.info(
            f"Order cancelled webhook from {x_shopify_shop_domain}: Order #{order_data.get('order_number')}"
        )

        # Process cancellation

        return JSONResponse(status_code=200, content={"ok": True})

    except Exception as e:
        logger.error(f"Error processing orders/cancelled webhook: {str(e)}")
        return JSONResponse(status_code=200, content={"ok": True})


@webhook_router.post("/app_uninstalled")
async def webhook_app_uninstalled(
    request: Request,
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    db: Session = Depends(get_db),
    integration_id: Optional[int] = None,
):
    """
    Handle app/uninstalled webhook from Shopify
    REQUIRED: Must handle app uninstallation

    This webhook is triggered when a merchant uninstalls the app from their Shopify admin.
    We must properly deactivate the integration and stop all syncing.
    
    Args:
        integration_id: Integration ID from query parameter (for optimized lookup)
    """
    try:
        if not await verify_shopify_webhook(request, x_shopify_hmac_sha256):
            logger.warning(
                f"Invalid HMAC for app/uninstalled from {x_shopify_shop_domain}"
            )
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={"error": "Invalid HMAC"},
            )

        webhook_data = await request.json()
        shop_id = webhook_data.get("id")
        shop_domain = webhook_data.get("domain") or x_shopify_shop_domain

        logger.info(f"App uninstalled from shop: {shop_domain} (ID: {shop_id})")

        # Get integration_id from query params if not provided in function signature
        if integration_id is None:
            integration_id = request.query_params.get("integration_id")
            if integration_id:
                try:
                    integration_id = int(integration_id)
                except ValueError:
                    integration_id = None

        integration = None

        # Optimized lookup: Use integration_id if provided
        if integration_id:
            integration = (
                db.query(ClientChannelIntegration)
                .filter(ClientChannelIntegration.id == integration_id)
                .first()
            )
            
            # Verify shop_domain matches for security
            if integration:
                try:
                    creds = decrypt_credentials(integration.credentials)
                    if creds.get("shop_domain") != shop_domain:
                        logger.warning(
                            f"Shop domain mismatch for integration {integration_id}: "
                            f"expected {creds.get('shop_domain')}, got {shop_domain}"
                        )
                        integration = None
                except Exception as e:
                    logger.error(f"Error decrypting credentials for integration {integration_id}: {str(e)}")
                    integration = None

        # Fallback: Query by shop_domain if integration_id not provided or not found
        if not integration:
            from models import ChannelMaster

            shopify_channel = (
                db.query(ChannelMaster).filter(ChannelMaster.slug == "shopify").first()
            )

            if not shopify_channel:
                logger.error("Shopify channel not found in database")
                return JSONResponse(status_code=200, content={"ok": True})

            integrations = (
                db.query(ClientChannelIntegration)
                .filter(
                    ClientChannelIntegration.channel_id == shopify_channel.id,
                    ClientChannelIntegration.is_active == True,
                )
                .all()
            )

            for integ in integrations:
                try:
                    creds = decrypt_credentials(integ.credentials)
                    if creds.get("shop_domain") == shop_domain:
                        integration = integ
                        break
                except:
                    continue

        if not integration:
            logger.warning(f"No integration found for shop {shop_domain}")
            return JSONResponse(status_code=200, content={"ok": True})

        # Mark as uninstalled
        integration.is_active = False
        integration.connection_status = "uninstalled"
        integration.auto_sync_enabled = False
        integration.webhook_enabled = False
        integration.updated_at = datetime.utcnow()

        # Keep metadata for reference but clear sensitive data
        if integration.additional_metadata:
            integration.additional_metadata["uninstalled_at"] = (
                datetime.utcnow().isoformat()
            )

        db.commit()
        logger.info(
            f"Successfully deactivated integration {integration.id} for shop {shop_domain}"
        )
        
        # Create audit log for store uninstall
        try:
            IntegrationAuditLog.create_audit_log(
                db_session=db,
                integration_id=integration.id,
                event_type="store_uninstalled",
                trigger="store_action",
                status="success",
                event_category="lifecycle",
                event_data={
                    "shop_domain": shop_domain,
                    "shop_id": shop_id,
                    "uninstalled_at": datetime.utcnow().isoformat(),
                },
            )
            logger.info(f"✅ Logged store uninstall to database: integration_id={integration.id}, shop={shop_domain}")
        except Exception as log_error:
            logger.error(f"Failed to log store uninstall to database: {str(log_error)}", exc_info=True)

        return JSONResponse(status_code=200, content={"ok": True})

    except Exception as e:
        logger.error(
            f"Error processing app/uninstalled webhook: {str(e)}", exc_info=True
        )
        # Always return 200 to prevent Shopify retries
        return JSONResponse(status_code=200, content={"ok": True})


# ============================================
# GDPR COMPLIANCE ENDPOINTS (MANDATORY)
# ============================================


@webhook_router.post("/customers_redact")
async def gdpr_customers_redact(
    request: Request,
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    db: Session = Depends(get_db),
):
    """
    GDPR: Redact customer data
    REQUIRED: Must respond within 48 hours

    Shopify sends this when a customer requests data deletion
    """
    try:
        if not await verify_shopify_webhook(request, x_shopify_hmac_sha256):
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={"error": "Invalid HMAC"},
            )

        payload = await request.json()

        customer_id = payload.get("customer", {}).get("id")
        customer_email = payload.get("customer", {}).get("email")

        logger.info(
            f"GDPR: Customer data redaction request from {x_shopify_shop_domain} for customer {customer_id}"
        )

        # TODO: Implement customer data redaction
        # 1. Find all orders with this customer
        # 2. Anonymize/redact customer PII
        # 3. Keep order data for accounting but remove personal info

        # For now, log the request
        logger.info(
            f"Customer redaction: {customer_email} from {x_shopify_shop_domain}"
        )

        return JSONResponse(status_code=200, content={"ok": True})

    except Exception as e:
        logger.error(f"Error processing customers/redact webhook: {str(e)}")
        return JSONResponse(status_code=200, content={"ok": True})


@webhook_router.post("/shop_redact")
async def gdpr_shop_redact(
    request: Request,
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    db: Session = Depends(get_db),
):
    """
    GDPR: Redact shop data
    REQUIRED: Must respond within 48 hours

    Shopify sends this 48 hours after app uninstall
    """
    try:
        if not await verify_shopify_webhook(request, x_shopify_hmac_sha256):
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={"error": "Invalid HMAC"},
            )

        payload = await request.json()
        shop_id = payload.get("shop_id")
        shop_domain = payload.get("shop_domain")

        logger.info(f"GDPR: Shop data redaction request from {x_shopify_shop_domain}")

        # TODO: Implement shop data redaction
        # 1. Delete/anonymize all data related to this shop
        # 2. Remove integration records
        # 3. Keep minimal data for accounting compliance

        from models import ChannelMaster

        shopify_channel = (
            db.query(ChannelMaster).filter(ChannelMaster.slug == "shopify").first()
        )

        if shopify_channel:
            integrations = (
                db.query(ClientChannelIntegration)
                .filter(
                    ClientChannelIntegration.channel_id == shopify_channel.id,
                )
                .all()
            )

            for integration in integrations:
                try:
                    creds = decrypt_credentials(integration.credentials)
                    if creds.get("shop_domain") == shop_domain:
                        # Mark for deletion or anonymize
                        integration.is_active = False
                        integration.credentials = ""  # Clear credentials
                        db.commit()
                        logger.info(
                            f"Redacted integration {integration.id} for shop {shop_domain}"
                        )
                        break
                except:
                    continue

        return JSONResponse(status_code=200, content={"ok": True})

    except Exception as e:
        logger.error(f"Error processing shop/redact webhook: {str(e)}")
        return JSONResponse(status_code=200, content={"ok": True})


@webhook_router.post("/customers_data_request")
async def gdpr_customers_data_request(
    request: Request,
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    db: Session = Depends(get_db),
):
    """
    GDPR: Customer data request
    REQUIRED: Must respond within 48 hours

    Shopify sends this when a customer requests their data
    """
    try:
        if not await verify_shopify_webhook(request, x_shopify_hmac_sha256):
            return JSONResponse(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                content={"error": "Invalid HMAC"},
            )

        payload = await request.json()

        customer_id = payload.get("customer", {}).get("id")
        customer_email = payload.get("customer", {}).get("email")

        logger.info(
            f"GDPR: Customer data request from {x_shopify_shop_domain} for customer {customer_id}"
        )

        # TODO: Implement customer data export
        # 1. Collect all data related to this customer
        # 2. Generate exportable format (JSON/CSV)
        # 3. Send to customer via email
        # 4. Must complete within 48 hours

        logger.info(
            f"Customer data request: {customer_email} from {x_shopify_shop_domain}"
        )

        return JSONResponse(status_code=200, content={"ok": True})

    except Exception as e:
        logger.error(f"Error processing customers/data_request webhook: {str(e)}")
        return JSONResponse(status_code=200, content={"ok": True})
