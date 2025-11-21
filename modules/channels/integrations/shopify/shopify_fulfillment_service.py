"""
Shopify Fulfillment Service
Pushes tracking information back to Shopify when AWB is generated
"""

from typing import Optional
from logger import logger
from context_manager.context import get_db_session
from models import Order, ClientChannelIntegration, ChannelMaster
from utils.credential_encryption import decrypt_credentials
from .shopify_connector import ShopifyConnector


class ShopifyFulfillmentService:
    """Service to push fulfillment/tracking data back to Shopify"""
    
    @staticmethod
    async def push_tracking_to_shopify(
        order_id: str,
        awb_number: str,
        courier_name: str,
        client_id: int
    ) -> bool:
        """
        Push tracking information to Shopify when AWB is generated
        
        Args:
            order_id: Internal order ID
            awb_number: AWB/tracking number
            courier_name: Courier company name
            client_id: Client ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            db = get_db_session()
            
            # Get the order
            order = db.query(Order).filter(
                Order.order_id == order_id,
                Order.client_id == client_id,
                Order.source == "shopify_oauth",
            ).first()
            
            if not order:
                logger.warning(f"Order {order_id} not found or not from Shopify OAuth")
                return False
            
            marketplace_order_id = order.marketplace_order_id
            
            if not marketplace_order_id:
                logger.warning(f"No marketplace_order_id for order {order_id}")
                return False
            
            # Get Shopify integration for this client
            shopify_channel = db.query(ChannelMaster).filter(
                ChannelMaster.slug == "shopify"
            ).first()
            
            if not shopify_channel:
                logger.warning("Shopify channel not found in system")
                return False
            
            integration = db.query(ClientChannelIntegration).filter(
                ClientChannelIntegration.client_id == client_id,
                ClientChannelIntegration.channel_id == shopify_channel.id,
                ClientChannelIntegration.is_active == True,
            ).first()
            
            if not integration:
                logger.warning(f"No active Shopify integration for client {client_id}")
                return False
            
            # Decrypt credentials
            credentials = decrypt_credentials(integration.credentials)
            
            # Create connector
            connector = ShopifyConnector(
                credentials["shop_domain"],
                credentials["access_token"]
            )
            
            # Build tracking URL
            tracking_url = f"https://app.lastmiles.co/tracking/awb/{awb_number}"
            
            # Create fulfillment in Shopify
            result = await connector.create_fulfillment(
                order_id=marketplace_order_id,
                tracking_number=awb_number,
                tracking_url=tracking_url,
                courier_name=courier_name,
                notify_customer=True
            )
            
            if result["success"]:
                logger.info(f"Successfully pushed tracking for order {order_id} to Shopify")
                
                # Update order action history
                from sqlalchemy.orm.attributes import flag_modified
                from datetime import datetime
                
                order.action_history.append({
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": f"Tracking pushed to Shopify: {awb_number}",
                })
                flag_modified(order, "action_history")
                db.commit()
                
                return True
            else:
                logger.error(f"Failed to push tracking to Shopify: {result['message']}")
                return False
                
        except Exception as e:
            logger.error(f"Error pushing tracking to Shopify: {str(e)}")
            return False
    
    @staticmethod
    def should_push_to_shopify(order: Order) -> bool:
        """
        Check if tracking should be pushed to Shopify for this order
        
        Args:
            order: Order instance
            
        Returns:
            True if should push, False otherwise
        """
        # Only push for Shopify OAuth orders
        if order.source != "shopify_oauth":
            return False
        
        # Don't push if already self-fulfilled
        if order.order_tags and "self_fulfilled" in order.order_tags:
            return False
        
        # Must have AWB number
        if not order.awb_number:
            return False
        
        return True


