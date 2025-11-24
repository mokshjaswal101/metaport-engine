"""
Shopify Order Sync Service
Maps Shopify orders to Last Miles internal order format
Handles order import and synchronization
"""

import http
from typing import Dict, Any, List, Optional
from datetime import datetime
import unicodedata
import re

from logger import logger
from context_manager.context import get_db_session
from models import Order, Pickup_Location, ClientChannelIntegration, IntegrationSyncLog
from utils.credential_encryption import decrypt_credentials
from schema.base import GenericResponseModel
from sqlalchemy.orm.attributes import flag_modified

from .shopify_connector import ShopifyConnector


class ShopifyOrderSync:
    """Service to sync orders from Shopify to Last Miles"""
    
    def __init__(self, integration: ClientChannelIntegration):
        """
        Initialize order sync service
        
        Args:
            integration: ClientChannelIntegration instance
        """
        self.integration = integration
        self.client_id = integration.client_id
        
        # Decrypt credentials
        credentials = decrypt_credentials(integration.credentials)
        self.shop_domain = credentials["shop_domain"]
        self.access_token = credentials["access_token"]
        
        # Create connector
        self.connector = ShopifyConnector(self.shop_domain, self.access_token)
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text fields"""
        if text is None:
            return ""
        # Normalize Unicode and replace non-breaking spaces
        text = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()
        # Replace special characters except comma and hyphen
        text = re.sub(r"[^a-zA-Z0-9\s,-]", " ", text)
        # Replace multiple spaces with single space
        return re.sub(r"\s+", " ", text).strip()
    
    def map_shopify_order_to_internal(self, shopify_order: Dict[str, Any], pickup_location_code: str) -> Dict[str, Any]:
        """
        Map Shopify order data to Last Miles internal format
        
        Args:
            shopify_order: Shopify order JSON
            pickup_location_code: Pickup location code to use
            
        Returns:
            Mapped order data dictionary
        """
        # Get addresses
        shipping_address = shopify_order.get("shipping_address") or {}
        billing_address = shopify_order.get("billing_address") or {}
        
        def get_address_field(field_name: str, default: str = "") -> str:
            """Get field from shipping address with fallback to billing"""
            return shipping_address.get(field_name) or billing_address.get(field_name, default)
        
        # Get and normalize phone number
        consignee_phone = get_address_field("phone", "") or ""
        if isinstance(consignee_phone, str) and consignee_phone.startswith("+91"):
            consignee_phone = consignee_phone.removeprefix("+91")
        
        # Fallback to customer phone if no address phone
        if not consignee_phone:
            customer_phone = shopify_order.get("customer", {}).get("phone", "") or ""
            if isinstance(customer_phone, str) and customer_phone.startswith("+91"):
                customer_phone = customer_phone.removeprefix("+91")
            consignee_phone = customer_phone
        
        # Determine payment mode
        payment_mode = "prepaid" if float(shopify_order.get("total_outstanding", 0)) == 0 else "COD"
        
        # Determine status
        if shopify_order.get("cancelled_at"):
            status = "cancelled"
        else:
            status = "new"
        
        # Calculate weight
        weight = 0.5  # Default
        if shopify_order.get("total_weight", 0) > 0:
            weight = shopify_order["total_weight"] / 1000  # grams to kg
        else:
            line_item_weight = sum(
                item.get("grams", 0) for item in shopify_order.get("line_items", [])
            )
            weight = line_item_weight / 1000 if line_item_weight > 0 else 0.5
        
        # Default dimensions (can be customized per client)
        length = 10
        breadth = 10
        height = 10
        
        # Build mapped order
        mapped_order = {
            "consignee_full_name": self.clean_text(
                f"{get_address_field('first_name')} {get_address_field('last_name')}".strip()
            ),
            "consignee_phone": consignee_phone,
            "consignee_email": shopify_order.get("email", ""),
            "consignee_address": self.clean_text(get_address_field("address1")),
            "consignee_landmark": self.clean_text(get_address_field("address2")),
            "consignee_pincode": get_address_field("zip"),
            "consignee_city": self.clean_text(get_address_field("city")),
            "consignee_country": "India",
            "consignee_state": get_address_field("province"),
            "pickup_location_code": pickup_location_code,
            "order_id": str(shopify_order["order_number"]),
            "order_date": shopify_order["created_at"],
            "channel": "shopify",
            "billing_is_same_as_consignee": True,
            "products": [
                {
                    "name": product["name"],
                    "quantity": product["quantity"],
                    "unit_price": product["price"],
                    "sku_code": product.get("sku") or "",
                    "line_item_id": product["id"],
                }
                for product in shopify_order.get("line_items", [])
            ],
            "payment_mode": payment_mode,
            "total_amount": shopify_order["total_price"],
            "order_value": sum(
                float(product["quantity"]) * float(product["price"])
                for product in shopify_order.get("line_items", [])
            ),
            "client_id": self.client_id,
            "source": "shopify_oauth",  # Different from manual integration
            "marketplace_order_id": str(shopify_order["id"]),
            "status": status,
            "sub_status": status,
            "length": length,
            "breadth": breadth,
            "height": height,
            "weight": round(weight, 3),
            "company_id": 1,
            "order_type": "B2C",
            "discount": float(shopify_order.get("total_discounts", 0)),
            "tax_amount": (
                0 if shopify_order.get("taxes_included") == True
                else float(shopify_order.get("total_tax", 0))
            ),
            "shipping_charges": sum(
                float(item["price"]) for item in shopify_order.get("shipping_lines", [])
            ),
        }
        
        # Calculate volumetric weight
        volumetric_weight = round((length * breadth * height) / 5000, 3)
        applicable_weight = round(max(mapped_order["weight"], volumetric_weight), 3)
        
        mapped_order["volumetric_weight"] = volumetric_weight
        mapped_order["applicable_weight"] = applicable_weight
        
        # Calculate product quantity
        mapped_order["product_quantity"] = sum(
            product["quantity"] for product in mapped_order["products"]
        )
        
        # Parse tags
        tags = shopify_order.get("tags", "")
        mapped_order["order_tags"] = tags.split(",") if tags else []
        
        # Calculate zone
        from modules.shipment.shipment_service import ShipmentService
        db = get_db_session()
        pickup_pincode = db.query(Pickup_Location.pincode).filter(
            Pickup_Location.location_code == pickup_location_code
        ).first()
        
        if pickup_pincode:
            zone_data = ShipmentService.calculate_shipping_zone(
                pickup_pincode[0], mapped_order["consignee_pincode"]
            )
            mapped_order["zone"] = zone_data.data.get("zone", "D")
        else:
            mapped_order["zone"] = "D"
        
        return mapped_order
    
    async def import_single_order(self, shopify_order: Dict[str, Any]) -> GenericResponseModel:
        """
        Import a single order from Shopify
        
        Args:
            shopify_order: Shopify order JSON
            
        Returns:
            GenericResponseModel with success/failure status
        """
        try:
            db = get_db_session()
            
            # Get or create pickup location
            pickup_location_code = await self._get_or_create_pickup_location(shopify_order)
            
            if not pickup_location_code:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="No pickup location available"
                )
            
            # Map order
            mapped_order = self.map_shopify_order_to_internal(shopify_order, pickup_location_code)
            
            # Check if order already exists
            existing_order = db.query(Order).filter(
                Order.marketplace_order_id == mapped_order["marketplace_order_id"],
                Order.client_id == self.client_id,
            ).first()
            
            # Handle fulfilled orders
            fulfillment_status = shopify_order.get("fulfillment_status")
            if fulfillment_status == "fulfilled":
                current_tags = mapped_order.get("order_tags", [])
                if "self_fulfilled" not in current_tags:
                    current_tags.append("self_fulfilled")
                    mapped_order["order_tags"] = current_tags
            
            is_new_order = existing_order is None
            
            if existing_order:
                # Update existing order
                # Skip update if order is already fulfilled and not in new/cancelled status
                if fulfillment_status == "fulfilled" and existing_order.status not in ["new", "cancelled"]:
                    return GenericResponseModel(
                        status=True,
                        status_code=http.HTTPStatus.OK,
                        message="Order already fulfilled, skipping update"
                    )
                
                response = self._update_existing_order(existing_order, mapped_order, db)
            else:
                # Create new order
                response = self._create_new_order(mapped_order, db)
            
            # Update integration sync stats - only increment counter for new orders
            if response.status:
                if is_new_order:
                    self.integration.total_orders_synced += 1
                self.integration.last_order_sync_at = datetime.utcnow()
                self.integration.last_successful_sync_at = datetime.utcnow()
                db.commit()
            
            return response
            
        except Exception as e:
            logger.error(f"Error importing Shopify order: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=f"Failed to import order: {str(e)}"
            )
    
    async def _get_or_create_pickup_location(self, shopify_order: Dict[str, Any]) -> Optional[str]:
        """
        Get existing pickup location or create from Shopify store address
        
        Returns:
            Pickup location code or None
        """
        db = get_db_session()
        
        # Get default pickup location for client
        default_location = db.query(Pickup_Location).filter(
            Pickup_Location.client_id == self.client_id,
            Pickup_Location.is_default == True,
            Pickup_Location.is_deleted == False,
        ).first()
        
        if default_location:
            return default_location.location_code
        
        # Get any active location
        any_location = db.query(Pickup_Location).filter(
            Pickup_Location.client_id == self.client_id,
            Pickup_Location.is_deleted == False,
        ).first()
        
        if any_location:
            return any_location.location_code
        
        # TODO: Auto-create pickup location from Shopify store address
        # This requires fetching shop info and creating a new Pickup_Location
        
        return None
    
    def _create_new_order(self, order_data: Dict[str, Any], db) -> GenericResponseModel:
        """Create a new order"""
        if order_data.get("status") == "cancelled":
            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Cancelled order, not importing"
            )
        
        order_data["action_history"] = [
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "message": "Order imported from Shopify (OAuth integration)",
            }
        ]
        
        new_order = Order.create_db_entity(order_data)
        db.add(new_order)
        db.commit()
        
        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.CREATED,
            message="Order created successfully"
        )
    
    def _update_existing_order(self, existing_order: Order, order_data: Dict[str, Any], db) -> GenericResponseModel:
        """Update an existing order with new data from Shopify"""
        
        # Handle cancellation
        if order_data.get("status") == "cancelled":
            existing_order.status = "cancelled"
            existing_order.sub_status = "cancelled"
            existing_order.action_history.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": "Order cancelled on Shopify",
                }
            )
            flag_modified(existing_order, "action_history")
            db.commit()
            
            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Order cancelled successfully"
            )
        
        # For orders that are already processed (shipped/delivered), only update non-critical fields
        # Critical fields: address, products, payment mode, order value
        # Non-critical fields: tags, notes, metadata
        is_processed = existing_order.status not in ["new", "cancelled"]
        
        # Fields that can always be updated
        safe_fields = [
            "order_tags", "consignee_email", "consignee_phone",
            "discount", "tax_amount", "shipping_charges"
        ]
        
        # Fields that can only be updated if order is not processed
        critical_fields = [
            "consignee_full_name", "consignee_address", "consignee_landmark",
            "consignee_pincode", "consignee_city", "consignee_state",
            "products", "payment_mode", "total_amount", "order_value",
            "weight", "length", "breadth", "height", "volumetric_weight",
            "applicable_weight", "product_quantity", "zone"
        ]
        
        # Update safe fields (always allowed)
        for field in safe_fields:
            if field in order_data and hasattr(existing_order, field):
                setattr(existing_order, field, order_data[field])
        
        # Update critical fields only if order is not processed
        if not is_processed:
            for field in critical_fields:
                if field in order_data and hasattr(existing_order, field):
                    setattr(existing_order, field, order_data[field])
        else:
            # For processed orders, log that update was attempted but restricted
            logger.info(
                f"Order {existing_order.order_id} (status: {existing_order.status}) "
                f"update restricted - only safe fields updated"
            )
        
        # Handle JSON fields that need flag_modified
        if "order_tags" in order_data:
            flag_modified(existing_order, "order_tags")
        if "products" in order_data and not is_processed:
            flag_modified(existing_order, "products")
        
        # Add update history
        existing_order.action_history.append(
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "message": f"Order updated from Shopify{' (safe fields only)' if is_processed else ''}",
            }
        )
        flag_modified(existing_order, "action_history")
        
        existing_order.updated_at = datetime.utcnow()
        db.commit()
        
        return GenericResponseModel(
            status=True,
            status_code=http.HTTPStatus.OK,
            message="Order updated successfully"
        )


