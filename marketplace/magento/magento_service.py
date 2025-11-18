import requests
from typing import Dict, List, Optional, Any
import json
from datetime import datetime, timedelta
import re
import pandas as pd
from fastapi import HTTPException
import httpx

from context_manager.context import context_user_data, get_db_session
from logger import logger

# models
from models import Pickup_Location, Order, Market_Place

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel
from modules.shipping_notifications.shipping_notifications_service import (
    ShippingNotificaitions,
)

# Store credentials for different clients
# The structure is {client_id: {marketplace_id: {credentials}}}
creds = {
    2: {
        1: {
            "BASE_URL": "http://beta.myimaginestore.com",
            "USERNAME": "nakul",
            "PASSWORD": "nakul123#",
        }
    }
}


class Magento:
    """
    Magento service class for handling interactions with Magento API
    """

    def __init__(self, client_id: int, marketplace_id: int):
        """
        Initialize Magento client with client and marketplace IDs

        Args:
            client_id: Client ID
            marketplace_id: Marketplace ID
        """
        self.client_id = client_id
        self.marketplace_id = marketplace_id

        print("inside the magento function")

        # Get credentials for the client and marketplace
        if client_id in creds and marketplace_id in creds[client_id]:
            self.creds = creds[client_id][marketplace_id]
            self.base_url = self.creds.get("BASE_URL")
            self.username = self.creds.get("USERNAME")
            self.password = self.creds.get("PASSWORD")
        else:
            # If credentials not found in the hardcoded dict, try to fetch from database
            try:
                with get_db_session() as db:
                    marketplace = (
                        db.query(Market_Place)
                        .filter(
                            Market_Place.client_id == client_id,
                            Market_Place.id == marketplace_id,
                        )
                        .first()
                    )

                    if marketplace:
                        self.base_url = marketplace.base_url
                        self.username = marketplace.username
                        self.password = (
                            marketplace.access_token
                        )  # Using access_token field to store password
                    else:
                        raise Exception(
                            f"No credentials found for client_id: {client_id}, marketplace_id: {marketplace_id}"
                        )
            except Exception as e:
                logger.error(f"Error fetching Magento credentials: {str(e)}")
                raise HTTPException(
                    status_code=500, detail="Failed to initialize Magento client"
                )

        # Initialize session with auth token
        self.session = None
        self.token = None

    def _get_auth_token(self) -> str:
        """
        Get authentication token from Magento API

        Returns:
            str: Authentication token
        """
        try:
            # Try different possible authentication endpoints
            endpoints_to_try = [
                "/rest/V1/integration/admin/token",
                "/rest/all/V1/integration/admin/token",
                "/api/rest/admin/token",
                "/rest/default/V1/integration/admin/token",
                "/index.php/rest/V1/integration/admin/token",
                "/api/index.php/integration/admin/token",
                "/webapi/rest/V1/integration/admin/token",
                "/magento/rest/V1/integration/admin/token",
                "/store/rest/V1/integration/admin/token",
            ]

            payload = {"username": self.username, "password": self.password}
            headers = {"Content-Type": "application/json"}

            # First, let's check what's available on the server
            logger.info(f"Checking server structure for: {self.base_url}")

            try:
                # Check the main page to see if it's actually a Magento store
                main_response = requests.get(self.base_url, timeout=10)
                logger.info(f"Main page response: {main_response.status_code}")

                if main_response.status_code == 200:
                    content_lower = main_response.text.lower()
                    if "magento" in content_lower:
                        logger.info("Confirmed: This appears to be a Magento store")
                    elif "shopify" in content_lower:
                        logger.warning(
                            "This appears to be a Shopify store, not Magento"
                        )
                        raise HTTPException(
                            status_code=400,
                            detail="This appears to be a Shopify store, not a Magento store",
                        )
                    elif "woocommerce" in content_lower or "wordpress" in content_lower:
                        logger.warning(
                            "This appears to be a WooCommerce store, not Magento"
                        )
                        raise HTTPException(
                            status_code=400,
                            detail="This appears to be a WooCommerce store, not a Magento store",
                        )
                    else:
                        logger.warning("Could not detect platform type from main page")

            except requests.exceptions.RequestException as e:
                logger.warning(f"Could not check main page: {str(e)}")

            for endpoint in endpoints_to_try:
                url = f"{self.base_url}{endpoint}"
                logger.info(f"Trying authentication endpoint: {url}")

                try:
                    response = requests.post(
                        url, headers=headers, data=json.dumps(payload), timeout=30
                    )

                    if response.status_code == 200:
                        # Token is returned as a JSON string with quotes, remove them
                        token = response.json()
                        if isinstance(token, str):
                            token = token.strip('"')
                        logger.info(
                            f"Successfully authenticated with endpoint: {endpoint}"
                        )
                        return token
                    else:
                        logger.warning(
                            f"Authentication failed for {endpoint}: {response.status_code} - {response.text[:200]}"
                        )

                except requests.exceptions.RequestException as req_e:
                    logger.warning(f"Request failed for {endpoint}: {str(req_e)}")
                    continue

            # If all endpoints failed, try to get more information about the server
            logger.error(
                "All authentication endpoints failed. Checking server response..."
            )

            # Try a simple GET request to see what the server responds with
            try:
                test_url = f"{self.base_url}/rest"
                test_response = requests.get(test_url, timeout=10)
                logger.info(
                    f"Server test response: {test_response.status_code} - {test_response.text[:500]}"
                )
            except Exception as test_e:
                logger.error(f"Server test failed: {str(test_e)}")

            raise HTTPException(
                status_code=404,
                detail=f"Failed to authenticate with Magento. Tried multiple endpoints. Server: {self.base_url}",
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting Magento auth token: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to authenticate with Magento: {str(e)}"
            )

    def _get_session(self):
        """
        Get or create an authenticated session

        Returns:
            requests.Session: Authenticated session
        """
        if not self.token:
            self.token = self._get_auth_token()

        if not self.session:
            self.session = requests.Session()
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                }
            )

        return self.session

    def test_connection(self) -> Dict:
        """
        Test the connection to Magento by attempting to get a simple resource

        Returns:
            Dict: Connection test result with status and details
        """
        try:
            logger.info(f"Testing connection to Magento: {self.base_url}")
            logger.info(f"Using username: {self.username}")

            # First, test basic connectivity to the server
            try:
                basic_response = requests.get(self.base_url, timeout=10)
                logger.info(f"Basic server response: {basic_response.status_code}")
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Cannot reach server at {self.base_url}: {str(e)}",
                    "base_url": self.base_url,
                    "step": "basic_connectivity",
                }

            # Try to get authentication token
            try:
                session = self._get_session()
                logger.info("Successfully obtained authentication session")
            except Exception as auth_e:
                return {
                    "status": "error",
                    "message": f"Authentication failed: {str(auth_e)}",
                    "base_url": self.base_url,
                    "step": "authentication",
                    "username": self.username,
                }

            # Try to get store information as a simple connection test
            endpoints_to_test = [
                "/rest/V1/store/storeConfigs",
                "/rest/all/V1/store/storeConfigs",
                "/rest/default/V1/store/storeConfigs",
                "/rest/V1/directory/countries",
            ]

            for endpoint in endpoints_to_test:
                try:
                    url = f"{self.base_url}{endpoint}"
                    response = session.get(url, timeout=30)

                    if response.status_code == 200:
                        data = response.json()
                        return {
                            "status": "success",
                            "message": "Successfully connected to Magento",
                            "endpoint_used": endpoint,
                            "data_count": len(data) if isinstance(data, list) else 1,
                            "base_url": self.base_url,
                            "response_code": response.status_code,
                            "username": self.username,
                        }
                    else:
                        logger.warning(
                            f"Endpoint {endpoint} failed: {response.status_code} - {response.text[:200]}"
                        )

                except Exception as endpoint_e:
                    logger.warning(f"Endpoint {endpoint} error: {str(endpoint_e)}")
                    continue

            return {
                "status": "error",
                "message": "Authentication successful but no working API endpoints found",
                "base_url": self.base_url,
                "step": "api_endpoints",
                "endpoints_tried": endpoints_to_test,
            }

        except Exception as e:
            logger.error(f"Error testing Magento connection: {str(e)}")
            return {
                "status": "error",
                "message": f"Connection test failed: {str(e)}",
                "base_url": self.base_url,
                "exception": str(e),
            }

    def get_orders(self, params: Dict = None) -> List[Dict]:
        """
        Fetch orders from Magento

        Args:
            params: Query parameters for filtering orders

        Returns:
            List[Dict]: List of orders
        """
        try:
            session = self._get_session()

            # Default parameters if none provided
            if not params:
                params = {
                    "searchCriteria[pageSize]": 50,
                    "searchCriteria[currentPage]": 1,
                    # Add more default filters as needed
                    "searchCriteria[filterGroups][0][filters][0][field]": "status",
                    "searchCriteria[filterGroups][0][filters][0][value]": "processing",
                    "searchCriteria[filterGroups][0][filters][0][condition_type]": "eq",
                }

            url = f"{self.base_url}/rest/V1/orders"
            response = session.get(url, params=params)

            if response.status_code == 200:
                return response.json().get("items", [])
            else:
                logger.error(f"Failed to fetch Magento orders: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to fetch orders from Magento: {response.text}",
                )
        except Exception as e:
            logger.error(f"Error fetching Magento orders: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to fetch orders from Magento"
            )

    def get_order_by_id(self, order_id: str) -> Dict:
        """
        Fetch a specific order by ID

        Args:
            order_id: Magento order ID

        Returns:
            Dict: Order details
        """
        try:
            session = self._get_session()

            url = f"{self.base_url}/rest/V1/orders/{order_id}"
            response = session.get(url)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(
                    f"Failed to fetch Magento order {order_id}: {response.text}"
                )
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to fetch order {order_id} from Magento: {response.text}",
                )
        except Exception as e:
            logger.error(f"Error fetching Magento order {order_id}: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to fetch order {order_id} from Magento"
            )

    def create_webhook(self, webhook_url: str, webhook_type: str = "orders") -> Dict:
        """
        Register a webhook in Magento for receiving events

        Args:
            webhook_url: The URL to send webhook events to
            webhook_type: Type of webhook (e.g., orders, products)

        Returns:
            Dict: Webhook registration response
        """
        try:
            session = self._get_session()

            # Magento uses different endpoint for webhooks depending on the module installed
            # This example assumes a standard webhook module
            url = f"{self.base_url}/rest/V1/webhooks"

            payload = {
                "webhook": {
                    "name": f"LastMilesWebhook-{webhook_type}",
                    "url": webhook_url,
                    "status": 1,  # Active
                    "topic": webhook_type,  # Topic like orders/create, orders/update
                    "format": "json",
                }
            }

            response = session.post(url, data=json.dumps(payload))

            if response.status_code in [200, 201]:
                return response.json()
            else:
                logger.error(f"Failed to create Magento webhook: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to create webhook in Magento: {response.text}",
                )
        except Exception as e:
            logger.error(f"Error creating Magento webhook: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create webhook in Magento"
            )

    def update_order_status(
        self, order_id: str, status: str, comment: str = None
    ) -> bool:
        """
        Update order status in Magento

        Args:
            order_id: Magento order ID
            status: New order status
            comment: Optional comment for the status change

        Returns:
            bool: Success status
        """
        try:
            session = self._get_session()

            url = f"{self.base_url}/rest/V1/orders/{order_id}/comments"

            payload = {
                "statusHistory": {
                    "comment": comment or f"Status updated to {status}",
                    "status": status,
                    "isCustomerNotified": True,
                    "isVisibleOnFront": False,
                }
            }

            response = session.post(url, data=json.dumps(payload))

            if response.status_code in [200, 201]:
                return True
            else:
                logger.error(f"Failed to update Magento order status: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to update order status in Magento: {response.text}",
                )
        except Exception as e:
            logger.error(f"Error updating Magento order status: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to update order status in Magento"
            )

    def create_shipment(self, order_id: str, tracking_info: Dict = None) -> Dict:
        """
        Create a shipment for an order in Magento

        Args:
            order_id: Magento order ID
            tracking_info: Dict with tracking information

        Returns:
            Dict: Shipment details
        """
        try:
            session = self._get_session()

            url = f"{self.base_url}/rest/V1/order/{order_id}/ship"

            # Prepare payload
            payload = {
                "items": [],  # Items to be shipped, if empty all items will be shipped
                "notify": True,  # Notify customer
            }

            # Add tracking information if provided
            if tracking_info:
                payload["tracks"] = [
                    {
                        "track_number": tracking_info.get("tracking_number"),
                        "title": tracking_info.get("courier_name", "Shipping"),
                        "carrier_code": tracking_info.get("courier_code", "custom"),
                    }
                ]

            response = session.post(url, data=json.dumps(payload))

            if response.status_code in [200, 201]:
                return response.json()
            else:
                logger.error(f"Failed to create Magento shipment: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to create shipment in Magento: {response.text}",
                )
        except Exception as e:
            logger.error(f"Error creating Magento shipment: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create shipment in Magento"
            )

    def map_magento_order_to_lastmiles(self, magento_order: Dict) -> Dict:
        """
        Map Magento order data to LastMiles order format

        Args:
            magento_order: Order data from Magento

        Returns:
            Dict: Order data in LastMiles format
        """
        try:
            # Extract shipping address
            shipping_address = next(
                (
                    addr
                    for addr in magento_order.get("addresses", [])
                    if addr.get("address_type") == "shipping"
                ),
                {},
            )

            # Extract billing address
            billing_address = next(
                (
                    addr
                    for addr in magento_order.get("addresses", [])
                    if addr.get("address_type") == "billing"
                ),
                {},
            )

            # Extract items
            items = []
            for item in magento_order.get("items", []):
                items.append(
                    {
                        "name": item.get("name", ""),
                        "sku": item.get("sku", ""),
                        "quantity": int(item.get("qty_ordered", 0)),
                        "price": float(item.get("price", 0)),
                        "weight": (
                            float(item.get("weight", 0)) if item.get("weight") else 0.5
                        ),  # Default weight if not provided
                    }
                )

            # Calculate order total
            order_total = float(magento_order.get("grand_total", 0))

            # Check if COD
            payment_method = magento_order.get("payment", {}).get("method", "")
            is_cod = payment_method.lower() == "cashondelivery"

            # Map to LastMiles order format
            lastmiles_order = {
                "client_order_id": magento_order.get("increment_id", ""),
                "marketplace_order_id": str(magento_order.get("entity_id", "")),
                "order_date": magento_order.get("created_at", ""),
                "customer_name": (
                    f"{shipping_address.get('firstname', '')} {shipping_address.get('lastname', '')}"
                ).strip(),
                "customer_email": magento_order.get("customer_email", ""),
                "customer_phone": shipping_address.get("telephone", ""),
                "shipping_address_line1": shipping_address.get("street", [""])[0],
                "shipping_address_line2": (
                    shipping_address.get("street", ["", ""])[1]
                    if len(shipping_address.get("street", [])) > 1
                    else ""
                ),
                "shipping_city": shipping_address.get("city", ""),
                "shipping_state": shipping_address.get("region", ""),
                "shipping_country": shipping_address.get("country_id", ""),
                "shipping_pincode": shipping_address.get("postcode", ""),
                "billing_name": (
                    f"{billing_address.get('firstname', '')} {billing_address.get('lastname', '')}"
                ).strip(),
                "billing_address_line1": billing_address.get("street", [""])[0],
                "billing_address_line2": (
                    billing_address.get("street", ["", ""])[1]
                    if len(billing_address.get("street", [])) > 1
                    else ""
                ),
                "billing_city": billing_address.get("city", ""),
                "billing_state": billing_address.get("region", ""),
                "billing_country": billing_address.get("country_id", ""),
                "billing_pincode": billing_address.get("postcode", ""),
                "order_items": items,
                "payment_method": payment_method,
                "order_value": order_total,
                "cod_amount": order_total if is_cod else 0,
                "is_cod": is_cod,
                "currency": magento_order.get("order_currency_code", "INR"),
                "weight": sum(
                    item.get("weight", 0.5) * item.get("quantity", 1) for item in items
                ),  # Total weight
                "status": magento_order.get("status", ""),
                "source": "magento",
            }

            return lastmiles_order
        except Exception as e:
            logger.error(f"Error mapping Magento order to LastMiles format: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to map Magento order to LastMiles format",
            )

    def fetch_and_sync_orders(self, days_back: int = 7) -> List[Dict]:
        """
        Fetch orders from Magento and sync them to LastMiles

        Args:
            days_back: Number of days to look back for orders

        Returns:
            List[Dict]: List of synced orders
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)

            # Format dates for Magento API
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            # Set up search criteria
            params = {
                "searchCriteria[pageSize]": 100,
                "searchCriteria[currentPage]": 1,
                "searchCriteria[filterGroups][0][filters][0][field]": "created_at",
                "searchCriteria[filterGroups][0][filters][0][value]": start_date_str,
                "searchCriteria[filterGroups][0][filters][0][condition_type]": "from",
                "searchCriteria[filterGroups][1][filters][0][field]": "created_at",
                "searchCriteria[filterGroups][1][filters][0][value]": end_date_str,
                "searchCriteria[filterGroups][1][filters][0][condition_type]": "to",
            }

            # Fetch orders
            magento_orders = self.get_orders(params)

            print(f"Fetched {len(magento_orders)} orders from Magento")

            return

            # Process and sync orders
            synced_orders = []
            for magento_order in magento_orders:
                try:
                    # Map to LastMiles format
                    lastmiles_order = self.map_magento_order_to_lastmiles(magento_order)

                    # Check if order already exists in database
                    with get_db_session() as db:
                        existing_order = (
                            db.query(Order)
                            .filter(
                                Order.client_order_id
                                == lastmiles_order.get("client_order_id")
                            )
                            .first()
                        )

                        if not existing_order:
                            # Create order in database
                            # Note: In a real implementation, you'd use proper database insert methods
                            # This is just a placeholder for the logic

                            # For demonstration purposes only
                            # In a real implementation, you should use your order creation service
                            synced_orders.append(lastmiles_order)

                            logger.info(
                                f"Successfully synced Magento order {lastmiles_order.get('client_order_id')}"
                            )
                        else:
                            logger.info(
                                f"Order {lastmiles_order.get('client_order_id')} already exists, skipping"
                            )

                except Exception as order_error:
                    # Log error but continue processing other orders
                    logger.error(
                        f"Error processing order {magento_order.get('increment_id')}: {str(order_error)}"
                    )

            return synced_orders

        except Exception as e:
            logger.error(f"Error syncing Magento orders: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to sync Magento orders")

    def handle_webhook_event(self, event_data: Dict) -> Dict:
        """
        Process incoming webhook events from Magento

        Args:
            event_data: Webhook event data

        Returns:
            Dict: Processing result
        """
        try:
            event_type = event_data.get("type", "")

            if "order" in event_type.lower():
                # This is an order-related event
                order_id = event_data.get("order_id")

                if not order_id:
                    raise HTTPException(
                        status_code=400, detail="Missing order_id in webhook data"
                    )

                # Fetch complete order details
                magento_order = self.get_order_by_id(order_id)

                # Map to LastMiles format
                lastmiles_order = self.map_magento_order_to_lastmiles(magento_order)

                # Process the order
                # In a real implementation, this would call your order service
                # For demonstration, we're just returning the mapped order
                return {
                    "success": True,
                    "message": f"Successfully processed webhook for order {order_id}",
                    "order": lastmiles_order,
                }
            else:
                # Handle other event types if needed
                return {
                    "success": True,
                    "message": f"Received webhook event of type {event_type}, no action taken",
                }

        except Exception as e:
            logger.error(f"Error handling Magento webhook event: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to process webhook event: {str(e)}"
            )
