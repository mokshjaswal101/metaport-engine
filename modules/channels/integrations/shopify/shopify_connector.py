"""
Shopify API Connector
Handles all API interactions with Shopify including rate limiting, retries, and error handling
"""

import http
import httpx
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from logger import logger

from .shopify_config import (
    SHOPIFY_API_VERSION,
    SHOPIFY_API_KEY,
    SHOPIFY_API_SECRET,
    WEBHOOK_TOPICS,
)


class ShopifyConnector:
    """Connector for Shopify API with rate limiting and retry logic"""

    def __init__(self, shop_domain: str, access_token: str):
        """
        Initialize Shopify connector

        Args:
            shop_domain: Shopify store domain (e.g., mystore.myshopify.com)
            access_token: Shopify access token
        """
        self.shop_domain = shop_domain.rstrip("/")
        self.access_token = access_token
        self.base_url = f"https://{self.shop_domain}/admin/api/{SHOPIFY_API_VERSION}"
        self.headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        }
        self.rate_limit_delay = 0.5  # Default delay between requests (seconds)

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Make HTTP request to Shopify API with rate limiting and retry logic

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            json_data: JSON body for POST/PUT requests
            retries: Number of retry attempts

        Returns:
            Response dictionary with 'success', 'data', 'message', 'status_code'

        Raises:
            Exception: If request fails after all retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        for attempt in range(retries):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=self.headers,
                        params=params,
                        json=json_data,
                    )

                    # Handle rate limiting (429)
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 2))
                        logger.warning(
                            f"Rate limited for {self.shop_domain}, waiting {retry_after}s"
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    # Handle successful responses
                    if response.status_code in [200, 201]:
                        return {
                            "success": True,
                            "data": response.json() if response.content else {},
                            "status_code": response.status_code,
                        }

                    # Handle 401 Unauthorized (token expired)
                    if response.status_code == 401:
                        logger.error(
                            f"Unauthorized access for {self.shop_domain} - token may be expired"
                        )
                        return {
                            "success": False,
                            "message": "Unauthorized - access token may have expired",
                            "status_code": 401,
                            "data": {},
                        }

                    # Handle other errors
                    error_data = {}
                    try:
                        error_data = response.json()
                    except:
                        error_data = {"error": response.text}

                    logger.error(
                        f"Shopify API error for {self.shop_domain}: {response.status_code} - {error_data}"
                    )

                    return {
                        "success": False,
                        "message": (
                            error_data.get("errors", {}).get("base", [response.text])[0]
                            if isinstance(
                                error_data.get("errors", {}).get("base"), list
                            )
                            else str(error_data.get("errors", response.text))
                        ),
                        "status_code": response.status_code,
                        "data": error_data,
                    }

            except httpx.TimeoutException:
                logger.warning(
                    f"Request timeout for {self.shop_domain} (attempt {attempt + 1}/{retries})"
                )
                if attempt < retries - 1:
                    await asyncio.sleep(2**attempt)  # Exponential backoff
                    continue
                return {
                    "success": False,
                    "message": "Request timeout - Shopify API did not respond",
                    "status_code": 408,
                    "data": {},
                }

            except Exception as e:
                logger.error(
                    f"Error making request to Shopify for {self.shop_domain}: {str(e)}",
                    exc_info=True,
                )
                if attempt < retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                return {
                    "success": False,
                    "message": f"Request failed: {str(e)}",
                    "status_code": 500,
                    "data": {},
                }

            # Small delay between requests to respect rate limits
            await asyncio.sleep(self.rate_limit_delay)

        return {
            "success": False,
            "message": "Request failed after all retries",
            "status_code": 500,
            "data": {},
        }

    async def test_connection(self) -> Dict[str, Any]:
        """
        Test connection to Shopify API by fetching shop information

        Returns:
            Dictionary with 'success', 'data' (shop info), 'message'
        """
        result = await self._make_request("GET", "shop.json")

        if result["success"]:
            shop_data = result["data"].get("shop", {})
            return {
                "success": True,
                "data": {
                    "shop_domain": self.shop_domain,
                    "shop_name": shop_data.get("name"),
                    "shop_email": shop_data.get("email"),
                    "shop_owner": shop_data.get("shop_owner"),
                    "plan_name": shop_data.get("plan_name"),
                    "currency": shop_data.get("currency"),
                },
                "message": "Connection successful",
            }
        else:
            return {
                "success": False,
                "data": {},
                "message": result.get("message", "Failed to connect to Shopify"),
            }

    async def fetch_orders(
        self,
        since_date: Optional[datetime] = None,
        fulfillment_status: Optional[str] = None,
        limit: int = 250,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch orders from Shopify

        Args:
            since_date: Fetch orders created after this date
            fulfillment_status: Filter by fulfillment status ('unfulfilled', 'fulfilled', 'partial')
            limit: Maximum number of orders to fetch (default 250, max 250)
            status: Filter by order status ('open', 'closed', 'cancelled', 'any')

        Returns:
            List of order dictionaries
        """
        params = {"limit": min(limit, 250)}

        if since_date:
            params["created_at_min"] = since_date.isoformat()

        if fulfillment_status:
            params["fulfillment_status"] = fulfillment_status

        if status:
            params["status"] = status

        all_orders = []
        page_info = None

        while True:
            if page_info:
                params["page_info"] = page_info
                # Remove created_at_min when using pagination
                params.pop("created_at_min", None)

            result = await self._make_request("GET", "orders.json", params=params)

            if not result["success"]:
                logger.error(f"Failed to fetch orders: {result.get('message')}")
                break

            orders_data = result["data"].get("orders", [])
            all_orders.extend(orders_data)

            # Check for pagination
            link_header = result.get("headers", {}).get("link", "")
            if 'rel="next"' not in link_header:
                break

            # Extract page_info from link header
            # Link header format: <url>; rel="next"
            try:
                next_link = [
                    link for link in link_header.split(",") if 'rel="next"' in link
                ][0]
                page_info = next_link.split("page_info=")[1].split("&")[0]
            except:
                break

            if len(orders_data) < limit:
                break

        logger.info(f"Fetched {len(all_orders)} orders from {self.shop_domain}")
        return all_orders

    async def get_webhooks(self) -> List[Dict[str, Any]]:
        """
        Get all registered webhooks for this shop

        Returns:
            List of webhook dictionaries
        """
        result = await self._make_request("GET", "webhooks.json")

        if result["success"]:
            return result["data"].get("webhooks", [])
        else:
            logger.error(f"Failed to fetch webhooks: {result.get('message')}")
            return []

    async def create_webhook(self, topic: str, address: str) -> Dict[str, Any]:
        """
        Create a webhook subscription

        Args:
            topic: Webhook topic (e.g., 'orders/create')
            address: Webhook URL

        Returns:
            Dictionary with 'success', 'data' (webhook info), 'message'
        """
        webhook_data = {
            "webhook": {
                "topic": topic,
                "address": address,
                "format": "json",
            }
        }

        result = await self._make_request(
            "POST", "webhooks.json", json_data=webhook_data
        )

        if result["success"]:
            webhook = result["data"].get("webhook", {})
            logger.info(
                f"Created webhook {topic} for {self.shop_domain}: {webhook.get('id')}"
            )
            return {
                "success": True,
                "data": webhook,
                "message": f"Webhook {topic} created successfully",
            }
        else:
            logger.error(f"Failed to create webhook {topic}: {result.get('message')}")
            return {
                "success": False,
                "data": {},
                "message": result.get("message", "Failed to create webhook"),
            }

    async def update_webhook(self, webhook_id: int, address: str) -> Dict[str, Any]:
        """
        Update an existing webhook

        Args:
            webhook_id: Webhook ID
            address: New webhook URL

        Returns:
            Dictionary with 'success', 'data', 'message'
        """
        webhook_data = {
            "webhook": {
                "address": address,
            }
        }

        result = await self._make_request(
            "PUT", f"webhooks/{webhook_id}.json", json_data=webhook_data
        )

        if result["success"]:
            webhook = result["data"].get("webhook", {})
            logger.info(f"Updated webhook {webhook_id} for {self.shop_domain}")
            return {
                "success": True,
                "data": webhook,
                "message": "Webhook updated successfully",
            }
        else:
            logger.error(
                f"Failed to update webhook {webhook_id}: {result.get('message')}"
            )
            return {
                "success": False,
                "data": {},
                "message": result.get("message", "Failed to update webhook"),
            }

    async def delete_webhook(self, webhook_id: int) -> bool:
        """
        Delete a webhook

        Args:
            webhook_id: Webhook ID

        Returns:
            True if deleted successfully, False otherwise
        """
        result = await self._make_request("DELETE", f"webhooks/{webhook_id}.json")

        if result["success"]:
            logger.info(f"Deleted webhook {webhook_id} for {self.shop_domain}")
            return True
        else:
            logger.error(
                f"Failed to delete webhook {webhook_id}: {result.get('message')}"
            )
            return False

    async def register_all_webhooks(
        self, webhook_base_url: str, integration_id: int
    ) -> List[Dict[str, Any]]:
        """
        Register all required webhooks for the integration

        Args:
            webhook_base_url: Base URL for webhook endpoints (e.g., https://api.example.com)
            integration_id: Integration ID to include in webhook URL for direct lookup

        Returns:
            List of registration results
        """
        results = []
        existing_webhooks = await self.get_webhooks()

        # Create a map of existing webhooks by topic
        webhook_map = {wh.get("topic"): wh for wh in existing_webhooks}

        for topic in WEBHOOK_TOPICS:
            # Include integration_id in webhook URL for optimized lookup
            # Map topic to route format: app/uninstalled -> app_uninstalled
            topic_route = topic.replace("/", "_")
            webhook_url = f"{webhook_base_url.rstrip('/')}/{topic_route}?integration_id={integration_id}"

            existing_webhook = webhook_map.get(topic)

            if existing_webhook:
                # Check if URL matches
                if existing_webhook.get("address") == webhook_url:
                    logger.info(f"Webhook {topic} already registered with correct URL")
                    results.append(
                        {
                            "topic": topic,
                            "success": True,
                            "action": "skipped",
                            "message": "Webhook already registered",
                            "webhook_id": existing_webhook.get("id"),
                        }
                    )
                else:
                    # Update webhook URL
                    logger.info(f"Updating webhook {topic} URL")
                    update_result = await self.update_webhook(
                        existing_webhook.get("id"), webhook_url
                    )
                    results.append(
                        {
                            "topic": topic,
                            "success": update_result["success"],
                            "action": "updated",
                            "message": update_result.get("message"),
                            "webhook_id": existing_webhook.get("id"),
                        }
                    )
            else:
                # Create new webhook
                logger.info(f"Creating new webhook {topic}")
                create_result = await self.create_webhook(topic, webhook_url)
                results.append(
                    {
                        "topic": topic,
                        "success": create_result["success"],
                        "action": "created",
                        "message": create_result.get("message"),
                        "webhook_id": create_result.get("data", {}).get("id"),
                    }
                )

        return results

    async def create_fulfillment(
        self,
        order_id: str,
        tracking_number: str,
        tracking_url: str,
        courier_name: str,
        notify_customer: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a fulfillment for an order

        Args:
            order_id: Shopify order ID
            tracking_number: Tracking number (AWB)
            tracking_url: Tracking URL
            courier_name: Courier name
            notify_customer: Whether to notify customer

        Returns:
            Dictionary with 'success', 'data', 'message'
        """
        # First, get the fulfillment order ID
        order_result = await self._make_request("GET", f"orders/{order_id}.json")

        if not order_result["success"]:
            return {
                "success": False,
                "message": f"Order {order_id} not found",
                "data": {},
            }

        order_data = order_result["data"].get("order", {})
        line_items = order_data.get("line_items", [])

        if not line_items:
            return {
                "success": False,
                "message": f"Order {order_id} has no line items",
                "data": {},
            }

        # Create fulfillment data
        fulfillment_data = {
            "fulfillment": {
                "notify_customer": notify_customer,
                "tracking_info": {
                    "number": tracking_number,
                    "url": tracking_url,
                    "company": courier_name,
                },
                "line_items_by_fulfillment_order": [
                    {
                        "fulfillment_order_id": line_items[0].get(
                            "fulfillment_order_id"
                        ),
                    }
                ],
            }
        }

        # If fulfillment_order_id is not available, use line item IDs
        if not line_items[0].get("fulfillment_order_id"):
            fulfillment_data["fulfillment"]["line_items"] = [
                {"id": item.get("id")} for item in line_items
            ]

        result = await self._make_request(
            "POST", f"orders/{order_id}/fulfillments.json", json_data=fulfillment_data
        )

        if result["success"]:
            fulfillment = result["data"].get("fulfillment", {})
            logger.info(
                f"Created fulfillment for order {order_id} in {self.shop_domain}"
            )
            return {
                "success": True,
                "data": fulfillment,
                "message": "Fulfillment created successfully",
            }
        else:
            logger.error(
                f"Failed to create fulfillment for order {order_id}: {result.get('message')}"
            )
            return {
                "success": False,
                "data": {},
                "message": result.get("message", "Failed to create fulfillment"),
            }
