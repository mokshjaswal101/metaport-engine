import http
from psycopg2 import DatabaseError
from typing import Dict
import requests
from pydantic import BaseModel
from datetime import datetime, timedelta
import base64
import hmac
import hashlib
from pathlib import Path
from datetime import datetime
from psycopg2 import DatabaseError
from fastapi import HTTPException
import httpx
import unicodedata
import re
import pandas as pd
import json
from sqlalchemy.orm.attributes import flag_modified

# from .data import orders


from context_manager.context import context_user_data, get_db_session

from logger import logger


# models
from models import Pickup_Location, Order

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

from modules.shipping_notifications.shipping_notifications_service import (
    ShippingNotificaitions,
)


creds = {}

# https://dashlifestyle.co/


SHOPIFY_API_VERSION = "2024-10"


class TempModel(BaseModel):
    client_id: int


def get_value_from_note_attributes(note_attributes, key):
    """
    Helper function to search for a specific key in note_attributes.
    Returns the value if found; otherwise, returns an empty string.
    """
    for attribute in note_attributes:
        if attribute["name"].lower() == key.lower():
            return attribute["value"]
    return ""


class Shopify:

    def determine_payment_type(order):
        payment_gateway = order.get("payment_gateway_names", [])
        cod_keywords = [
            "Cash on Delivery",
            "COD",
            "GoKwik Cash On Delivery",
        ]  # Add all possible variations of COD here

        for gateway in payment_gateway:
            if any(keyword in gateway for keyword in cod_keywords):
                return "COD"

        return "prepaid"

    @staticmethod
    async def adhoc_order():
        # data_file = Path("data.json")
        excel_file = Path("data/orders.xlsx")

        # Load JSON data
        # with open(data_file, "r", encoding="utf-8") as file:
        #     orders = json.load(file)

        # Load Excel data into DataFrame
        # df = pd.read_excel(excel_file, dtype=str)
        # Load Excel data into a list of dictionaries for faster lookup
        df = pd.read_excel(excel_file, dtype=str)
        billing_data_list = df.to_dict(orient="records")

        # print(billing_data_list)

        for order in orders:
            billing_address = order.get("billing_address", {})

            # If zip key is present, proceed
            if "zip" in billing_address:
                await Shopify.create_or_update_order(order, "99")
                continue

            # If zip is missing, try fetching from Excel using Order ID
            order_id = str(order.get("order_number"))
            matched_entry = next(
                (
                    entry
                    for entry in billing_data_list
                    if str(entry["Order ID"]) == order_id
                ),
                None,
            )

            # print(matched_entry)

            # print("yyoyo")
            # continue

            if matched_entry:
                order["billing_address"].update(
                    {
                        "first_name": matched_entry["Customer Name"],
                        "last_name": "",
                        "phone": matched_entry["Customer Mobile"],
                        "address1": matched_entry["Address Line 1"],
                        "address2": matched_entry["Address Line 2"],
                        "city": matched_entry["Address City"],
                        "province": matched_entry["Address State"],
                        "country": "India",
                        "zip": matched_entry["Address Pincode"],
                        "country_code": "",
                        "province_code": "",
                    }
                )

            # return order

            # Send the updated order to processing function
            await Shopify.create_or_update_order(order, "99")

    @staticmethod
    async def create_or_update_order(order_data, client_id, store_id=None):

        try:
            pickup_location_code = None

            if client_id == "1":
                pickup_location_code = "0002"

            if client_id == "5":
                pickup_location_code = "0005"

            if client_id == "13":
                pickup_location_code = "0018"

            if client_id == "2":
                pickup_location_code = "0029"

            # if client_id == "17":
            #     pickup_location_code = "0021"

            if client_id == "24":
                pickup_location_code = "0031"

            if client_id == "20":
                pickup_location_code = "0023"

            # if client_id == "52":
            #     pickup_location_code = "0064"

            if client_id == "67":
                pickup_location_code = "0083"

            if client_id == "72":
                pickup_location_code = "0099"

            if client_id == "79":
                pickup_location_code = "0090"

            if client_id == "89":
                pickup_location_code = "0106"

            if client_id == "68":
                pickup_location_code = "0107"

            if client_id == "44":
                pickup_location_code = "0112"

            if client_id == "94":
                pickup_location_code = "0789"

            if client_id == "97":
                pickup_location_code = "0270"

            if client_id == "90":
                pickup_location_code = "0239"

            if client_id == "99":
                pickup_location_code = "0275"

            if client_id == "101":
                pickup_location_code = "0280"

            if client_id == "100":
                pickup_location_code = "0279"

            if client_id == "102":
                pickup_location_code = "0282"

            if client_id == "82":
                pickup_location_code = "0292"

            if client_id == "109":
                pickup_location_code = "0305"

            if client_id == "106":
                pickup_location_code = "0307"

            if client_id == "113":
                pickup_location_code = "0319"

            if client_id == "116":
                pickup_location_code = "0323"

            if client_id == "105":
                pickup_location_code = "0330"

            if client_id == "121":
                pickup_location_code = "0341"

            if client_id == "122":
                pickup_location_code = "0342"

            if client_id == "123":
                pickup_location_code = "0343"

            if client_id == "132":
                pickup_location_code = "0359"

            # if client_id == "19":
            #     pickup_location_code = "0269"

            if client_id == "125":
                pickup_location_code = "0346"

            if client_id == "129":
                pickup_location_code = "0382"

            if client_id == "138":
                pickup_location_code = "0375"

            if client_id == "143":
                pickup_location_code = "0379"

            if client_id == "184":
                pickup_location_code = "0388"

            if client_id == "180":
                pickup_location_code = "0386"

            if client_id == "182":
                pickup_location_code = "0391"

            if client_id == "186":
                pickup_location_code = "0393"

            if client_id == "221":
                pickup_location_code = "0411"

            if client_id == "253":
                pickup_location_code = "0497"

            if client_id == "140":
                pickup_location_code = "0378"

            if client_id == "264":
                pickup_location_code = "0550"

            if client_id == "261":
                pickup_location_code = "0556"

            if client_id == "259":
                pickup_location_code = "0571"

            # if client_id == "276":
            #     pickup_location_code = "0596"

            if client_id == "274":
                pickup_location_code = "0679"

            if client_id == "392":
                pickup_location_code = "0759"

            if client_id == "398":
                pickup_location_code = "0881"

            if client_id == "401":
                pickup_location_code = "0811"

            if client_id == "402":
                pickup_location_code = "0859"

            if client_id == "407":
                pickup_location_code = "0860"

            if client_id == "413":
                pickup_location_code = "0909"

            if client_id == "424":
                pickup_location_code = "0961"

            if client_id == "423":
                pickup_location_code = "0969"

            if client_id == "313":
                pickup_location_code = "1008"

            count = 1

            # orders_reversed = orders[::-1]

            print(order_data)

            # Map Shopify payload to internal order fields
            mapped_order_data = Shopify.map_order_data(
                order_data, pickup_location_code, client_id
            )

            if store_id:
                mapped_order_data["store_id"] = store_id

            count = count + 1

            # Establish database session
            db = get_db_session()

            # Check fulfillment status from Shopify order
            fulfillment_status = order_data.get("fulfillment_status")

            # Check if order already exists
            existing_order = (
                db.query(Order)
                .filter(
                    Order.marketplace_order_id
                    == str(mapped_order_data["marketplace_order_id"]),
                    Order.client_id == mapped_order_data["client_id"],
                )
                .first()
            )

            # Pre-process fulfilled orders - add tag to mapped_order_data before create/update
            if fulfillment_status == "fulfilled":
                current_tags = mapped_order_data.get("order_tags", [])
                if "self_fulfilled" not in current_tags:
                    current_tags.append("self_fulfilled")
                    mapped_order_data["order_tags"] = current_tags

            # Handle fulfilled orders from Shopify
            if existing_order:
                # Skip if order exists and is already fulfilled (not new or cancelled)
                if fulfillment_status == "fulfilled" and existing_order.status not in [
                    "new",
                    "cancelled",
                ]:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        message="Order already fulfilled on both platforms, skipping.",
                        status=True,
                    )

                # Handle updates to existing orders
                response = Shopify.update_existing_order(
                    existing_order, mapped_order_data, db
                )
            else:
                # Handle creation of new orders (including fulfilled ones)
                response = Shopify.create_new_order(mapped_order_data, db)

                context_user_data.set(TempModel(**{"client_id": int(client_id)}))

                if response.status == True and (
                    client_id == "68" or client_id == "138"
                ):

                    order = (
                        db.query(Order)
                        .filter(
                            Order.client_id == int(client_id),
                            Order.order_id == mapped_order_data["order_id"],
                        )
                        .first()
                    )

                    if order.payment_mode == "COD":

                        ShippingNotificaitions.send_order_confirmation(order)

            # Post-process fulfilled orders - add action history for self-fulfillment
            if fulfillment_status == "fulfilled" and response.status:
                # Get the order that was just created or updated
                processed_order = (
                    existing_order
                    if existing_order
                    else (
                        db.query(Order)
                        .filter(
                            Order.client_id == mapped_order_data["client_id"],
                            Order.order_id == mapped_order_data["order_id"],
                        )
                        .first()
                    )
                )

                if processed_order:
                    # Ensure the order_tags include self_fulfilled
                    current_tags = processed_order.order_tags or []
                    if "self_fulfilled" not in current_tags:
                        current_tags.append("self_fulfilled")
                        processed_order.order_tags = current_tags
                        flag_modified(processed_order, "order_tags")

                    # Add action history entry for self-fulfillment
                    processed_order.action_history.append(
                        {
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "message": "Added self fulfilled tag - order was fulfilled on Shopify",
                        }
                    )
                    flag_modified(processed_order, "action_history")
                    db.add(processed_order)
                    db.commit()

            return GenericResponseModel(
                status_code=response.status_code,
                message=response.message,
                status=response.status,
            )

        except DatabaseError as e:
            # Log and return database error
            logger.error(f"Database error: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Database error occurred while processing the order.",
            )

        except ValueError as e:
            # Handle payload decoding issues
            logger.error(f"JSON decoding error: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=False,
                message=f"Invalid JSON payload: {str(e)}",
            )

        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Unexpected error: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message=f"An unexpected error occurred: {str(e)}",
            )

    @staticmethod
    def map_order_data(order_data, pickup_location_code, client_id):

        from modules.shipment.shipment_service import ShipmentService

        def clean_text(text):
            if text is None:
                return ""
            # Normalize Unicode and replace non-breaking spaces with normal spaces
            text = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()
            # Replace all special characters except comma and hyphen with a space
            text = re.sub(r"[^a-zA-Z0-9\s,-]", " ", text)
            # Replace multiple spaces with a single space
            return re.sub(r"\s+", " ", text).strip()

        length = 10
        breadth = 10
        height = 10
        weight = 0.5

        if client_id == "89":
            length = 12
            breadth = 8
            height = 8

        if client_id == "99":
            length = 18.3
            breadth = 10.4
            height = 8.4

        if client_id == "259":
            weight = 0.12

        if client_id == "402" or client_id == "424":
            length = 20
            breadth = 25
            height = 5
            weight = 0.5

        if order_data["cancelled_at"]:
            status = "cancelled"
        else:
            status = "new"

        order_id = None
        if client_id == "97" or client_id == "180":
            order_id = order_data["name"].replace("#", "")

        elif client_id == "140" or client_id == "413":
            order_id = order_data["name"]

        else:
            order_id = str(order_data["order_number"])

        """Maps Shopify order data to internal format."""
        # Get shipping and billing addresses
        shipping_address = order_data.get("shipping_address") or {}
        billing_address = order_data.get("billing_address") or {}

        # Helper function to get address field with fallback
        def get_address_field(field_name, default=""):
            return shipping_address.get(field_name) or billing_address.get(
                field_name, default
            )

        calc_consignee_phone = get_address_field("phone", "") or ""

        # Normalize only if it’s a string and starts with +91
        if isinstance(calc_consignee_phone, str) and calc_consignee_phone.startswith(
            "+91"
        ):
            calc_consignee_phone = calc_consignee_phone.removeprefix("+91")

        # If consignee phone is empty or None, fallback to customer phone
        if not calc_consignee_phone:
            customer_phone = order_data.get("customer", {}).get("phone", "") or ""
            if isinstance(customer_phone, str) and customer_phone.startswith("+91"):
                customer_phone = customer_phone.removeprefix("+91")
            calc_consignee_phone = customer_phone

        body = {
            "consignee_full_name": (
                f"{get_address_field('first_name')} {get_address_field('last_name')}".strip()
            ),
            "consignee_phone": calc_consignee_phone,
            "consignee_email": order_data.get("email", ""),
            "consignee_address": get_address_field("address1"),
            "consignee_landmark": get_address_field("address2"),
            "consignee_pincode": get_address_field("zip"),
            "consignee_city": get_address_field("city"),
            "consignee_country": "India",
            "consignee_state": get_address_field("province"),
            "pickup_location_code": pickup_location_code,
            "order_id": order_id,
            "order_date": order_data["created_at"],
            "channel": "shopify",
            "billing_is_same_as_consignee": True,
            "products": [
                {
                    "name": product["name"],
                    "quantity": product["quantity"],
                    "unit_price": product["price"],
                    "sku_code": product["sku"] or "",
                    "line_item_id": product["id"],
                }
                for product in order_data["line_items"]
            ],
            "payment_mode": (
                "prepaid" if float(order_data["total_outstanding"]) == 0 else "COD"
            ),
            "total_amount": order_data["total_price"],
            "order_value": sum(
                float(product["quantity"]) * float(product["price"])
                for product in order_data["line_items"]
            ),
            "client_id": client_id,
            "source": "shopify",
            "marketplace_order_id": order_data["id"],
            "status": status,
            "sub_status": status,
            # wights and dimensions
            "length": order_data.get("length", length),
            "breadth": order_data.get("breadth", breadth),
            "height": order_data.get("height", height),
            "company_id": 1,
            "order_type": "B2C",
            "discount": order_data.get("total_discounts", 0),
            "tax_amount": (
                0
                if order_data.get("taxes_included") == True
                else order_data.get("total_tax", 0)
            ),
            "shipping_charges": sum(
                float(item["price"]) for item in order_data["shipping_lines"]
            ),
        }

        if order_data.get("total_weight", 0) > 0:
            weight = order_data["total_weight"] / 1000  # Convert grams to kilograms
        else:
            # Sum the weights of line items if grams key is present
            line_item_weight = sum(
                item.get("grams", 0) for item in order_data.get("line_items", [])
            )
            # Default to 0.5 kg if all weights are 0
            weight = line_item_weight / 1000 if line_item_weight > 0 else 0.5

        body["weight"] = round(weight, 3)

        if client_id == "116":
            body["weight"] = 0.18

        volumetric_weight = round(
            (body["length"] * body["breadth"] * body["height"]) / 5000,
            3,
        )
        applicable_weight = round(max(body["weight"], volumetric_weight), 3)
        body["volumetric_weight"] = volumetric_weight
        body["applicable_weight"] = applicable_weight

        # calc product quantity
        body["product_quantity"] = sum(
            product["quantity"] for product in body["products"]
        )

        body["order_tags"] = (
            order_data.get("tags", "").split(",") if order_data.get("tags") else []
        )

        db = get_db_session()

        pickup_pincode: int = (
            db.query(Pickup_Location.pincode)
            .filter(Pickup_Location.location_code == pickup_location_code)
            .first()
        )[0]

        zone_data = ShipmentService.calculate_shipping_zone(
            pickup_pincode, body["consignee_pincode"]
        )

        body["zone"] = zone_data.data.get("zone", "D")

        return body

    @staticmethod
    def update_existing_order(existing_order, order_data, db):

        if existing_order.status not in ["new", "cancelled"]:
            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                message="Only new or canceled orders can be updated.",
            )

        if order_data.get("status") == "cancelled":

            existing_order.status = "cancelled"
            existing_order.sub_status = "cancelled"
            existing_order.action_history.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": "Order canceled on platform",
                }
            )

            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK, message="Order canceled successfully."
            )

        else:

            for key, value in order_data.items():
                setattr(existing_order, key, value)

            # Mark JSON fields as modified for SQLAlchemy change detection
            if "order_tags" in order_data:
                flag_modified(existing_order, "order_tags")
            if hasattr(existing_order, "action_history"):
                flag_modified(existing_order, "action_history")

            existing_order.action_history.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": "Order updated on platform",
                }
            )

            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK, message="Order updated successfully."
            )

    @staticmethod
    def create_new_order(order_data, db):

        if order_data.get("status") == "cancelled":
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK, message="cancelled order."
            )

        """Creates a new order record."""
        order_data["action_history"] = [
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "message": "Order created on platform",
            }
        ]

        new_order = Order.create_db_entity(order_data)
        db.add(new_order)
        db.commit()

        return GenericResponseModel(
            status_code=http.HTTPStatus.OK,
            status=True,
            message="Order created successfully.",
        )

    @staticmethod
    async def update_shopify_order_id(orders):
        client_id = 5

        db = get_db_session()

        for order in orders:

            order_id = str(order["order_number"])

            existing_order = (
                db.query(Order)
                .filter(Order.client_id == client_id, Order.order_id == order_id)
                .first()
            )

            if existing_order is None:
                continue

            existing_order.source = "shopify"
            existing_order.marketplace_order_id = str(order["id"])

            db.add(existing_order)
            db.flush()

        db.commit()

        return "Hello"

    @staticmethod
    async def get_webhooks():
        # 1346004517081
        url = f"https://{STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/webhooks/1346004517081.json"
        headers = {"X-Shopify-Access-Token": ACCESS_TOKEN}
        response = requests.delete(url, headers=headers)

        if response.status_code == 200:
            webhooks = response.json()
            return webhooks  # This will return the list of active webhooks
        else:
            return {
                "error": f"Failed to retrieve webhooks. Status code: {response.status_code}"
            }

    @staticmethod
    async def create_webhook():
        url = f"https://{STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/webhooks/1352374255768.json"

        # Define the webhook data
        # payload = {
        #     "webhook": {
        #         "topic": "orders/create",
        #         "address": "https://api.lastmiles.co/api/v1/shopify/order-create?client_id=13",
        #         "format": "json",
        #     }
        # }

        payload = {
            "webhook": {
                "address": "https://api.lastmiles.co/api/v1/shopify/order-create?client_id=423",
            }
        }

        headers = {
            "X-Shopify-Access-Token": ACCESS_TOKEN,
            "Content-Type": "application/json",
        }

        response = requests.put(url, json=payload, headers=headers)

        if response.status_code == 201:
            return {"message": "Webhook created successfully", "data": response.json()}
        else:
            return {
                "error": f"Failed to create webhook. Status code: {response.status_code}",
                "details": response.text,
            }

    @staticmethod
    async def update_order(order_id, client_id, store_id):

        credentials = creds[client_id][store_id]
        base_url = f"https://{credentials['STORE_DOMAIN']}/admin/api/{SHOPIFY_API_VERSION}/orders/{order_id}.json"
        headers = {
            "X-Shopify-Access-Token": credentials["ACCESS_TOKEN"],
            "Content-Type": "application/json",
        }

        print(order_id)

        # Fetch the existing order details
        response = requests.get(base_url, headers=headers)

        print(response.json())

        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch order: {response.status_code} - {response.text}"
            )

        order_data = response.json().get("order", {})
        if not order_data:
            raise Exception("Order not found or empty response.")

        # Modify the address slightly (e.g., adding an extra space)

        # Prepare payload
        payload = {
            "order": {
                "id": order_id,
                "note": "order update syncing",
            }
        }

        # Send update request
        update_response = requests.put(base_url, headers=headers, json=payload)
        if update_response.status_code != 200:
            raise Exception(
                f"Failed to update order: {update_response.status_code} - {update_response.text}"
            )

        return update_response.json()

    @staticmethod
    async def get_open_orders():

        client_id = 313
        store_id = 1

        credentials = creds[client_id][store_id]

        # Calculate the date for 7 days ago in ISO 8601 format
        seven_days_ago = (datetime.utcnow() - timedelta(days=14)).isoformat()
        # max_date = (datetime.utcnow() - timedelta(days=30)).isoformat()

        base_url = f"https://{credentials['STORE_DOMAIN']}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
        headers = {
            "X-Shopify-Access-Token": credentials["ACCESS_TOKEN"],
            "Content-Type": "application/json",
        }

        print(base_url)

        # Initial request parameters
        params = {
            "limit": 250,  # Maximum allowed value for 'limit'
            "fulfillment_status": "unfulfilled",  # Fetch only unfulfilled orders
            # "status": "cancelled",
            "created_at_min": seven_days_ago,  # Fetch orders created in the last 7 days
            # "created_at_max": max_date,
        }

        all_orders = []
        next_page_info = None

        while True:
            if next_page_info:
                # For subsequent requests, use only the `page_info` parameter
                params = {
                    "limit": 250,
                    "page_info": next_page_info,
                }

            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code != 200:
                raise Exception(
                    f"Failed to fetch orders: {response.status_code} - {response.text}"
                )

            data = response.json()

            new_data = data.get("orders", [])

            for order in new_data:
                if order["order_number"] == 160179:
                    return order

            all_orders.extend(new_data)

            # Parse 'Link' header for pagination
            link_header = response.headers.get("Link")
            if link_header:
                next_page_info = None
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        next_page_info = part.split("page_info=")[1].split(">")[0]
                        break
                if not next_page_info:
                    break  # No more pages
            else:
                break  # No pagination info, exit loop

        for order in all_orders:
            await Shopify.update_order(
                order_id=order["id"], client_id=client_id, store_id=store_id
            )

        # count = 1
        # for order in all_orders:
        #     print(f"Processing order {count} of {len(all_orders)}")
        #     count += 1
        #     await Shopify.create_or_update_order(
        #         order_data=order, client_id=str(client_id)
        #     )

        return all_orders

    @staticmethod
    async def update_all_orders_fullfillment():

        url = f"https://{STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/orders.json"

        # You can adjust the financial and fulfillment statuses as per your needs
        params = {
            "status": "open",  # Only fetch orders that are open
            "fulfillment_status": "unfulfilled",
            "limit": 250,  # Only fetch orders that are not yet fulfilled
        }

        headers = {
            "X-Shopify-Access-Token": ACCESS_TOKEN,
            "Content-Type": "application/json",
        }

        response = requests.get(url, headers=headers, params=params)

        db = get_db_session()

        if response.status_code == 200:

            response = response.json()

            orders = response.get("orders", [])

            for order in orders:
                order_id = str(order["order_number"])

                existing_order = (
                    db.query(Order)
                    .filter(Order.client_id == 5, Order.order_id == order_id)
                    .first()
                )

                if existing_order is None:
                    continue

                if (
                    existing_order.status == "new"
                    or existing_order.status == "cancelled"
                ):
                    continue

                Shopify.update_order_fulfillment_status(
                    order_id=order["id"], awb_number=existing_order.awb_number
                )

            return "successfull"

        else:
            return {
                "error": f"Failed to retrieve orders. Status code: {response.status_code}",
                "details": response.text,
            }

    @staticmethod
    def update_order_fulfillment_status(order_id, awb_number, store_id=None):

        try:

            client_id = context_user_data.get().client_id

            if order_id is None:
                return False

            print(order_id)

            client_creds = creds.get(client_id)
            credentials = client_creds.get(1)

            if store_id:
                credentials = client_creds.get(store_id)

            if credentials is None:
                return False

            headers = {
                "X-Shopify-Access-Token": credentials["ACCESS_TOKEN"],
                "Content-Type": "application/json",
            }

            fulfilment_order_id = Shopify.get_fullfilment_order_id(
                order_id=order_id, credentials=credentials
            )

            if fulfilment_order_id == False:
                return False

            url = f"https://{credentials['STORE_DOMAIN']}/admin/api/{SHOPIFY_API_VERSION}/fulfillments.json"

            body = {
                "fulfillment": {
                    "line_items_by_fulfillment_order": [
                        {"fulfillment_order_id": fulfilment_order_id}
                    ],
                    "notify_customer": True,
                    "tracking_info": {
                        "number": awb_number,  # Track the tracking number
                        "url": "https://app.lastmiles.co/tracking/awb/"
                        + awb_number,  # Track the tracking URL
                        "company": "Warehousity",
                    },
                }
            }

            response = requests.post(url, headers=headers, json=body)

            print(response.json())

            if response.status_code == 201:
                return True
            else:
                False
        except Exception as e:
            print("Error in update_order_fulfillment_status:", str(e))
            return False

    @staticmethod
    def get_fullfilment_order_id(order_id, credentials):

        if order_id is None:
            return False

        url = f"https://{credentials['STORE_DOMAIN']}/admin/api/{SHOPIFY_API_VERSION}/orders/{order_id}/fulfillment_orders.json"

        headers = {
            "X-Shopify-Access-Token": credentials["ACCESS_TOKEN"],
            "Content-Type": "application/json",
        }

        response = requests.get(url, headers=headers)

        print(response)

        if response.status_code == 200:

            print("in")

            orders = response.json()

            fulfillments = orders.get("fulfillment_orders", [])

            print("fulfillment_id", fulfillments)

            if not fulfillments:
                return False

            first_fulfillment = fulfillments[0]
            fulfillment_id = first_fulfillment.get("id", None)

            return fulfillment_id if fulfillment_id else False
        else:
            False

    @staticmethod
    async def get_locations():
        url = f"https://{STORE_DOMAIN}/admin/api/2023-10/locations.json"

        headers = {
            "X-Shopify-Access-Token": ACCESS_TOKEN,
            "Content-Type": "application/json",
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return {
                "message": "Locations Fetched Successfully",
                "data": response.json(),
            }
        else:
            return {
                "error": f"Failed to retrieve locations. Status code: {response.status_code}",
                "details": response.text,
            }

    @staticmethod
    async def get_all_products(client_id: int = 413, store_id: int = 1):
        """
        Fetch all products from a Shopify store with pagination support.

        Args:
            client_id: The client ID from the creds dictionary
            store_id: The store ID for the client (default: 1)

        Returns:
            dict: Dictionary containing success status, message, and product data
        """
        try:
            # Get credentials for the specified client and store
            if client_id not in creds or store_id not in creds[client_id]:
                return {
                    "success": False,
                    "error": f"Invalid client_id ({client_id}) or store_id ({store_id})",
                    "data": [],
                }

            credentials = creds[client_id][store_id]

            base_url = f"https://{credentials['STORE_DOMAIN']}/admin/api/{SHOPIFY_API_VERSION}/products.json"
            headers = {
                "X-Shopify-Access-Token": credentials["ACCESS_TOKEN"],
                "Content-Type": "application/json",
            }

            all_products = []
            params = {"limit": 250}  # Maximum allowed limit
            next_page_info = None

            # Paginate through all products
            while True:
                if next_page_info:
                    params = {
                        "limit": 250,
                        "page_info": next_page_info,
                    }

                response = requests.get(base_url, headers=headers, params=params)

                if response.status_code == 200:
                    data = response.json()
                    products = data.get("products", [])

                    if not products:
                        break

                    all_products.extend(products)

                    # Check for pagination link in headers
                    link_header = response.headers.get("Link", "")
                    if 'rel="next"' in link_header:
                        # Extract page_info from the Link header
                        for link in link_header.split(","):
                            if 'rel="next"' in link:
                                next_page_info = link.split("page_info=")[1].split(">")[
                                    0
                                ]
                                break
                        else:
                            next_page_info = None
                    else:
                        next_page_info = None

                    # If no next page, break the loop
                    if not next_page_info:
                        break
                else:
                    return {
                        "success": False,
                        "error": f"Failed to retrieve products. Status code: {response.status_code}",
                        "details": response.text,
                        "data": all_products,  # Return whatever we've collected so far
                    }

            return {
                "success": True,
                "message": f"Successfully fetched {len(all_products)} products",
                "count": len(all_products),
                "data": all_products,
            }

        except Exception as e:
            logger.error(f"Error fetching products for client {client_id}: {str(e)}")
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}",
                "data": [],
            }
