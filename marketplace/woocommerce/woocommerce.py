import http
from http import HTTPStatus
from psycopg2 import DatabaseError
import math
from typing import Dict
import requests
from datetime import datetime
import base64
import hmac
import hashlib
from datetime import datetime, timedelta
from psycopg2 import DatabaseError
from fastapi import HTTPException, Request

from woocommerce import API


from context_manager.context import context_user_data, get_db_session

from logger import logger

# service
from modules.shipment import ShipmentService

# models
from models import Pickup_Location, Order

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel


ERROR_MSG_WEBHOOK = "Webhook not created your details or contact with lastMiles"
ERROR_MSG_WEBHOOK_INSERTION = "Something wrong"
SUCCESS_MSG_WEBHOOK = "webhook created successfully"


class Woocommerce:

    from datetime import datetime, timedelta


from http import HTTPStatus
from sqlalchemy.exc import DatabaseError


class Woocommerce:
    @staticmethod
    def create_order(request: dict, com_id: str):
        try:
            db = get_db_session()
            status = request["status"]
            pickup_location_code, client_id = Woocommerce.get_client_details(com_id)

            if not pickup_location_code or not client_id:
                return GenericResponseModel(
                    status_code=HTTPStatus.BAD_REQUEST, message="Invalid Company ID"
                )

            order_data = Woocommerce.construct_order_data(
                request, pickup_location_code, client_id
            )
            order_data = Woocommerce.calculate_order_metrics(order_data)
            order_data["order_date"] = Woocommerce.convert_to_utc(
                order_data["order_date"]
            )

            pickup_pincode = Woocommerce.get_pickup_pincode(
                db, order_data["pickup_location_code"]
            )
            if pickup_pincode is None:
                return GenericResponseModel(
                    status_code=HTTPStatus.BAD_REQUEST,
                    message="Invalid Pickup Location",
                )

            zone_data = ShipmentService.calculate_shipping_zone(
                pickup_pincode, order_data["consignee_pincode"]
            )

            order_data["zone"] = zone_data.data.get("zone", "D")

            existing_order = (
                db.query(Order)
                .filter(
                    Order.order_id == order_data["order_id"],
                    Order.client_id == client_id,
                )
                .first()
            )

            if existing_order:
                return Woocommerce.update_existing_order(
                    db, existing_order, order_data, status
                )
            else:
                return Woocommerce.create_new_order(db, order_data, status)

        except DatabaseError as e:
            logger.error(f"Database Error: {e}")
            return GenericResponseModel(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An unexpected error occurred. Please try again later.",
            )
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            return GenericResponseModel(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An unexpected error occurred. Please try again later.",
            )
        finally:
            if db:
                db.close()

    @staticmethod
    def get_client_details(com_id: str):
        client_map = {
            "dev": ("0026", 2),
            "clickmart": ("0082", 56),
            "thukral_foods": ("0101", 81),
            "nakodagheetel": ("0609", 277),
        }
        return client_map.get(com_id, (None, None))

    @staticmethod
    def construct_order_data(request: dict, pickup_location_code: str, client_id: int):
        order_data = {
            "consignee_full_name": f"{request.get('billing', {}).get('first_name', '')} {request.get('billing', {}).get('last_name', '')}",
            "consignee_phone": request.get("billing", {}).get("phone", ""),
            "consignee_email": request.get("billing", {}).get("email", ""),
            "consignee_address": f"{request.get('billing', {}).get('address_1', '')} {request.get('billing', {}).get('address_2', '')}",
            "consignee_pincode": request.get("billing", {}).get("postcode", ""),
            "consignee_city": request.get("billing", {}).get("city", ""),
            "consignee_state": request.get("billing", {}).get("state", ""),
            "consignee_country": request.get("billing", {}).get("country", ""),
            "pickup_location_code": pickup_location_code,
            "order_id": str(request.get("id", "")),
            "order_date": request.get("date_created", "").replace("T", " "),
            "channel": "woocommerce",
            "products": [
                {
                    "name": p.get("name", ""),
                    "quantity": p.get("quantity", 0),
                    "unit_price": p.get("price", 0.0),
                    "sku_code": p.get("sku", ""),
                }
                for p in request.get("line_items", [])
            ],
            "payment_mode": (
                "COD" if request.get("payment_method", "") == "cod" else "prepaid"
            ),
            "total_amount": (
                float(request.get("total", 0)) + float(request.get("total_tax", 0))
                if not request.get("prices_include_tax", False)
                else request.get("total", 0)
            ),
            "discount": (
                float(request.get("discount_total", 0))
                + float(request.get("discount_tax", 0))
                if not request.get("prices_include_tax", False)
                else request.get("discount_total", 0)
            ),
            "tax_amount": request.get("total_tax", 0),
            "shipping_charges": (
                float(request.get("shipping_total", 0))
                + float(request.get("shipping_tax", 0))
                if not request.get("prices_include_tax", False)
                else request.get("shipping_total", 0)
            ),
            "billing_is_same_as_consignee": True,
            "weight": 0.5,
            "length": 10,
            "breadth": 10,
            "height": 10,
            "client_id": client_id,
            "company_id": 1,
            "status": "new",
            "sub_status": "new",
            "gift_wrap_charges": 0,
            "other_charges": 0,
            "eway_bill_number": "",
            "cod_charges": 0,
            "request": request,
            "action_history": [
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "message": "Order Created on Platform",
                    "user_data": "",
                }
            ],
            "order_type": "B2C",
        }
        return order_data

    @staticmethod
    def calculate_order_metrics(order_data: dict):
        order_data["order_value"] = order_data["order_value"] = (
            math.ceil(
                sum(
                    float(p.get("unit_price", 0)) * int(p.get("quantity", 0))
                    for p in order_data["products"]
                )
                * 1000  # Shift decimal places
            )
            / 1000
        )
        order_data["product_quantity"] = sum(
            p["quantity"] for p in order_data["products"]
        )
        volumetric_weight = round((10 * 10 * 10) / 5000, 3)
        order_data["volumetric_weight"] = volumetric_weight
        order_data["applicable_weight"] = round(max(0.5, volumetric_weight), 3)
        return order_data

    @staticmethod
    def convert_to_utc(order_date: str):
        ist_time = datetime.strptime(order_date, "%Y-%m-%d %H:%M:%S")
        return ist_time - timedelta(hours=5, minutes=30)

    @staticmethod
    def get_pickup_pincode(db, pickup_location_code):
        return (
            db.query(Pickup_Location.pincode)
            .filter(Pickup_Location.location_code == pickup_location_code)
            .scalar()
        )

    @staticmethod
    def update_existing_order(db, existing_order, order_data, status):
        if existing_order.status != "new":
            return {"code": 200, "message": "Order Already Processed"}

        for key, value in order_data.items():
            setattr(existing_order, key, value)

        if status in ["checkout-draft", "completed", "failed"]:
            return {"code": 200, "message": "No Updates Required"}

        existing_order.status = (
            "cancelled" if status in ["cancelled", "failed"] else "new"
        )
        existing_order.sub_status = existing_order.status
        existing_order.action_history.append(
            {
                "date": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "event": "Order Updated on Platform",
            }
        )
        db.commit()
        return GenericResponseModel(
            status_code=200, status=True, message="Order Updated"
        )

    @staticmethod
    def create_new_order(db, order_data, status):
        if status in ["checkout-draft", "completed", "failed"]:
            return {"code": 200, "message": "No Action Taken"}

        print("hellodpkdkfvkdfmvldkfvd")
        print(order_data)

        order_instance = Order.create_db_entity(order_data)
        Order.create_new_order(order_instance)
        db.commit()
        return GenericResponseModel(
            status_code=200, status=True, message="Order created"
        )

    @classmethod
    async def add_marketplace(this):
        try:
            with get_db_session() as db:

                try:

                    # wcapi = API(
                    #     url="https://www.fastwinsbag.com/",
                    #     consumer_key="ck_a44951196fe60caa548dae8323b4cd85a23bf205",
                    #     consumer_secret="cs_864fa86e3ee49bc4e9cfce0382a60e75dfa3a3ac",
                    #     timeout=15,
                    # )

                    wcapi = API(
                        url="https://nakodagheetel.com/",
                        consumer_key="ck_ffa8e4a8e0ef15d6a5dd0029f87338204fa41a65",
                        consumer_secret="cs_b7d9b3da91e48cad9f08abcb4de20232770540e2",
                        timeout=15,
                    )

                    # wcapi = API(
                    #     url="https://exqhome.in",
                    #     consumer_key="ck_bd2ea105286c6f31af9a60119bc2cbf8e031e829",
                    #     consumer_secret="cs_d5e1733b0789ec2724bfa5b8b1607f41a48ce1e0",
                    #     timeout=15,
                    # )

                    # wcapi = API(
                    #     url="https://thukralfoods.com",
                    #     consumer_key="ck_f670b2779ddc05847ce3c077867792b0c2f6213b",
                    #     consumer_secret="cs_5ad269fb68d3f3a42294984d6a2c7e9c738aeb6e",
                    #     timeout=15,
                    # )

                    company_id = "nakodagheetel"

                    # logger.info("webhook connection open successfuly")

                    data = {
                        "name": "Last Miles Order create",
                        "topic": "order.update",
                        "delivery_url": "https://api.lastmiles.co/api/v1/order/webhook?company_id="
                        + company_id,
                    }

                    # data = wcapi.get("orders")

                    # data = data.json()

                    # return {
                    #     "status_code": http.HTTPStatus.OK,
                    #     "message": data,
                    # }

                    # print(wcapi.get("").json())
                    if wcapi != None:
                        webhookRes = wcapi.post("webhooks", data).json()

                        print(webhookRes)

                    if webhookRes.get("code") or webhookRes.get("data"):
                        raise HTTPException(
                            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                            detail=ERROR_MSG_WEBHOOK_INSERTION,
                        )

                    logger.info("webhook created successfuly")
                    if webhookRes != None:
                        newOrder = {
                            "company_id": company_id,
                            "config_data": [webhookRes],
                        }

                    return {
                        "status_code": http.HTTPStatus.OK,
                        "data": webhookRes,
                        "message": SUCCESS_MSG_WEBHOOK,
                    }

                except Exception as e:
                    logger.error(f"An error occurred: {e}")
                    raise HTTPException(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        detail="ERROR_MSG_WEBHOOK_INSERTION",
                    )
        except DatabaseError as e:
            logger.error(
                msg="Error retrieving marketplace: {}".format(str(e)),
            )
            raise HTTPException(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                detail=ERROR_MSG_WEBHOOK_INSERTION,
            )

    @classmethod
    async def remove_webhook(this, request: Request):
        try:
            with get_db_session() as db:
                try:
                    # wcapi = API(
                    #     url="https://www.myshopin.in/",
                    #     consumer_key="ck_19711d85364a921163d70ed2c678395c17437f3a",
                    #     consumer_secret="cs_5142ed7ed1f306f6755fd6100a2ec74413b24ddd",
                    #     timeout=15,
                    # )

                    # wcapi = API(
                    #     url="https://exqhome.in",
                    #     consumer_key="ck_bd2ea105286c6f31af9a60119bc2cbf8e031e829",
                    #     consumer_secret="cs_d5e1733b0789ec2724bfa5b8b1607f41a48ce1e0",
                    #     timeout=15,
                    # )

                    wcapi = API(
                        url="https://vastraas.com",
                        consumer_key="ck_371fcbe9f4450a9d8ab02378bb91521d5e1b6ea3",
                        consumer_secret="cs_1395fd74deac0b35571febdbead0f7c71f17e8e3",
                        timeout=15,
                    )

                    company_id = "vastras"

                    logger.info("connection open successfuly")

                    # webhookRes = wcapi.get("orders")

                    # return webhookRes.json()

                    # if webhookRes.get("code") or webhookRes.get("data"):
                    #     raise HTTPException(
                    #         status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    #         detail=ERROR_MSG_WEBHOOK_INSERTION,
                    #     )

                    # logger.info("webhook created successfuly")
                    # if webhookRes != None:
                    #     newOrder = {
                    #         "company_id": company_id,
                    #         "config_data": [webhookRes],
                    #     }

                    #     mergedata = {**newOrder, **formatted_order}

                    #     try:
                    #         final = Marketplace_model(**mergedata)
                    #     except Exception as e:
                    #         logger.error(
                    #             f"insert query not working: {e}. Array contents: {str(mergedata)}"
                    #         )
                    #         raise HTTPException(
                    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    #             detail=ERROR_MSG_WEBHOOK_INSERTION,
                    #         )

                    #     db.add(final)
                    #     db.commit()

                    # return {
                    #     "status_code": http.HTTPStatus.OK,
                    #     "message": SUCCESS_MSG_WEBHOOK,
                    # }

                except Exception as e:
                    logger.error(f"An error occurred: {e}")
                    raise HTTPException(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        detail="ERROR_MSG_WEBHOOK_INSERTION",
                    )
        except DatabaseError as e:
            logger.error(
                msg="Error retrieving marketplace: {}".format(str(e)),
            )
            raise HTTPException(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                detail=ERROR_MSG_WEBHOOK_INSERTION,
            )
