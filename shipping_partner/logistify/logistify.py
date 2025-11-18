import http
from psycopg2 import DatabaseError
from typing import Dict
from pydantic import BaseModel
import requests
from datetime import datetime
import base64
import pytz

from context_manager.context import context_user_data, get_db_session

from logger import logger

# models
from models import Pickup_Location, Order

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel

# data
from .status_mapping import status_mapping
from .delivery_partner_mapping import courier_mapping

# service
from modules.wallet.wallet_service import WalletService


class TempModel(BaseModel):
    client_id: int


class Logistify:

    @staticmethod
    def create_pickup_location(pickup_location_id: int, credentials: Dict[str, str]):

        print("inside the terror")

        try:

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == pickup_location_id)
                .first()
            )

            api_url = "https://api.logistify.in/connect/create-pickup-point"

            client_id = context_user_data.get().client_id

            body = {
                "merchant_id": credentials["merchant_id"],
                "contact_name": pickup_location.contact_person_name,
                "pickup_address_1": pickup_location.address,
                "pickup_address_2": pickup_location.landmark,
                "pickup_pincode": pickup_location.pincode,
                "pickup_city_name": pickup_location.city,
                "pickup_state_id": pickup_location.state,
                "pickup_phone": pickup_location.contact_person_phone,
                "pickup_details_for_label": pickup_location.address,
                "return_address_1": pickup_location.address,
                "return_address_2": pickup_location.landmark,
                "return_pincode": pickup_location.pincode,
                "return_city_name": pickup_location.city,
                "return_phone": pickup_location.contact_person_phone,
                "return_details_for_label": pickup_location.address,
                "return_state_id": pickup_location.state,
                "address_label_bottom_line": "",
            }

            token_string = credentials["email"] + ":" + credentials["password"]
            text_bytes = token_string.encode("utf-8")
            token = base64.b64encode(text_bytes).decode("utf-8")

            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
            }

            response = requests.post(api_url, json=body, headers=headers, verify=False)

            response_data = response.json()

            # if location creation failed
            if response_data["error"] == True:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

            # if successfully created location

            if response_data["error"] == False:
                logistify_location_id = response_data["data"]

                pickup_location.courier_location_codes = {
                    **pickup_location.courier_location_codes,
                    "logistify": logistify_location_id,
                }

                db.add(pickup_location)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=logistify_location_id,
                    message="Location created successfully",
                )

            # in case of any other issue
            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating location at shiperfecto: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Error occurred while creating location",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):

        try:

            client_id = context_user_data.get().client_id

            # get the location code for shiperfecto from the db

            db = get_db_session()

            courier_location_codes = (
                db.query(Pickup_Location.courier_location_codes)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )[0]

            # get the logistify location code from the db
            logistify_pickup_location = courier_location_codes.get("logistify", None)

            # if no logistify location code mapping is found for the current pickup location, create a new warehouse at logistify
            if logistify_pickup_location is None:

                logistify_pickup_location = Logistify.create_pickup_location(
                    order.pickup_location_code, credentials
                )

                # if could not create location at shiperfecto, throw error
                if logistify_pickup_location.status == False:

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Unable to place order. Please try again later",
                    )

                else:
                    logistify_pickup_location = logistify_pickup_location.data

            if delivery_partner.aggregator_slug == 66:

                print("inside the condition")
                # Combine address and landmark
                combined_address = (
                    order.consignee_address.strip()
                    + " "
                    + order.consignee_landmark.strip()
                ).strip()
                # Split into two parts
                consignee_address1 = combined_address[:40]
                consignee_landmark = combined_address[40:]

                print(consignee_address1)
                print(consignee_landmark)
            else:
                consignee_address1 = order.consignee_address.strip()
                consignee_landmark = order.consignee_landmark.strip()

            body = {
                "multiple_product": [
                    {
                        "name": product["name"],
                        "qty": product["quantity"],
                        "price": product["unit_price"],
                        "sku": product["sku_code"],
                    }
                    for product in order.products
                ],
                "product_code": order.products[0]["sku_code"],
                "product_name": order.products[0]["name"],
                "product_qty": order.products[0]["quantity"],
                "order_no": "OR/LM/" + str(client_id) + "/" + order.order_id,
                "service_id": "",
                "service_name": (
                    "Surface" if delivery_partner.mode == "surface" else "Express"
                ),
                "courier_id": int(delivery_partner.aggregator_slug),
                "movement_type": "Forward",
                "is_reverse_pickup": "0",
                "invoice_value": float(order.total_amount),
                "cod_value": float(
                    0 if order.payment_mode.lower() == "prepaid" else order.total_amount
                ),
                "weight_in_kgs": float(order.weight),
                "length_in_cms": float(order.length) if order.length > 1 else 1,
                "breadth_in_cms": float(order.breadth) if order.breadth > 1 else 1,
                "height_in_cms": float(order.height) if order.height > 1 else 1,
                "delivery_type": (
                    "Prepaid" if order.payment_mode.lower() == "prepaid" else "COD"
                ),
                "order_date": str(order.order_date),
                "customer_name": order.consignee_full_name,
                "customer_pincode": order.consignee_pincode,
                "customer_address_1": consignee_address1,
                "customer_address_2": consignee_landmark,
                "customer_city": order.consignee_city,
                "customer_state": order.consignee_state,
                "customer_mobile_no": order.consignee_phone.removeprefix("+91"),
                "customer_alt_no": order.consignee_alternate_phone,
                "pickup_location_id": int(logistify_pickup_location),
                "is_merchant_api": 1,
            }

            print(body)

            headers = {
                "Authorization": "Bearer " + credentials["api_key"],
                "Content-Type": "application/json",
                "User-Agent": "LastMiles/1.0",
            }

            api_url = "https://api.logistify.in/connect/order-ship"

            print(headers)

            response = requests.post(
                api_url, json=body, headers=headers, verify=True, timeout=10
            )

            print(response.text)

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # If order creation failed at Logistify, return message
            if response_data["error"] == True:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["msg"][0],
                )

            # if order created successfully at Logistify
            if response_data["error"] == False:

                order.status == "new"
                order.sub_status = "processing"

                db.add(order)
                db.flush()
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"processing": True},
                    message="Order Under Processing",
                )

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.aggregator = "logistify"

                # update the activity

                db.add(order)
                db.flush()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="AWB assigned successfully",
                )

            # else:
            #     return GenericResponseModel(
            #         status_code=http.HTTPStatus.BAD_REQUEST,
            #         message=response_data["order_data"]["error"],
            #     )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error posting shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while posting the shipment.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    def tracking_webhook(track_req):

        try:

            print(track_req)

            db = get_db_session()

            print(1)

            order_id = track_req.get("order_no", "")
            parts = order_id.split("/")

            awb_number = track_req.get("awb_no", None)

            client_id = parts[2] if len(parts) > 2 else None
            order_id = parts[3] if len(parts) > 3 else None

            if client_id == None or order_id == None:
                return {"msg": "Invalid Order ID"}

            context_user_data.set(TempModel(**{"client_id": client_id}))

            print("client", "order")
            print(client_id, order_id)

            order = (
                db.query(Order)
                .filter(Order.order_id == order_id, Order.client_id == client_id)
                .first()
            )

            if order is None:
                return

            print(2)

            if (
                track_req.get("status_id", None) != 3
                and track_req.get("status_id", None) != 25
            ):

                courier_status = int(track_req.get("status_id", None))

                current_order_status = order.status

                print(courier_status)

                if courier_status == None:
                    return

                if courier_status in status_mapping:
                    order.status = status_mapping[courier_status].get(
                        "status", order.status
                    )
                    order.sub_status = status_mapping[courier_status].get(
                        "sub_status", order.sub_status
                    )
                else:
                    # Handle the case where courier_status is not in status_mapping
                    # For example, log an error or set a default value
                    print(
                        f"Warning: courier_status '{courier_status}' not found in status_mapping."
                    )
                    return

                order.courier_status = courier_status

                order.awb_number = awb_number

                if current_order_status == "new":
                    order.aggregator = "logistify"

                    delivery_partner = str(track_req.get("courier_id"))
                    delivery_partner = courier_mapping[delivery_partner]

                    print("delivery_partner")
                    print(delivery_partner)

                    order.courier_partner = delivery_partner

                    new_activity = {
                        "event": "Shipment Created",
                        "subinfo": "delivery partner - " + delivery_partner,
                        "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    }

                    print(4)

                    # update the activity

                    order.action_history.append(new_activity)

                    total_freight = (
                        order.forward_freight
                        + order.forward_cod_charge
                        + order.forward_tax
                    )

                    # if the payment of the order is COD, add the cod amount to the provisional COD in wallet
                    if order.payment_mode == "COD":
                        WalletService.add_provisional_cod(order.total_amount)

                    WalletService.deduct_money(total_freight, awb_number=awb_number)

                db.add(order)

                db.flush()
                db.commit()

            elif track_req.get("status_id", None) == 3:

                # store the freight data in db
                order.forward_freight = None
                order.forward_cod_charge = None
                order.forward_tax = None

                order.status = "new"
                order.sub_status = "Failed"
                order.courier_status = "3"

                db.add(order)

                db.flush()
                db.commit()

            elif track_req.get("status_id", None) == 25:

                order.status = "new"
                order.sub_status = "processing"
                order.courier_status = "25"

                db.add(order)

                db.flush()
                db.commit()

            else:
                None

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Tracking successfull",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def track_shipment(order: Order_Model, awb_number: str):

        try:

            text = "crm@warehousity.com:crm@123#"
            text_bytes = text.encode("utf-8")
            base64_bytes = base64.b64encode(text_bytes)
            base64_text = base64_bytes.decode("utf-8")

            headers = {
                "Authorization": "Bearer " + base64_text,
                "Content-Type": "application/json",
                "User-Agent": "LastMiles/1.0",
            }

            api_url = "https://api.logistify.in/connect/order-tracking"

            print(headers)

            body = {"awb_nos": awb_number}

            response = requests.post(
                api_url, json=body, headers=headers, verify=True, timeout=10
            )

            # print(response.text)

            # If tracking failed at Logistify, return message
            try:
                response_data = response.json()
                # print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            if response_data["error"] == True:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message=response_data["msg"],
                )

            tracking_data = response_data.get("data", None)

            # if tracking_data is not present in the respnse
            if not tracking_data:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            courier_status = tracking_data[0].get("order_status_id", "")
            updated_awb_number = tracking_data[0].get("awb_no", "")

            activites = tracking_data[0].get("tracking", "")

            db = get_db_session()

            old_status = order.status
            old_sub_status = order.sub_status

            # update the order status, and awb if different
            order.status = (
                status_mapping[courier_status].get("status", None) or old_status
            )
            order.sub_status = (
                status_mapping[courier_status].get("sub_status", None) or old_sub_status
            )
            order.courier_status = courier_status

            order.awb_number = (
                updated_awb_number if updated_awb_number else order.awb_number
            )

            # update the tracking info
            if activites:
                new_tracking_info = [
                    {
                        "status": status_mapping[activity.get("status_id", "")].get(
                            "sub_status", ""
                        )
                        or "",
                        "description": activity.get("courier_msg", ""),
                        "subinfo": "",
                        "datetime": (
                            datetime.strptime(
                                activity.get("courier_event_date_time"),
                                "%Y-%m-%dT%H:%M:%S.%fZ",
                            )
                            .replace(tzinfo=pytz.utc)
                            .astimezone(pytz.timezone("Asia/Kolkata"))
                            .strftime("%d-%m-%Y %H:%M:%S")
                            if activity.get("courier_event_date_time")
                            else ""
                        ),
                        "location": activity.get("current_location", ""),
                    }
                    for activity in activites
                ]

                new_tracking_info.reverse()

                order.tracking_info = new_tracking_info

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "awb_number": updated_awb_number,
                    "current_status": order.status,
                },
                message="Tracking successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Some error occurred",
            )

    @staticmethod
    def cancel_shipment(order: Order_Model, awb_number: str):

        try:

            text = "crm@warehousity.com:crm@123#"
            text_bytes = text.encode("utf-8")
            base64_bytes = base64.b64encode(text_bytes)
            base64_text = base64_bytes.decode("utf-8")

            headers = {
                "Authorization": "Bearer " + "865fc3fb405bc9a5fa3c4832a7a24009",
                "Content-Type": "application/json",
                "User-Agent": "LastMiles/1.0",
            }

            api_url = "https://api.logistify.in/connect/cancel-awb"

            body = {"awb_no": awb_number}

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=10
            )

            # If tracking failed at Logistify, return message
            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            if response_data["error"] == True:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message=response_data["msg"],
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Succesfully cancelled",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Some error occurred",
            )
