import http
from psycopg2 import DatabaseError
from typing import Dict
import requests
from datetime import datetime

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


class Fship:

    BASE_URL = "https://capi-qc.fship.in/api"

    @staticmethod
    def create_pickup_location(pickup_location_id: int, credentials: Dict[str, str]):

        try:

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == pickup_location_id)
                .first()
            )

            client_id = context_user_data.get().client_id

            api_url = Fship.BASE_URL + "/addwarehouse"

            body = {
                "warehouseId": pickup_location_id,
                "warehouseName": str(client_id) + "/" + pickup_location.location_name,
                "contactName": pickup_location.contact_person_name,
                "addressLine1": pickup_location.address,
                "addressLine2": "",
                "pincode": pickup_location.pincode,
                "city": pickup_location.country,
                "phoneNumber": pickup_location.contact_person_phone,
                "email": pickup_location.contact_person_email,
            }

            headers = {
                "signature": credentials["security_key"],
                "Content-Type": "application/json",
            }

            response = requests.post(api_url, json=body, headers=headers, verify=False)

            response_data = response.json()

            print(response_data)

            # if location creation failed
            if response_data["status"] == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

            # if successfully created location

            if response_data["status"] == True:
                fship_location_id = response_data["warehouseId"]

                pickup_location.courier_location_codes = {
                    **pickup_location.courier_location_codes,
                    "fship": fship_location_id,
                }

                db.add(pickup_location)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=fship_location_id,
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

            # get the location code for shiperfecto from the db
            with get_db_session() as db:
                courier_location_codes = (
                    db.query(Pickup_Location.courier_location_codes)
                    .filter(Pickup_Location.location_code == order.pickup_location_code)
                    .first()
                )[0]

                fship_pickup_location = courier_location_codes.get("fship", None)

                # if no fship location code mapping is found for the current pickup location, create a new warehouse at fship
                if fship_pickup_location is None:

                    fship_pickup_location = Fship.create_pickup_location(
                        order.pickup_location_code, credentials
                    )

                    # if could not create location at Fship, throw error
                    if fship_pickup_location.status == False:

                        return GenericResponseModel(
                            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                            message="Unable to place order. Please try again later",
                        )

                    else:
                        fship_pickup_location = fship_pickup_location.data

            print(0)

            body = {
                "customer_Name": order.consignee_full_name,
                "customer_Mobile": order.consignee_phone,
                "customer_Emailid": order.consignee_email,
                "customer_Address": order.consignee_address,
                "landMark": order.consignee_landmark,
                "customer_Address_Type": "warehouse",
                "customer_PinCode": str(order.consignee_pincode),
                "customer_City": order.consignee_city,
                "orderId": order.order_id,
                "invoice_Number": "",
                "payment_Mode": 2 if order.payment_mode == "prepaid" else 1,
                "express_Type": delivery_partner.mode,
                "is_Ndd": 0,
                "order_Amount": float(order.order_value),
                "tax_Amount": float(order.tax_amount),
                "extra_Charges": float(
                    order.other_charges
                    + order.shipping_charges
                    + order.gift_wrap_charges
                    + order.cod_charges
                    - order.discount
                ),
                "total_Amount": float(order.total_amount),
                "cod_Amount": (
                    0 if order.payment_mode == "prepaid" else float(order.total_amount)
                ),
                "shipment_Weight": float(order.weight),
                "shipment_Length": float(order.length),
                "shipment_Width": float(order.breadth),
                "shipment_Height": float(order.height),
                "pick_Address_ID": fship_pickup_location,
                "return_Address_ID": fship_pickup_location,
                "products": [
                    {
                        "productName": product["name"],
                        "unitPrice": float(product["unit_price"]),
                        "quantity": product["quantity"],
                        "sku": product["sku_code"],
                    }
                    for product in order.products
                ],
                "courierId": int(delivery_partner.aggregator_slug),
            }

            print(1)

            headers = {
                "signature": credentials["security_key"],
                "Content-Type": "application/json",
            }

            print(2)

            api_url = Fship.BASE_URL + "/createforwardorder"

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=10
            )

            print(3)

            try:
                response_data = response.json()

                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # If order creation failed at Fship, return message
            if (
                response_data.get("status", None) is None
                or response_data["status"] == False
            ):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=list(response_data.values())[0][0],
                )

            # if order created successfully at shiperfecto
            if response_data["status"] == True:

                with get_db_session() as db:

                    # update status
                    order.status = "booked"
                    order.sub_status = "booked"
                    order.courier_status = "BOOKED"

                    order.awb_number = response_data["order_data"]["awb"]
                    order.aggregator = "shiperfecto"
                    order.shipping_partner_order_id = response_data["order_data"][
                        "order_id"
                    ]
                    order.courier_partner = delivery_partner.slug

                    new_activity = {
                        "event": "Shipment Created",
                        "subinfo": "delivery partner - "
                        + response_data["order_data"]["delivery_partner"],
                        "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    }

                    # update the activity

                    order.action_history.append(new_activity)

                    db.add(order)
                    db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["order_data"]["awb"],
                        "delivery_partner": response_data["order_data"][
                            "delivery_partner"
                        ],
                    },
                    message="AWB assigned successfully",
                )

            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["order_data"]["error"],
                )

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

    @staticmethod
    def track_shipment(order: Order_Model, awb_number: str):

        try:
            headers = {
                "Authorization": "Token " + "Nd5yU3yienFHsoQ3jnYSYJdLCUefPI2awS7hZIOK",
                "Content-Type": "application/json",
            }

            api_url = "http://app.shiperfecto.com/api/v1/tracking?awb=" + awb_number

            response = requests.get(api_url, headers=headers, verify=False, timeout=10)

            # If tracking failed at Shiperfecto, return message
            try:
                response_data = response.json()
                print("1")

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            print("2")

            # If tracking failed, return message
            if response_data["success"] == False:

                print("3")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["tracking"]["status"],
                )

            tracking_data = response_data.get("tracking", "")

            print("4")

            # if tracking_data is not present in the respnse
            if not tracking_data:
                print("5")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            print("6")
            courier_status = tracking_data.get("status", "")
            updated_awb_number = tracking_data.get("awb", "")

            activites = tracking_data.get("events", "")

            with get_db_session() as db:

                # update the order status, and awb if different
                order.status = status_mapping[courier_status]["status"]
                order.sub_status = status_mapping[courier_status]["sub_status"]
                order.courier_status = courier_status

                order.awb_number = (
                    updated_awb_number if updated_awb_number else order.awb_number
                )

                # update the tracking info
                if activites:
                    new_tracking_info = [
                        {
                            "status": status_mapping[activity.get("status", "")][
                                "sub_status"
                            ],
                            "description": activity.get("description", ""),
                            "subinfo": activity.get("status", ""),
                            "datetime": activity.get("status_datetime", ""),
                            "location": activity.get("status_location", ""),
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
                    "current_status": status_mapping[courier_status]["sub_status"],
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
