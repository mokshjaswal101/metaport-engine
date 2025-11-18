import http
from psycopg2 import DatabaseError
from typing import Dict
import requests
import pytz
import os
from datetime import datetime
import unicodedata
from pydantic import BaseModel
from dateutil.parser import parse
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

from context_manager.context import context_user_data, get_db_session

from logger import logger
import re

# models
from models import (
    Pickup_Location,
    Order,
    Client,
    Company_Contract,
    Pincode_Serviceability,
)

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel
from modules.company_contract.company_contract_schema import CompanyContractModel
from shipping_partner.delhivery.delhivery import Delhivery

# data
from .status_mapping import status_mapping

# service
from modules.wallet.wallet_service import WalletService


from utils.datetime import parse_datetime


def clean_text(text):
    if text is None:
        return ""
    # Normalize Unicode and replace non-breaking spaces with normal spaces
    text = unicodedata.normalize("NFKC", text).replace("\xa0", "").strip()
    # Keep only letters and numbers, remove all other characters
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)
    return text


class TempModel(BaseModel):
    client_id: int


class Bluedart:

    # GenerateToken
    def Add_Contract_Generate_Token(credentials: Dict[str, str]):
        try:
            api_url = "https://apigateway.bluedart.com/in/transportation/token/v1/login"

            headers = {
                "Content-Type": "application/json",
                "ClientID": credentials["client_id"],
                "clientSecret": credentials["client_secret"],
            }

            response = requests.get(api_url, headers=headers, verify=True, timeout=10)
            #  Debug prints
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            try:
                print(f"Response JSON: {response.json()}")
            except Exception:
                print("Response is not in JSON format.")

            print(response.status_code, "<status_code>")
            #  Check response
            if response.status_code == 200:
                return {
                    "status_code": 200,
                    "status": True,
                    "message": "Token is valid",
                    # "data": response.json(),
                }

            elif response.status_code in [401, 403]:
                return {
                    "status_code": response.status_code,
                    "status": False,
                    "message": "Invalid or expired token",
                    "data": response.text,
                }

            else:
                #  Return actual code and response for debugging
                return {
                    "status_code": response.status_code,
                    "status": False,
                    "message": "Unexpected response from API",
                    "data": response.text,
                }

        except ConnectionError:
            logger.error(
                "Error: Unable to connect to the DTDC API. Check the URL or network connection."
            )
            return {"error": "Unable to connect to the DTDC API"}

        except Timeout:
            logger.error("Error: The request to the DTDC API timed out.")
            return {"error": "Request timed out"}

        except HTTPError as http_err:
            logger.error("HTTP error occurred", http_err)
            return {"error": f"HTTP error: {http_err}"}

        except RequestException as req_err:
            logger.error("An error occurred", req_err)
            return {"error": f"Request error: {req_err}"}

        except Exception as e:
            logger.error("Unexpected error", e)
            return {"error": "An unexpected error occurred"}

    @staticmethod
    def get_token(credentials: Dict[str, str]):

        try:

            api_url = "https://apigateway.bluedart.com/in/transportation/token/v1/login"

            headers = {
                "Content-Type": "application/json",
                "ClientID": credentials["client_id"],
                "clientSecret": credentials["client_secret"],
            }

            response = requests.get(api_url, headers=headers, verify=True, timeout=10)
            response = response.json()

            token = response.get("JWTToken", None)

            if token is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response.get("title", "Failed to create shipment"),
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.BAD_REQUEST,
                status=True,
                message="success",
                data=token,
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

            token = Bluedart.get_token(credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            # get the location code for shiperfecto from the db

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )

            client = db.query(Client).filter(Client.id == client_id).first()

            consignee_address1 = clean_text(order.consignee_address.strip())
            consignee_landmark = clean_text(
                order.consignee_landmark.strip() if order.consignee_landmark else ""
            )

            pickup_address1 = clean_text(pickup_location.address.strip())
            pickup_landmark = clean_text(
                pickup_location.landmark.strip() if pickup_location.landmark else ""
            )

            body = {
                "Request": {
                    "Consignee": {
                        "AvailableDays": "",
                        "AvailableTiming": "",
                        "ConsigneeAddress1": consignee_address1,
                        "ConsigneeAddress2": consignee_landmark,
                        "ConsigneeAddress3": "",
                        "ConsigneeAddressType": "",
                        "ConsigneeAddressinfo": "",
                        "ConsigneeAttention": "",
                        "ConsigneeEmailID": "",
                        "ConsigneeFullAddress": consignee_address1
                        + " "
                        + consignee_landmark,
                        "ConsigneeGSTNumber": "",
                        "ConsigneeLatitude": "",
                        "ConsigneeLongitude": "",
                        "ConsigneeMaskedContactNumber": "",
                        "ConsigneeMobile": order.consignee_phone,
                        "ConsigneeName": order.consignee_full_name,
                        "ConsigneePincode": order.consignee_pincode,
                        "ConsigneeTelephone": (
                            order.consignee_alternate_phone
                            if order.consignee_alternate_phone
                            else ""
                        ),
                    },
                    "Returnadds": {
                        "ManifestNumber": "",
                        "ReturnAddress1": pickup_address1,
                        "ReturnAddress2": pickup_landmark,
                        "ReturnAddress3": "",
                        "ReturnAddressinfo": "",
                        "ReturnContact": clean_text(
                            pickup_location.contact_person_name
                        ),
                        "ReturnEmailID": pickup_location.contact_person_email,
                        "ReturnLatitude": "",
                        "ReturnLongitude": "",
                        "ReturnMaskedContactNumber": "",
                        "ReturnMobile": pickup_location.contact_person_phone,
                        "ReturnPincode": pickup_location.pincode,
                        "ReturnTelephone": "",
                    },
                    "Services": {
                        "ActualWeight": float(order.applicable_weight),
                        "CollectableAmount": (
                            "0"
                            if order.payment_mode.lower() == "prepaid"
                            else str(order.total_amount)
                        ),
                        "Commodity": {
                            "CommodityDetail1": "",
                            "CommodityDetail2": "",
                            "CommodityDetail3": "",
                        },
                        "CreditReferenceNo": str(order.client_id)
                        + "/"
                        + str(order.id)
                        + (
                            f"/{str(order.cancel_count)}"
                            if order.cancel_count > 0
                            else ""
                        ),
                        "CreditReferenceNo2": "",
                        "CreditReferenceNo3": "",
                        "DeclaredValue": str(order.total_amount),
                        "DeliveryTimeSlot": "",
                        "Dimensions": [
                            {
                                "Breadth": float(order.breadth),
                                "Count": 1,
                                "Height": float(order.height),
                                "Length": float(order.length),
                            }
                        ],
                        "FavouringName": "",
                        "IsDedicatedDeliveryNetwork": False,
                        "IsDutyTaxPaidByShipper": False,
                        "IsForcePickup": False,
                        "IsPartialPickup": False,
                        "IsReversePickup": False,
                        "ItemCount": order.product_quantity,
                        "Officecutofftime": "",
                        "PDFOutputNotRequired": True,
                        "PackType": "L",
                        "ParcelShopCode": "",
                        "PayableAt": "",
                        "PickupDate": f"/Date({int(datetime.now().timestamp() * 1000)})/",
                        "PickupMode": "",
                        "PickupTime": "1400",
                        "PickupType": "",
                        "PieceCount": 1,
                        "RegisterPickup": True,
                        "PreferredPickupTimeSlot": "",
                        "ProductCode": "A",
                        "ProductFeature": "",
                        "ProductType": 1,
                        "SpecialInstruction": "",
                        "SubProductCode": (
                            "P" if order.payment_mode.lower() == "prepaid" else "C"
                        ),
                        "itemdtl": [
                            {
                                "ItemName": product["name"],
                                "ItemValue": str(product["unit_price"]),
                                "Itemquantity": str(product["quantity"]),
                            }
                            for product in order.products
                        ],
                        "noOfDCGiven": 0,
                    },
                    "Shipper": {
                        "CustomerAddress1": pickup_address1,
                        "CustomerAddress2": pickup_landmark,
                        "CustomerAddress3": "",
                        "CustomerAddressinfo": "",
                        "CustomerBusinessPartyTypeCode": "",
                        "CustomerCode": credentials["client_code"],
                        "CustomerEmailID": pickup_location.contact_person_email,
                        "CustomerLatitude": "",
                        "CustomerLongitude": "",
                        "CustomerMaskedContactNumber": "",
                        "CustomerMobile": pickup_location.contact_person_phone,
                        "CustomerName": pickup_location.contact_person_name,
                        "CustomerPincode": pickup_location.pincode,
                        "CustomerTelephone": "",
                        "IsToPayCustomer": False,
                        "OriginArea": {89: "KLL", 99: "AHD", 120: "BOM"}.get(
                            client_id, "DEL"
                        ),
                        "VendorCode": pickup_location.location_code,
                    },
                },
                "Profile": {
                    "LoginID": credentials["login_id"],
                    "LicenceKey": credentials["licence_key"],
                    "Api_type": "S",
                },
            }

            print(body)

            headers = {
                "Content-Type": "application/json",
                "JWTToken": token,
            }

            print(headers)

            api_url = "https://apigateway.bluedart.com/in/transportation/waybill/v1/GenerateWayBill"

            response = requests.post(
                api_url, json=body, headers=headers, verify=False, timeout=10
            )

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )

            # if order created successfully at shiperfecto
            if response.status_code != 200:

                try:
                    error = response_data["error-response"]
                    error = error[0]["Status"]
                    error = error[0]["StatusInformation"]

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=error,
                    )

                except:

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Bad Request, could not assign AWB, please try again",
                    )

            if response.status_code == 200:

                data = response_data["GenerateWayBillResult"]
                awb = data.get("AWBNo", None)

                if not awb:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Failed to assign AWB, please try again",
                    )

                # update status
                order.status = "booked"
                order.sub_status = "shipment booked"
                order.courier_status = "BOOKED"

                order.awb_number = awb
                order.aggregator = "bluedart"
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + "bluedart",
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                }

                # update the activity

                order.action_history.append(new_activity)

                db.add(order)
                db.flush()

                db.flush()
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": awb,
                        "delivery_partner": delivery_partner.slug,
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

            client_id = context_user_data.get().client_id

            credentials = {
                "client_id": "KMxooc9ePA4v3Y4yzoUqm7rJUCsZcxLf",
                "client_secret": "zsF6Doq3x6kpg96O",
                "login_id": "BOM18228",
                "client_code": "704480",
                "licence_key": "lgislqejkgi6qktviqtohpofnvgbrrkm",
            }

            token = Bluedart.get_token(credentials=credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            headers = {
                "Content-Type": "application/json",
                "JWTToken": token,
            }

            api_url = f"https://apigateway.bluedart.com/in/transportation/tracking/v1/shipment?handler=tnt&loginid=BOM18228&numbers={awb_number}&format=json&lickey=fmkuizsistupuokh2offlygmhsylxjro&scan=1&action=custawbquery&verno=1&awb=awb"

            response = requests.get(api_url, headers=headers, verify=False, timeout=10)

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            tracking_data = response_data.get("ShipmentData", "")

            # if tracking_data is not present in the respnse
            if not tracking_data:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            error = tracking_data.get("Error", "")

            if error:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=error,
                )

            tracking_data = tracking_data.get("Shipment", "")
            tracking_data = (
                tracking_data[0] if isinstance(tracking_data, list) else tracking_data
            )

            print(1)

            if not tracking_data:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            print(2)

            ist = pytz.timezone("Asia/Kolkata")
            utc = pytz.utc

            edd = tracking_data.get("ExpectedDelivery", None)

            if edd:
                try:
                    local_time = ist.localize(datetime.strptime(edd, "%d %B %Y"))
                    order.edd = local_time.astimezone(utc)
                except ValueError:
                    order.edd = None  # Handle invalid date format
            else:
                order.edd = None  # Handle missing key

            tracking_data = tracking_data.get("Scans")

            print(3)

            if not tracking_data:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Some error occurred while tracking, please try again",
                )

            courier_status = tracking_data[0]["ScanDetail"]["ScanCode"]
            courier_status_type = tracking_data[0]["ScanDetail"]["ScanType"]
            courier_status_group = tracking_data[0]["ScanDetail"]["ScanGroupType"]

            activites = tracking_data

            db = get_db_session()

            print(4)

            print(status_mapping["PU"]["500-S"]["status"])

            # print(courier_status)

            print(courier_status, courier_status_type, courier_status_group)
            print(f"{courier_status}-{courier_status_group}")

            # update the order status, and awb if different
            order.status = status_mapping[courier_status_type][
                f"{courier_status}-{courier_status_group}"
            ]["status"]

            print(order.status)

            order.sub_status = status_mapping[courier_status_type][
                f"{courier_status}-{courier_status_group}"
            ]["sub_status"]
            order.courier_status = f"{courier_status}-{courier_status_group}"

            print(order.status, order.sub_status, order.courier_status)

            # update the tracking info
            if activites:

                new_tracking_info = []
                for activity in activites:

                    try:

                        sd = activity["ScanDetail"]

                        code = str(sd.get("ScanCode", "")).strip()  # e.g. "007"
                        group = str(sd.get("ScanGroupType", "")).strip()  # e.g. "S"
                        key = (
                            f"{code}-{group}"  # -> "007-S" (preserves/pads leading 0s)
                        )

                        status = status_mapping[sd["ScanType"]][key]["status"]

                        obj = {
                            "status": status,
                            "description": sd.get("Scan", ""),
                            "subinfo": sd.get("Scan", ""),
                            "datetime": datetime.strptime(
                                f"{sd.get('ScanDate','').strip()} {(sd.get('ScanTime') or '00:00').strip()}",
                                "%d-%b-%Y %H:%M",
                            ).strftime("%Y-%m-%d %H:%M:%S"),
                            "location": sd.get("ScannedLocation", ""),
                        }
                        new_tracking_info.append(obj)

                    except Exception as e:
                        print(str(e))
                        continue
                # new_tracking_info.reverse()

                if client_id == 310:

                    if len(new_tracking_info) >= 2:
                        new_tracking_info[-2][
                            "location"
                        ] = "CIVIL LINE SERVICE CENTRE, MEERUT, UP"

                    if len(new_tracking_info) >= 1:
                        new_tracking_info[-1][
                            "location"
                        ] = "CIVIL LINE SERVICE CENTRE, MEERUT, UP"

                order.tracking_info = new_tracking_info

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "awb_number": awb_number,
                    "current_status": status_mapping[courier_status_type][
                        f"{courier_status}-{courier_status_group}"
                    ]["status"],
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

            credentials = {
                "client_id": "KMxooc9ePA4v3Y4yzoUqm7rJUCsZcxLf",
                "client_secret": "zsF6Doq3x6kpg96O",
                "login_id": "BOM18228",
                "client_code": "704480",
                "licence_key": "lgislqejkgi6qktviqtohpofnvgbrrkm",
            }

            token = Bluedart.get_token(credentials=credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            headers = {
                "Content-Type": "application/json",
                "JWTToken": token,
            }

            api_url = f"https://apigateway.bluedart.com/in/transportation/waybill/v1/CancelWaybill"

            body = {
                "Request": {"AWBNo": order.awb_number},
                "Profile": {
                    "Api_type": "S",
                    "LicenceKey": credentials["licence_key"],
                    "LoginID": credentials["login_id"],
                },
            }

            response = requests.post(
                api_url, headers=headers, json=body, verify=False, timeout=10
            )

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data.get(
                        "error-response", "Unknown error occurred"
                    ),
                )

            if response.status_code == 200:

                data = response_data.get("CancelWaybillResult", None)

                if data is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Failed to cancel shipment, please try again",
                    )

                isError = data.get("IsError", None)

                if isError:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=data.get("Failed to cancel shipment"),
                    )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Shipment cancelled successfully",
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
    def tracking_webhook(track_req):

        try:

            from modules.shipment.shipment_service import ShipmentService

            print(track_req)

            tracking = track_req.get("statustracking", None)

            tracking = tracking[0]

            track_req = tracking.get("Shipment", None)

            db = get_db_session()

            awb_number = track_req.get("WaybillNo", None)

            if awb_number == None or awb_number == "" or not awb_number:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=False,
                    message="Invalid AWB",
                )

            order = db.query(Order).filter(Order.awb_number == awb_number).first()

            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=False,
                    message="Invalid AWB",
                )

            scan_obj = track_req.get("Scans")
            scan_obj = scan_obj.get("ScanDetail")
            scan_obj = scan_obj[0]

            current_status = order.sub_status

            if current_status == "delivered" or current_status == "RTO delivered":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Tracking Successfull",
                )

            courier_status = scan_obj.get("ScanCode", "")
            courier_status_type = scan_obj.get("ScanType", "")
            courier_status_group = scan_obj.get("ScanGroupType", "")

            order.status = status_mapping[courier_status_type][
                f"{courier_status}-{courier_status_group}"
            ]["status"]

            order.sub_status = status_mapping[courier_status_type][
                f"{courier_status}-{courier_status_group}"
            ]["sub_status"]

            order.courier_status = f"{courier_status}-{courier_status_group}"

            location = f'{scan_obj.get("ScannedLocation", "")}, {scan_obj.get("ScannedLocationCity", "")}, {scan_obj.get("ScannedLocationStateCode", "")}'

            if order.client_id == 310:

                if order.status == "booked" or order.status == "pickup":
                    if "mumbai" in location.lower():
                        location = "CIVIL LINE SERVICE CENTRE, MEERUT, UP"
                    elif "delhi" in location.lower():
                        location = "CIVIL LINE SERVICE CENTRE, MEERUT, UP"
                    elif "gurgaon" in location.lower():
                        location = "CIVIL LINE SERVICE CENTRE, MEERUT, UP"

            new_tracking_info = {
                "status": order.status,
                "description": scan_obj.get("Scan", ""),
                "subinfo": scan_obj.get("Scan", ""),
                "datetime": datetime.strptime(
                    f"{scan_obj.get('ScanDate','').strip()} {(scan_obj.get('ScanTime') or '00:00').strip()}",
                    "%d-%m-%Y %H%M",
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "location": location,
            }

            if not order.tracking_info:
                order.tracking_info = []

            order.tracking_info = [new_tracking_info] + order.tracking_info

            ShipmentService.post_tracking(order)

            order.tracking_response = track_req

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Tracking Successfull",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Tracking Successfull",
            )

        finally:
            if db:
                db.close()

    @staticmethod
    def ndr_action(
        order: Order_Model,
        awb_number: str,
        # credentials: Dict[str, str],
    ):
        try:

            credentials = {
                "client_id": "KMxooc9ePA4v3Y4yzoUqm7rJUCsZcxLf",
                "client_secret": "zsF6Doq3x6kpg96O",
                "login_id": "BOM18228",
                "client_code": "704480",
                "licence_key": "lgislqejkgi6qktviqtohpofnvgbrrkm",
            }

            token = Bluedart.get_token(credentials=credentials)

            if token.status == False:
                return token

            else:
                token = token.data

            headers = {
                "Content-Type": "application/json",
                "JWTToken": token,
            }

            api_url = f"https://apigateway.bluedart.com/in/transportation/cust-instruction-update/v1/CustALTInstructionUpdate"

            body = {
                "altreq": {"AWBNo": order.awb_number, "AltInstRequestType": "DT"},
                "profile": {
                    "Api_type": "S",
                    "LicenceKey": credentials["licence_key"],
                    "LoginID": credentials["login_id"],
                },
            }

            response = requests.post(
                api_url, headers=headers, json=body, verify=False, timeout=10
            )

            try:
                response_data = response.json()
                print(response_data)

            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while tracking, please try again",
                )

            if response.status_code != 200:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data.get(
                        "error-response", "Unknown error occurred"
                    ),
                )

            if response.status_code == 200:

                data = response_data.get("CancelWaybillResult", None)

                if data is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Failed to cancel shipment, please try again",
                    )

                isError = data.get("IsError", None)

                if isError:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=data.get("Failed to cancel shipment"),
                    )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Shipment cancelled successfully",
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
