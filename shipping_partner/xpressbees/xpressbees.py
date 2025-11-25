import http
from psycopg2 import DatabaseError
from typing import Dict, Optional, List
import requests
from datetime import datetime, timedelta
from datetime import datetime
from fastapi import Request

import httpx
from sqlalchemy import select
import json
from sqlalchemy.orm import selectinload
from fastapi.encoders import jsonable_encoder

from decimal import Decimal, InvalidOperation
from collections import defaultdict
from httpx import ConnectError, HTTPStatusError, RequestError

# from datetime import timedelta
from context_manager.context import context_user_data, get_db_session
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

from logger import logger

# models
from models import Pickup_Location, Order

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_partner.shipping_partner_schema import AggregatorCourierModel


# data
from .status_mapping import status_mapping

# utils
from utils.string import clean_text


from utils.datetime import parse_datetime

from models.company_to_client_contract import Company_To_Client_Contract as Contract
from models.company_to_client_cod_rates import Company_To_Client_COD_Rates as CODRate
from models.company_to_client_rates import Company_To_Client_Rates as Rate
from models.new_company_to_client_rate import New_Company_To_Client_Rate

from .pincodes import xb_cream_pincodes


class Xpressbees:

    # API URL'S
    Generate_Url = "https://userauthapis.xbees.in/api/auth/generateToken"
    create_order_url = "https://global-api.xbees.in/global/v1/serviceRequest"
    cancellation_order_url = "https://clientshipupdatesapi.xbees.in/forwardcancellation"
    track_order_url = "https://apishipmenttracking.xbees.in/GetShipmentAuditLog"
    raise_ndr_url = (
        "https://clientshipupdatesapi.xbees.in/client/UpdateNDRDeferredDeliveryDate"
    )

    create_pickup_location_url = (
        "https://track.delhivery.com/api/backend/clientwarehouse/create/"
    )

    clientId = "27631"
    clientName = "Genex"
    secretkey = "2d8439c89437b453a46ec703afef0f95e48246c0b9205090dddb9bb8b5e473c7"
    username = "admin@surfgene.com"
    password = "$surfgene$"

    def Add_Contract_Generate_Token(credentials):
        try:
            url = Xpressbees.Generate_Url
            payload = json.dumps(
                {
                    "username": credentials["username"],
                    "password": credentials["password"],
                    "secretkey": credentials["secretkey"],
                }
            )
            headers = {"Authorization": "Basic xyz", "Content-Type": "application/json"}
            response = requests.request("POST", url, headers=headers, data=payload)
            data = response.json()  # convert response to dict
            #  Debug prints
            if "error" in data:
                return {
                    "status_code": http.HTTPStatus.BAD_REQUEST,
                    "status": False,
                    "message": f"Error: {data['error']} (code: {data.get('code')})",
                }
            else:
                return {
                    "status_code": 200,
                    "status": True,
                    "message": "Token is valid",
                }
        except ConnectionError:
            print(" Connection error occurred (network issue)")
            return {
                "status_code": http.HTTPStatus.BAD_REQUEST,
                "status": False,
                "message": "Unable to connect to Shiperfecto API.",
            }

        except Timeout:
            print(" Request timed out")
            return {
                "status_code": http.HTTPStatus.REQUEST_TIMEOUT,
                "status": False,
                "message": "Request timed out.",
            }

        except HTTPError as http_err:
            print(f" HTTP error occurred: {http_err}")
            return {
                "status_code": http.HTTPStatus.BAD_REQUEST,
                "status": False,
                "message": f"HTTP error: {http_err}",
            }

        except RequestException as req_err:
            print(f" Request exception: {req_err}")
            return {
                "status_code": http.HTTPStatus.BAD_REQUEST,
                "status": False,
                "message": f"Request error: {req_err}",
            }

        except Exception as e:
            print(f" Unexpected error: {e}")
            return {
                "status_code": http.HTTPStatus.INTERNAL_SERVER_ERROR,
                "status": False,
                "message": f"Unexpected error: {e}",
            }

    # Generate_Token
    def Generate_Token(credentials):
        try:
            url = Xpressbees.Generate_Url
            payload = json.dumps(
                {
                    "username": credentials["username"],
                    "password": credentials["password"],
                    "secretkey": credentials["secretkey"],
                }
            )
            headers = {"Authorization": "Basic xyz", "Content-Type": "application/json"}
            response = requests.request("POST", url, headers=headers, data=payload)
            response.raise_for_status()
            return response.json()

        except ConnectionError:
            logger.error(
                "Error: Unable to connect to the Xpressbees API. Check the URL or network connection."
            )
            return {"error": "Unable to connect to the Xpressbees API"}

        except Timeout:
            logger.error("Error: The request to the Xpressbees API timed out.")
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
    def create_pickup_location(
        pickup_location_id: int,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):
        try:
            print(1)
            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == pickup_location_id)
                .first()
            )
            print(2)
            api_url = Xpressbees.create_pickup_location_url

            client_id = context_user_data.get().client_id
            print(3)
            body = json.dumps(
                {
                    "name": str(client_id)
                    + " "
                    + clean_text(pickup_location.location_name),
                    "email": pickup_location.contact_person_email,
                    "phone": pickup_location.contact_person_phone,
                    "address": clean_text(pickup_location.address),
                    "city": clean_text(pickup_location.city),
                    "country": "India",
                    "pin": pickup_location.pincode,
                    "return_address": clean_text(pickup_location.address),
                    "return_pin": pickup_location.pincode,
                    "return_city": pickup_location.city,
                    "return_state": pickup_location.state,
                    "return_country": "India",
                }
            )

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "token " + credentials["token"],
            }
            logger.info(
                "Delhivery create_pickup_location payload ready to post %s",
                body,
                headers,
            )
            print(4)
            response = requests.request("POST", api_url, headers=headers, data=body)

            print(response)

            response_data = response.json()

            print(response)

            # if location creation failed
            if response_data["success"] == False:
                print(5)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="There was some issue in creating your location at the delivery partner. Please try again",
                )

            # if successfully created location
            print(6)
            if response_data["success"] == True:
                Delhivery_location_name = response_data["data"]["name"]

                if delivery_partner.aggregator_slug == "delhivery-air":

                    pickup_location.courier_location_codes = {
                        **pickup_location.courier_location_codes,
                        "delhivery-air": Delhivery_location_name,
                    }

                else:
                    pickup_location.courier_location_codes = {
                        **pickup_location.courier_location_codes,
                        "delhivery": Delhivery_location_name,
                    }

                db.add(pickup_location)
                db.commit()
                print(7)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=Delhivery_location_name,
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
                msg="Error creating location at Delhivery: {}".format(str(e)),
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
    async def dev_Generate_Token():
        try:
            print("<<**welcome to dev_Generate_Token**>>")

            url = "https://stageusermanagementapi.xbees.in/api/auth/generateToken"
            payload = {
                "username": "admin@surfgene.com",
                "password": "Admin@123",
                "secretkey": "5efc887632bcbf63a772789473bbf19b0241bc1c42e777c2ca3eb35d9635a029",
            }
            headers = {
                "Authorization": "Bearer xyz",
                "Content-Type": "application/json",
            }

            # ---- ASYNC HTTP CALL ----
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(url, json=payload, headers=headers)

            # Raise error for non-200 responses
            response.raise_for_status()

            return response.json()

        except ConnectError:
            logger.error("Unable to connect to the Xpressbees API.")
            return {"error": "Unable to connect to the Xpressbees API"}

        except httpx.TimeoutException:
            logger.error("The request to Xpressbees timed out.")
            return {"error": "Request timed out"}

        except HTTPStatusError as http_err:
            logger.error(f"HTTP error: {http_err}")
            return {"error": f"HTTP error: {http_err}"}

        except RequestError as req_err:
            logger.error(f"Request error: {req_err}")
            return {"error": f"Request error: {req_err}"}

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {"error": "An unexpected error occurred"}

    def contract_transfer():
        with get_db_session() as db:
            # contracts = (
            #     db.query(Company_To_Client_Contract)
            #     .options(
            #         selectinload(Company_To_Client_Contract.rates),
            #         selectinload(Company_To_Client_Contract.cod_rates),
            #     )
            #     .all()
            # )
            query = (
                db.query(
                    Contract.client_id,
                    Contract.company_contract_id,
                    Contract.isActive,
                    Contract.aggregator_courier_id,
                    Contract.id.label("contract_id"),
                    Contract.rate_type,
                    CODRate.percentage_rate,
                    CODRate.absolute_rate,
                    Rate.zone,
                    Rate.base_rate,
                    Rate.additional_rate,
                    Rate.rto_base_rate,
                    Rate.rto_additional_rate,
                )
                .join(CODRate, CODRate.contract_id == Contract.id)
                .join(Rate, Rate.contract_id == Contract.id)
                # .filter(
                #     # Contract.isActive == True,
                #     Contract.client_id
                #     == 11
                # )  # Optional: Filter only active contracts
            )
            results = query.all()
            print("HI")
            # Convert to list of dictionaries
            columns = [
                "client_id",
                "company_contract_id",
                "isActive",
                "aggregator_courier_id",
                "contract_id",
                "rate_type",
                "percentage_rate",
                "absolute_rate",
                "zone",
                "base_rate",
                "additional_rate",
                "rto_base_rate",
                "rto_additional_rate",
            ]
            # print([dict(zip(columns, row)) for row in results], "Hello Decinal Action")
            Xpressbees.insert_grouped_rates(
                db, [dict(zip(columns, row)) for row in results]
            )
            # return [dict(zip(columns, row)) for row in results]
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=[dict(zip(columns, row)) for row in results],
                message="transfer",
            )

    def insert_grouped_rates(db, rate_data: list):
        grouped = defaultdict(dict)
        print("WELCOME TO INSERT SECTION")
        for entry in rate_data:
            contract_id = entry["contract_id"]
            zone = entry["zone"].upper()

            # Group shared values only once
            if "client_id" not in grouped[contract_id]:
                try:
                    print(jsonable_encoder(entry), "<<isActive>>")
                    grouped[contract_id].update(
                        {
                            "client_id": entry["client_id"],
                            "aggregator_courier_id": entry["aggregator_courier_id"],
                            "company_contract_id": entry["company_contract_id"],
                            "rate_type": entry["rate_type"],
                            "percentage_rate": (
                                Decimal(entry["percentage_rate"])
                                if entry["percentage_rate"] is not None
                                else Decimal("0.00")
                            ),
                            "absolute_rate": (
                                Decimal(entry["absolute_rate"])
                                if entry["absolute_rate"] is not None
                                else Decimal("0.00")
                            ),
                            "isActive": entry["isActive"],
                            "company_id": 1,
                        }
                    )
                except InvalidOperation as e:
                    print(f"Invalid rate data for contract_id {contract_id}: {e}")
                    continue

            # Add zone-specific fields
            for rate_field in [
                "base_rate",
                "additional_rate",
                "rto_base_rate",
                "rto_additional_rate",
            ]:
                value = entry.get(rate_field)
                try:
                    grouped[contract_id][f"{rate_field}_zone_{zone.lower()}"] = (
                        Decimal(value) if value is not None else Decimal("0.00")
                    )
                except InvalidOperation:
                    print(
                        f"Invalid {rate_field} value for contract_id {contract_id}, zone {zone}: {value}"
                    )
                    grouped[contract_id][f"{rate_field}_zone_{zone.lower()}"] = Decimal(
                        "0.00"
                    )

        MAX_ALLOWED_DECIMAL = Decimal("999.99")  # For NUMERIC(5, 2)

        # Set missing zones to 0.00
        all_zones = ["a", "b", "c", "d", "e"]
        for data in grouped.values():
            for zone in all_zones:
                for key in [
                    "base_rate",
                    "additional_rate",
                    "rto_base_rate",
                    "rto_additional_rate",
                ]:
                    field = f"{key}_zone_{zone}"
                    data.setdefault(field, Decimal("0.00"))

            # Validate all Decimal fields
            for key, value in data.items():
                if isinstance(value, Decimal):
                    if value >= MAX_ALLOWED_DECIMAL:
                        print(f"⚠️ Capping {key} = {value} to 999.99")
                        data[key] = MAX_ALLOWED_DECIMAL
                    elif value < 0:
                        print(
                            f"⚠️ Negative value found {key} = {value}, resetting to 0.00"
                        )
                        data[key] = Decimal("0.00")

            try:
                db_entity = New_Company_To_Client_Rate(**data)
                db.add(db_entity)
            except Exception as e:
                print(
                    f"🚨 Failed to insert data for client_id={data.get('client_id')}: {e}"
                )
                continue

        db.commit()

    @staticmethod
    def create_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):
        try:

            print(credentials["clientName"])

            client_id = context_user_data.get().client_id

            if (
                client_id == 422
                and int(order.consignee_pincode) not in xb_cream_pincodes
            ):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Non Serviceable Pincode",
                )

            # get the location code for shiperfecto from the db
            db = get_db_session()
            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )

            pincodes = [
                "713358",
                "713321",
                "713323",
                "713347",
                "713338",
                "756122",
                "756120",
                "756112",
                "202002",
                "202122",
                "301712",
                "824231",
                "824203",
                "175010",
                "175011",
                "175046",
                "201206",
                "321210",
                "301607",
                "335512",
                "335528",
                "249306",
                "133206",
                "843325",
                "843329",
                "843334",
                "845403",
                "175009",
                "171018",
                "786158",
                "504293",
                "504295",
                "505528",
                "505473",
                "505530",
                "509209",
                "603104",
                "603102",
                "603127",
                "500003",
                "600060",
                "600001",
                "600063",
                "603048",
                "600045",
                "600047",
                "600064",
                "602024",
                "509215",
                "509203",
                "632511",
                "632531",
                "632516",
                "591304",
                "635103",
                "635114",
                "635119",
                "577526",
                "626116",
                "626126",
                "626132",
                "626133",
                "626138",
                "626149",
                "626190",
                "671312",
                "671310",
                "670353",
                "670644",
                "670646",
                "400064",
                "400095",
                "400019",
                "412201",
                "400057",
                "415540",
                "431207",
                "422210",
                "416510",
                "414602",
                "400612",
                "412202",
                "392061",
                "392060",
                "388120",
                "387310",
                "388110",
                "431117",
                "431114",
                "431007",
                "425109",
                "431216",
                "423120",
                "431200",
                "431218",
                "431222",
                "431223",
                "431224",
                "431225",
                "431534",
                "443107",
                "443108",
                "422219",
                "425449",
                "422213",
                "422212",
                "422010",
                "431121",
                "413737",
                "400012",
                "843327",
                "143601",
                "144201",
                "441701",
                "752111",
                "144303",
                "144301",
                "752107",
                "146115",
                "144302",
                "752106",
                "609807",
                "752110",
                "146114",
                "609802",
                "609810",
                "609204",
                "609202",
                "609811",
                "752116",
                "612106",
                "609803",
                "441703",
                "221406",
                "221404",
                "711107",
                "711227",
                "711204",
                "711203",
                "700105",
                "711101",
                "202150",
                "202001",
                "202117",
                "202171",
                "281005",
                "281006",
                "281002",
                "281001",
                "281004" "400004",
                "400006",
                "400007",
                "400008",
                "400010",
                "400011",
                "400026",
                "400027",
                "400033",
                "400035",
                "400043",
                "400088",
                "400071",
                "400072",
                "400074",
                "400089",
                "400094",
                "400086",
                "410101",
                "400067",
                "400066",
                "400017",
                "400018",
                "400013",
                "410221",
                "410216",
                "410208",
                "400615",
                "421102",
                "401107",
                "465230",
                "465444",
                "441902",
                "444301",
                "444307",
                "441906",
                "497235",
                "475220",
                "473865",
                "455118",
                "455116",
                "455115",
                "444105",
                "495444",
                "495445",
                "485666",
                "454221",
                "465677",
                "464114",
                "458895",
                "458669",
                "458667",
                "491340",
                "472442",
                "457769",
                "493221",
                "496445",
                "493559",
                "496450",
                "458990",
                "458558",
                "458389",
                "451228",
                "451224",
                "451221",
                "454552",
                "854202",
                "854205",
                "854203",
                "848202",
                "848203",
                "848204",
                "848201",
                "493662",
                "442502",
                "443203",
                "443308",
                "456313",
                "312606",
                "454010",
                "457772",
                "484336",
                "473287",
                "484444",
                "484771",
                "484776",
                "443202",
                "453551",
                "465441",
                "484774",
                "442505",
                "484770",
                "484334",
                "741233",
                "803121",
                "803117",
                "805107",
                "805141",
                "743291",
                "759127",
                "723213",
                "805124",
                "854327",
                "805130",
                "803116",
                "845301",
                "828205",
                "193411",
                "193404",
                "185102",
                "185101",
                "182121",
                "180004",
                "180012",
                "180020",
                "185155",
                "185151",
                "185152",
                "185153",
                "185156",
                "185234",
                "185131",
                "185132",
                "190021",
                "190009",
                "190007",
                "191132",
                "190005",
                "190008",
                "190018",
                "190014",
                "192121",
                "191102",
                "180003",
                "180009",
                "181101",
                "181102",
                "181103",
                "181105",
                "181111",
                "181113",
                "181131",
                "181132",
                "181104",
                "181112",
                "181114",
                "193411",
                "193404",
                "185102",
                "185101",
                "182121",
                "180004",
                "180012",
                "180020",
                "185155",
                "185151",
                "185152",
                "185153",
                "185156",
                "185234",
                "185131",
                "185132",
                "190021",
                "190009",
                "190007",
                "191132",
                "190005",
                "190008",
                "190018",
                "190014",
                "192121",
                "191102",
                "180003",
                "180009",
                "181101",
                "181102",
                "181103",
                "181105",
                "181111",
                "181113",
                "181131",
                "181132",
                "181104",
                "181112",
                "181114",
                "493554",
                "493555",
                "495695",
                "497449",
                "497450",
                "494553",
                "494556",
                "492885",
                "491885",
                "491444",
                "491881",
                "412240",
                "412212",
                "412213",
                "412238",
                "412239",
                "412241",
                "412236",
                "412211",
                "415022",
                "413409",
                "413401",
                "413022",
                "413021",
                "413411",
                "431603",
                "415302",
                "416408",
                "412308",
                "412101",
                "410506",
                "412113",
                "412106",
                "412109",
                "410507",
                "425115",
                "424006",
                "423402",
                "423104",
                "363421",
                "383310",
                "383330",
                "385535",
                "382241",
                "382276",
                "363424",
                "392230",
                "392170",
                "411057",
                "441107",
                "441109",
                "441112",
                "441113",
                "441117",
                "441403",
                "441502",
                "441503",
                "441504",
                "445210",
                "443404",
                "443407",
                "444304",
                "444704",
                "444720",
                "444810",
            ]

            if (
                order.consignee_pincode in pincodes
                or pickup_location.pincode in pincodes
            ):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Not serviceable by Xpressbees",
                )

            token = Xpressbees.Generate_Token(credentials)

            if "error" in token:
                logger.error(
                    "XPRESSBEES generate_token token error: %s", token["error"]
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=token["error"],
                )

            body = {
                "clientDetails": {
                    "clientId": credentials["clientId"],
                    "clientName": credentials["clientName"],
                    "pickupVendorCode": client_id,
                },
                "serviceDetails": {
                    "serviceName": "Forward",
                    "serviceMode": "SURFACE",
                    "serviceVertical": "Ecom",
                    "serviceType": "SD",
                },
                "shipmentDetails": {
                    "awbMpsGroupId": "",
                    "packageType": "packageType",
                    "orderType": (
                        "PRE" if order.payment_mode.lower() == "prepaid" else "COD"
                    ),
                    "partialRTOAllowed": False,
                    "allowPartialPickup": False,
                    "packageQuantity": {"value": 1, "unit": "Pck"},
                    "totalWeight": {
                        "value": float(order.applicable_weight),
                        "unit": "kg",
                    },
                    "orderDetails": [
                        {
                            "orderNumber": "LM-"
                            + str(client_id)
                            + "-"
                            + order.order_id
                            + (
                                f"/{str(order.cancel_count)}"
                                if order.cancel_count > 0
                                else ""
                            ),
                            "awbNumber": "",
                            "subOrderNumber": "",
                            "customerPromiseDate": "",
                            "collectableAmount": {
                                "unit": "INR",
                                "value": (
                                    0
                                    if order.payment_mode.lower() == "prepaid"
                                    else float(order.total_amount)
                                ),
                            },
                            "declaredAmount": {
                                "value": float(order.total_amount),
                                "unit": "INR",
                            },
                            "helpContent": {
                                "senderName": None,
                                "isOpenDelivery": None,
                                "isCommercialProperty": None,
                                "isDGShipmentType": None,
                            },
                            "pickUpInstruction": {
                                "pickupType": "Vendor",
                                "priorityRemarks": "",
                                "isPickupPriority": "1",
                                "pickupInstruction": None,
                                "pickupSlotsDate": (
                                    datetime.now() + timedelta(hours=1)
                                ).strftime("%d-%m-%Y %H:%M:%S"),
                            },
                            "packageDetails": {
                                "packageDimension": {
                                    "length": {
                                        "value": float(order.length),
                                        "unit": "cm",
                                    },
                                    "width": {
                                        "value": float(order.breadth),
                                        "unit": "cm",
                                    },
                                    "height": {
                                        "value": float(order.height),
                                        "unit": "cm",
                                    },
                                },
                                "packageWeight": {
                                    "physicalWeight": {
                                        "value": float(order.weight),
                                        "unit": "Kg",
                                    },
                                    "volumetricWeight": {
                                        "value": float(order.volumetric_weight),
                                        "unit": "Kg",
                                    },
                                    "billableWeight": {
                                        "value": float(order.applicable_weight),
                                        "unit": "Kg",
                                    },
                                },
                            },
                            "invoiceDetails": [
                                {
                                    "invoiceNumber": "",
                                    "invoiceDate": "",
                                    "invoiceValue": float(order.total_amount),
                                    "ebnExpDate": "",
                                    "ebnNumber": "",
                                    "billFrom": {
                                        "customerDetails": {
                                            "countryType": "ISO2",
                                            "type": "PRIMARY",
                                            "country": "Ind",
                                            "name": "seller",
                                            "addressLine": pickup_location.address,
                                            "pincode": pickup_location.pincode,
                                            "stateCountry": pickup_location.state,
                                            "city": pickup_location.city,
                                        },
                                        "contactDetails": {"type": "PRIMARY"},
                                        "tinNumber": {"taxIdentificationNumber": ""},
                                    },
                                    "billTo": {
                                        "customerDetails": {
                                            "countryType": "ISO2",
                                            "type": "PRIMARY",
                                            "name": order.consignee_full_name,
                                            "addressLine": clean_text(
                                                order.consignee_address
                                            ),
                                            "country": "Ind",
                                        },
                                        "contactDetails": {
                                            "emailid": order.consignee_email,
                                            "type": "Primary",
                                            "contactNumber": order.consignee_phone,
                                            "virtualNumber": None,
                                        },
                                        "tinNumber": {"taxIdentificationNumber": ""},
                                    },
                                    "productDetails": [
                                        {
                                            "productUniqueId": product["sku_code"],
                                            "productName": product["name"],
                                            "productValue": product["unit_price"],
                                            "productDescription": "",
                                            "productCategory": "",
                                            "productQuantity": product["quantity"],
                                            "tax": [],
                                            "hsnCode": "",
                                            "preTaxValue": "",
                                            "discount": 0,
                                        }
                                        for product in order.products
                                    ],
                                }
                            ],
                        }
                    ],
                },
                "shippingDetails": {
                    "dropDetails": {
                        "address": [
                            {
                                "country": "Ind",
                                "countryType": "ISO2",
                                "name": order.consignee_full_name,
                                "addressLine": clean_text(order.consignee_address),
                                "city": order.consignee_city,
                                "stateCountry": order.consignee_state,
                                "landmark": clean_text(order.consignee_landmark),
                                "pincode": order.consignee_pincode,
                                "type": "PRIMARY",
                            }
                        ],
                        "contactDetails": [
                            {
                                "emailid": order.consignee_email,
                                "type": "Primary",
                                "contactNumber": order.consignee_phone,
                                "virtualNumber": None,
                            }
                        ],
                        "geoFencingInstruction": {
                            "latitude": None,
                            "longitude": None,
                            "isGeoFencingEnabled": None,
                        },
                        "securityInstructions": {"securityCode": None},
                    },
                    "pickupDetails": {
                        "address": [
                            {
                                "country": "Ind",
                                "countryType": "ISO2",
                                "name": pickup_location.contact_person_name,
                                "addressLine": clean_text(pickup_location.address),
                                "city": pickup_location.city,
                                "stateCountry": pickup_location.state,
                                "landmark": clean_text(pickup_location.landmark),
                                "pincode": pickup_location.pincode,
                                "type": "Primary",
                            }
                        ],
                        "contactDetails": [
                            {
                                "emailid": pickup_location.contact_person_email,
                                "type": "Primary",
                                "contactNumber": pickup_location.contact_person_phone,
                                "virtualNumber": "",
                            }
                        ],
                        "geoFencingInstruction": {
                            "latitude": None,
                            "longitude": None,
                            "isGeoFencingEnabled": None,
                        },
                        "securityInstructions": {"securityCode": None},
                    },
                    "RTODetails": {
                        "address": [
                            {
                                "country": "Ind",
                                "countryType": "ISO2",
                                "name": pickup_location.contact_person_name,
                                "addressLine": clean_text(pickup_location.address),
                                "city": pickup_location.city,
                                "stateCountry": pickup_location.state,
                                "landmark": clean_text(pickup_location.landmark),
                                "pincode": pickup_location.pincode,
                                "type": "Primary",
                            }
                        ],
                        "contactDetails": [
                            {
                                "contactNumberExt": 91,
                                "emailid": pickup_location.contact_person_email,
                                "type": "Primary",
                                "contactNumber": pickup_location.contact_person_phone,
                                "virtualNumber": "",
                            }
                        ],
                        "geoFencingInstruction": {
                            "isGeoFencingEnabled": None,
                            "latitude": 0,
                            "longitude": 0,
                        },
                        "securityInstructions": {
                            "isGenSecurityCode": False,
                            "securityCode": 0,
                        },
                    },
                },
            }

            # print(body)

            print(2)

            api_url = Xpressbees.create_order_url
            headers = {"token": token["token"], "Content-Type": "application/json"}
            response = requests.request("POST", api_url, headers=headers, json=body)

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

            # If order creation failed at Xpressbees, return message
            if response_data["code"] != 100:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["message"],
                )

            # if order created successfully at Xpressbees

            print(4)

            if response_data["code"] == 100:

                print(5)
                # update status
                order.status = "booked"
                order.sub_status = "booked"
                order.courier_status = "BOOKED"

                order.awb_number = response_data["data"][0]["AWBNo"]
                order.aggregator = "xpressbees"
                order.shipping_partner_order_id = response_data["data"][0][
                    "TokenNumber"
                ]
                order.courier_partner = delivery_partner.slug

                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + str("xpressbees"),
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                }

                # update the activity
                print(6)

                order.action_history.append(new_activity)
                db.add(order)
                db.commit()

                print(7)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["data"][0]["AWBNo"],
                        "delivery_partner": "xpressbees",
                    },
                    message="AWB assigned successfully",
                )

            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg="Xpressbees Error is status is not OK: {}".format(
                        str(response_data)
                    ),
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["data"],
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Xpressbees Error posting shipment: {}".format(str(e)),
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
                msg="Xpressbees Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    # @staticmethod
    # def dev_create_order(
    #     order: Order_Model,
    #     credentials: Dict[str, str],
    #     delivery_partner: AggregatorCourierModel,
    # ):
    #     try:
    #         print(
    #             "welcome to express beex dev function",
    #             jsonable_encoder(delivery_partner),
    #         )
    #         client_id = context_user_data.get().client_id
    #         db = get_db_session()
    #         pickup_location = (
    #             db.query(Pickup_Location)
    #             .filter(Pickup_Location.location_code == order.pickup_location_code)
    #             .first()
    #         )
    #         token = Xpressbees.dev_Generate_Token()
    #         body = {
    #             "clientDetails": {
    #                 "clientId": "6295",
    #                 "clientName": "Genex",
    #                 "pickupVendorCode": 9,
    #                 "clientWarehouseId": "123",
    #             },
    #             "serviceDetails": {
    #                 "serviceName": "Forward",
    #                 "serviceMode": "SURFACE",
    #                 "serviceVertical": "Ecom",
    #                 "serviceType": "SD",
    #             },
    #             "shipmentDetails": {
    #                 "awbMpsGroupId": "",
    #                 "packageType": "packageType",
    #                 "orderType": "pre",
    #                 "partialRTOAllowed": False,
    #                 "allowPartialPickup": False,
    #                 "packageQuantity": {"value": 1, "unit": "Pck"},
    #                 "totalWeight": {"value": 10, "unit": "kg"},
    #                 "orderDetails": [
    #                     {
    #                         "orderNumber": "324324wdsfsd43538",
    #                         "awbNumber": "",
    #                         "subOrderNumber": "subOrderNumber",
    #                         "customerPromiseDate": "24-11-2025 10:14:13",
    #                         "manifestId": "manifestId",
    #                         "collectableAmount": {"unit": "INR", "value": 299},
    #                         "declaredAmount": {"value": 51000, "unit": "INR"},
    #                         "helpContent": {
    #                             "senderName": None,
    #                             "isOpenDelivery": None,
    #                             "isCommercialProperty": None,
    #                             "isDGShipmentType": None,
    #                         },
    #                         "pickUpInstruction": {
    #                             "pickupType": "Vendor",
    #                             "priorityRemarks": "PRIORITY_REMARKS",
    #                             "isPickupPriority": "1",
    #                             "pickupInstruction": None,
    #                             "pickupSlotsDate": "24-11-2025 10:14:13",
    #                         },
    #                         "packageDetails": {
    #                             "packageDimension": {
    #                                 "length": {"value": 0, "unit": "m"},
    #                                 "width": {"value": 0, "unit": "m"},
    #                                 "height": {"value": 0, "unit": "m"},
    #                             },
    #                             "packageWeight": {
    #                                 "physicalWeight": {"value": 0, "unit": "Kg"},
    #                                 "volumetricWeight": {"value": 0, "unit": "Kg"},
    #                                 "billableWeight": {"value": 0, "unit": "Kg"},
    #                             },
    #                         },
    #                         "invoiceDetails": [
    #                             {
    #                                 "invoiceNumber": "",
    #                                 "invoiceDate": "26-04-2023 10:16:13",
    #                                 "invoiceValue": 51000,
    #                                 "ebnExpDate": "26-04-2023 12:12:12",
    #                                 "ebnNumber": "123123",
    #                                 "billFrom": {
    #                                     "customerDetails": {
    #                                         "countryType": "ISO2",
    #                                         "type": "PRIMARY",
    #                                         "country": "Ind",
    #                                         "name": "seller",
    #                                         "addressLine": "billFromAddressline0",
    #                                         "pincode": 110075,
    #                                         "stateCountry": "maharastra",
    #                                         "city": None,
    #                                     },
    #                                     "contactDetails": {"type": "PRIMARY"},
    #                                     "tinNumber": {
    #                                         "taxIdentificationNumber": "27DKKPS2852A1ZM"
    #                                     },
    #                                 },
    #                                 "billTo": {
    #                                     "customerDetails": {
    #                                         "countryType": "ISO2",
    #                                         "type": "PRIMARY",
    #                                         "name": "",
    #                                         "addressLine": "",
    #                                         "country": "Ind",
    #                                     },
    #                                     "contactDetails": {
    #                                         "emailid": "billToName0@gmail.com",
    #                                         "type": "Primary",
    #                                         "contactNumber": "",
    #                                         "virtualNumber": None,
    #                                     },
    #                                     "tinNumber": {
    #                                         "taxIdentificationNumber": "27DKKPS2852A1ZM"
    #                                     },
    #                                 },
    #                                 "productDetails": [
    #                                     {
    #                                         "productUniqueId": "",
    #                                         "productName": "PRODUCT_NAME",
    #                                         "productValue": "productValue",
    #                                         "productDescription": "Tops and T-shirts",
    #                                         "productCategory": "Clothes & Shoes",
    #                                         "productQuantity": "1",
    #                                         "tax": [
    #                                             {
    #                                                 "taxType": "CGST1",
    #                                                 "taxValue": 0,
    #                                                 "taxPercentage": 0,
    #                                             },
    #                                             {
    #                                                 "taxType": "IGST",
    #                                                 "taxValue": 10,
    #                                                 "taxPercentage": 0,
    #                                             },
    #                                             {
    #                                                 "taxType": "SGST1",
    #                                                 "taxValue": 0,
    #                                                 "taxPercentage": 0,
    #                                             },
    #                                         ],
    #                                         "hsnCode": "",
    #                                         "preTaxValue": 10,
    #                                         "discount": 0,
    #                                         "qcDetails": {
    #                                             "isQualityCheck": True,
    #                                             "qcTemplateDetails": {
    #                                                 "templateId": None,
    #                                                 "templateCategory": None,
    #                                             },
    #                                             "textCapture": [
    #                                                 {
    #                                                     "label": None,
    #                                                     "type": None,
    #                                                     "valueToCheck": None,
    #                                                 }
    #                                             ],
    #                                             "pickupProductImage": [
    #                                                 {
    #                                                     "ImageUrl": "http://cdn.fc/box/11521166a.jpg",
    #                                                     "TextToShow": "Front Image",
    #                                                 }
    #                                             ],
    #                                             "captureImageRule": {
    #                                                 "minImage": 0,
    #                                                 "maxImage": 0,
    #                                             },
    #                                             "nonQcRVPType": "OpenBox1",
    #                                         },
    #                                     },
    #                                     {
    #                                         "productUniqueId": "productUniqueId2",
    #                                         "productName": "PRODUCT_NAME2",
    #                                         "productValue": "productValue2",
    #                                         "productDescription": "Tops and T-shirts2",
    #                                         "productCategory": "Clothes & Shoes2",
    #                                         "productQuantity": "1",
    #                                         "tax": [
    #                                             {
    #                                                 "taxType": "CGST1",
    #                                                 "taxValue": 100,
    #                                                 "taxPercentage": 12,
    #                                             },
    #                                             {
    #                                                 "taxType": "IGST",
    #                                                 "taxValue": 200,
    #                                                 "taxPercentage": 15,
    #                                             },
    #                                             {
    #                                                 "taxType": "SGST",
    #                                                 "taxValue": 100,
    #                                                 "taxPercentage": 10,
    #                                             },
    #                                         ],
    #                                         "hsnCode": "61091002",
    #                                         "preTaxValue": 102,
    #                                         "discount": 20,
    #                                         "qcDetails": None,
    #                                     },
    #                                 ],
    #                             }
    #                         ],
    #                     }
    #                 ],
    #             },
    #             "bufferAttribute": [],
    #             "shippingDetails": {
    #                 "dropDetails": {
    #                     "address": [
    #                         {
    #                             "country": "Ind",
    #                             "countryType": "ISO2",
    #                             "name": "dropname0",
    #                             "addressLine": "dropaddressLine0",
    #                             "city": "LUHARI",
    #                             "stateCountry": "HARYANA",
    #                             "landmark": "",
    #                             "pincode": "110075",
    #                             "type": "PRIMARY",
    #                         }
    #                     ],
    #                     "contactDetails": [
    #                         {
    #                             "emailid": "",
    #                             "type": "Primary",
    #                             "contactNumber": "1234567890",
    #                             "virtualNumber": None,
    #                         },
    #                         {
    #                             "emailid": "dropSecondary1@gmail.com",
    #                             "type": "Secondary",
    #                             "contactNumber": "1234567891",
    #                             "virtualNumber": None,
    #                         },
    #                     ],
    #                     "geoFencingInstruction": {
    #                         "latitude": None,
    #                         "longitude": None,
    #                         "isGeoFencingEnabled": None,
    #                     },
    #                     "securityInstructions": {"securityCode": None},
    #                 },
    #                 "pickupDetails": {
    #                     "address": [
    #                         {
    #                             "country": "Ind",
    #                             "countryType": "ISO2",
    #                             "name": "Shazli",
    #                             "addressLine": "c48b0950f3bef",
    #                             "city": "Kanpur Nagar",
    #                             "stateCountry": "Uttar Pradesh",
    #                             "landmark": "",
    #                             "pincode": "110001",
    #                             "type": "Primary",
    #                         },
    #                         {
    #                             "country": "Ind",
    #                             "countryType": "ISO2",
    #                             "name": "Shazli2",
    #                             "addressLine": "c48b0950fc085ef",
    #                             "city": "Kanpur Nagar",
    #                             "stateCountry": "Uttar Pradesh",
    #                             "landmark": "",
    #                             "pincode": "110075",
    #                             "type": None,
    #                         },
    #                     ],
    #                     "contactDetails": [
    #                         {
    #                             "emailid": "",
    #                             "type": "Primary",
    #                             "contactNumber": "1234567890",
    #                             "virtualNumber": "",
    #                         }
    #                     ],
    #                     "geoFencingInstruction": {
    #                         "latitude": None,
    #                         "longitude": None,
    #                         "isGeoFencingEnabled": None,
    #                     },
    #                     "securityInstructions": {"securityCode": None},
    #                 },
    #                 "RTODetails": {
    #                     "address": [
    #                         {
    #                             "name": "RAKSHIT GOYAL",
    #                             "addressLine": "SCO 261 First Floor BASKET",
    #                             "landmark": "string",
    #                             "city": "PANCHKULA",
    #                             "stateCountry": "Tamil Nadu",
    #                             "pincode": 110075,
    #                             "countryType": "ISO2",
    #                             "country": "ind",
    #                             "type": "primary",
    #                         }
    #                     ],
    #                     "contactDetails": [
    #                         {
    #                             "contactNumberExt": 91,
    #                             "contactNumber": 9465637062,
    #                             "virtualNumber": 0,
    #                             "emailid": "e@gmail.com",
    #                             "type": "primary",
    #                         }
    #                     ],
    #                     "customerTinDetails": {
    #                         "taxIdentificationNumber": 1230,
    #                         "taxIdentificationNumberType": "PERSONAL_NATIONAL",
    #                         "usage": 0,
    #                         "effictiveDate": None,
    #                         "expirationDate": None,
    #                     },
    #                     "geoFencingInstruction": {
    #                         "isGeoFencingEnabled": None,
    #                         "latitude": 0,
    #                         "longitude": 0,
    #                     },
    #                     "securityInstructions": {
    #                         "isGenSecurityCode": False,
    #                         "securityCode": 0,
    #                     },
    #                 },
    #             },
    #         }
    #         # print(body)
    #         # print(2)
    #         api_url = "https://stage-global-api.xbees.in/global/v1/serviceRequest"
    #         headers = {"token": token["token"], "Content-Type": "application/json"}
    #         response = requests.request("POST", api_url, headers=headers, json=body)
    #         # print(3)
    #         try:
    #             response_data = response.json()
    #             print(response_data, "Expressbees")
    #         except ValueError as e:
    #             logger.error("Failed to parse JSON response: %s", e)
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #                 message="Some error occurred while assigning AWB, please try again",
    #             )
    #         # If order creation failed at Xpressbees, return message
    #         if response_data["code"] != 100:
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.BAD_REQUEST,
    #                 message=response_data["message"],
    #             )

    #         # if order created successfully at Xpressbees

    #         print(
    #             response_data["code"],
    #             "<<Code section>>",
    #             jsonable_encoder(delivery_partner),
    #         )

    #         if response_data["code"] == 100:

    #             # print(5)
    #             # update status
    #             order.status = "booked"
    #             order.sub_status = "booked"
    #             order.courier_status = "BOOKED"

    #             order.awb_number = response_data["data"][0]["AWBNo"]
    #             order.aggregator = "xpressbees"
    #             order.shipping_partner_order_id = response_data["data"][0][
    #                 "TokenNumber"
    #             ]
    #             order.courier_partner = delivery_partner.slug

    #             new_activity = {
    #                 "event": "Shipment Created",
    #                 "subinfo": "delivery partner - " + str("xpressbees"),
    #                 "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
    #             }

    #             # update the activity
    #             # print(6)

    #             order.action_history.append(new_activity)
    #             db.add(order)
    #             db.commit()

    #             print("007")

    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.OK,
    #                 status=True,
    #                 data={
    #                     "awb_number": response_data["data"][0]["AWBNo"],
    #                     "delivery_partner": "xpressbees",
    #                 },
    #                 message="AWB assigned successfully",
    #             )

    #         else:
    #             logger.error(
    #                 extra=context_user_data.get(),
    #                 msg="Xpressbees Error is status is not OK: {}".format(
    #                     str(response_data)
    #                 ),
    #             )
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.BAD_REQUEST,
    #                 message=response_data["data"],
    #             )
    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Xpressbees Error posting shipment: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="An error occurred while posting the shipment.",
    #         )

    #     except Exception as e:
    #         # Log other unhandled exceptions
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Xpressbees Unhandled error: {}".format(str(e)),
    #         )
    #         # Return a general internal server error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="An internal server error occurred. Please try again later.",
    #         )

    @staticmethod
    async def dev_create_order(order, credentials, delivery_partner):
        try:
            client_id = context_user_data.get().client_id
            async with get_db_session() as db:

                pickup_location = await db.execute(
                    select(Pickup_Location).filter(
                        Pickup_Location.location_code == order.pickup_location_code
                    )
                )
                pickup_location = pickup_location.scalar_one_or_none()

                token = await Xpressbees.dev_Generate_Token()

                body = {
                    "clientDetails": {
                        "clientId": "6295",
                        "clientName": "Genex",
                        "pickupVendorCode": 9,
                        "clientWarehouseId": "123",
                    },
                    "serviceDetails": {
                        "serviceName": "Forward",
                        "serviceMode": "SURFACE",
                        "serviceVertical": "Ecom",
                        "serviceType": "SD",
                    },
                    "shipmentDetails": {
                        "awbMpsGroupId": "",
                        "packageType": "packageType",
                        "orderType": "pre",
                        "partialRTOAllowed": False,
                        "allowPartialPickup": False,
                        "packageQuantity": {"value": 1, "unit": "Pck"},
                        "totalWeight": {"value": 10, "unit": "kg"},
                        "orderDetails": [
                            {
                                "orderNumber": "324324wdsfsd43538",
                                "awbNumber": "",
                                "subOrderNumber": "subOrderNumber",
                                "customerPromiseDate": "24-11-2025 10:14:13",
                                "manifestId": "manifestId",
                                "collectableAmount": {"unit": "INR", "value": 299},
                                "declaredAmount": {"value": 51000, "unit": "INR"},
                                "helpContent": {
                                    "senderName": None,
                                    "isOpenDelivery": None,
                                    "isCommercialProperty": None,
                                    "isDGShipmentType": None,
                                },
                                "pickUpInstruction": {
                                    "pickupType": "Vendor",
                                    "priorityRemarks": "PRIORITY_REMARKS",
                                    "isPickupPriority": "1",
                                    "pickupInstruction": None,
                                    "pickupSlotsDate": "24-11-2025 10:14:13",
                                },
                                "packageDetails": {
                                    "packageDimension": {
                                        "length": {"value": 0, "unit": "m"},
                                        "width": {"value": 0, "unit": "m"},
                                        "height": {"value": 0, "unit": "m"},
                                    },
                                    "packageWeight": {
                                        "physicalWeight": {"value": 0, "unit": "Kg"},
                                        "volumetricWeight": {"value": 0, "unit": "Kg"},
                                        "billableWeight": {"value": 0, "unit": "Kg"},
                                    },
                                },
                                "invoiceDetails": [
                                    {
                                        "invoiceNumber": "",
                                        "invoiceDate": "26-04-2023 10:16:13",
                                        "invoiceValue": 51000,
                                        "ebnExpDate": "26-04-2023 12:12:12",
                                        "ebnNumber": "123123",
                                        "billFrom": {
                                            "customerDetails": {
                                                "countryType": "ISO2",
                                                "type": "PRIMARY",
                                                "country": "Ind",
                                                "name": "seller",
                                                "addressLine": "billFromAddressline0",
                                                "pincode": 110075,
                                                "stateCountry": "maharastra",
                                                "city": None,
                                            },
                                            "contactDetails": {"type": "PRIMARY"},
                                            "tinNumber": {
                                                "taxIdentificationNumber": "27DKKPS2852A1ZM"
                                            },
                                        },
                                        "billTo": {
                                            "customerDetails": {
                                                "countryType": "ISO2",
                                                "type": "PRIMARY",
                                                "name": "",
                                                "addressLine": "",
                                                "country": "Ind",
                                            },
                                            "contactDetails": {
                                                "emailid": "billToName0@gmail.com",
                                                "type": "Primary",
                                                "contactNumber": "",
                                                "virtualNumber": None,
                                            },
                                            "tinNumber": {
                                                "taxIdentificationNumber": "27DKKPS2852A1ZM"
                                            },
                                        },
                                        "productDetails": [
                                            {
                                                "productUniqueId": "",
                                                "productName": "PRODUCT_NAME",
                                                "productValue": "productValue",
                                                "productDescription": "Tops and T-shirts",
                                                "productCategory": "Clothes & Shoes",
                                                "productQuantity": "1",
                                                "tax": [
                                                    {
                                                        "taxType": "CGST1",
                                                        "taxValue": 0,
                                                        "taxPercentage": 0,
                                                    },
                                                    {
                                                        "taxType": "IGST",
                                                        "taxValue": 10,
                                                        "taxPercentage": 0,
                                                    },
                                                    {
                                                        "taxType": "SGST1",
                                                        "taxValue": 0,
                                                        "taxPercentage": 0,
                                                    },
                                                ],
                                                "hsnCode": "",
                                                "preTaxValue": 10,
                                                "discount": 0,
                                                "qcDetails": {
                                                    "isQualityCheck": True,
                                                    "qcTemplateDetails": {
                                                        "templateId": None,
                                                        "templateCategory": None,
                                                    },
                                                    "textCapture": [
                                                        {
                                                            "label": None,
                                                            "type": None,
                                                            "valueToCheck": None,
                                                        }
                                                    ],
                                                    "pickupProductImage": [
                                                        {
                                                            "ImageUrl": "http://cdn.fc/box/11521166a.jpg",
                                                            "TextToShow": "Front Image",
                                                        }
                                                    ],
                                                    "captureImageRule": {
                                                        "minImage": 0,
                                                        "maxImage": 0,
                                                    },
                                                    "nonQcRVPType": "OpenBox1",
                                                },
                                            },
                                            {
                                                "productUniqueId": "productUniqueId2",
                                                "productName": "PRODUCT_NAME2",
                                                "productValue": "productValue2",
                                                "productDescription": "Tops and T-shirts2",
                                                "productCategory": "Clothes & Shoes2",
                                                "productQuantity": "1",
                                                "tax": [
                                                    {
                                                        "taxType": "CGST1",
                                                        "taxValue": 100,
                                                        "taxPercentage": 12,
                                                    },
                                                    {
                                                        "taxType": "IGST",
                                                        "taxValue": 200,
                                                        "taxPercentage": 15,
                                                    },
                                                    {
                                                        "taxType": "SGST",
                                                        "taxValue": 100,
                                                        "taxPercentage": 10,
                                                    },
                                                ],
                                                "hsnCode": "61091002",
                                                "preTaxValue": 102,
                                                "discount": 20,
                                                "qcDetails": None,
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    },
                    "bufferAttribute": [],
                    "shippingDetails": {
                        "dropDetails": {
                            "address": [
                                {
                                    "country": "Ind",
                                    "countryType": "ISO2",
                                    "name": "dropname0",
                                    "addressLine": "dropaddressLine0",
                                    "city": "LUHARI",
                                    "stateCountry": "HARYANA",
                                    "landmark": "",
                                    "pincode": "110075",
                                    "type": "PRIMARY",
                                }
                            ],
                            "contactDetails": [
                                {
                                    "emailid": "",
                                    "type": "Primary",
                                    "contactNumber": "1234567890",
                                    "virtualNumber": None,
                                },
                                {
                                    "emailid": "dropSecondary1@gmail.com",
                                    "type": "Secondary",
                                    "contactNumber": "1234567891",
                                    "virtualNumber": None,
                                },
                            ],
                            "geoFencingInstruction": {
                                "latitude": None,
                                "longitude": None,
                                "isGeoFencingEnabled": None,
                            },
                            "securityInstructions": {"securityCode": None},
                        },
                        "pickupDetails": {
                            "address": [
                                {
                                    "country": "Ind",
                                    "countryType": "ISO2",
                                    "name": "Shazli",
                                    "addressLine": "c48b0950f3bef",
                                    "city": "Kanpur Nagar",
                                    "stateCountry": "Uttar Pradesh",
                                    "landmark": "",
                                    "pincode": "110001",
                                    "type": "Primary",
                                },
                                {
                                    "country": "Ind",
                                    "countryType": "ISO2",
                                    "name": "Shazli2",
                                    "addressLine": "c48b0950fc085ef",
                                    "city": "Kanpur Nagar",
                                    "stateCountry": "Uttar Pradesh",
                                    "landmark": "",
                                    "pincode": "110075",
                                    "type": None,
                                },
                            ],
                            "contactDetails": [
                                {
                                    "emailid": "",
                                    "type": "Primary",
                                    "contactNumber": "1234567890",
                                    "virtualNumber": "",
                                }
                            ],
                            "geoFencingInstruction": {
                                "latitude": None,
                                "longitude": None,
                                "isGeoFencingEnabled": None,
                            },
                            "securityInstructions": {"securityCode": None},
                        },
                        "RTODetails": {
                            "address": [
                                {
                                    "name": "RAKSHIT GOYAL",
                                    "addressLine": "SCO 261 First Floor BASKET",
                                    "landmark": "string",
                                    "city": "PANCHKULA",
                                    "stateCountry": "Tamil Nadu",
                                    "pincode": 110075,
                                    "countryType": "ISO2",
                                    "country": "ind",
                                    "type": "primary",
                                }
                            ],
                            "contactDetails": [
                                {
                                    "contactNumberExt": 91,
                                    "contactNumber": 9465637062,
                                    "virtualNumber": 0,
                                    "emailid": "e@gmail.com",
                                    "type": "primary",
                                }
                            ],
                            "customerTinDetails": {
                                "taxIdentificationNumber": 1230,
                                "taxIdentificationNumberType": "PERSONAL_NATIONAL",
                                "usage": 0,
                                "effictiveDate": None,
                                "expirationDate": None,
                            },
                            "geoFencingInstruction": {
                                "isGeoFencingEnabled": None,
                                "latitude": 0,
                                "longitude": 0,
                            },
                            "securityInstructions": {
                                "isGenSecurityCode": False,
                                "securityCode": 0,
                            },
                        },
                    },
                }
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        "https://stage-global-api.xbees.in/global/v1/serviceRequest",
                        headers={
                            "token": token["token"],
                            "Content-Type": "application/json",
                        },
                        json=body,
                    )
                print("123***", response.json())

                response_data = response.json()
                print("124***")
                if response_data["code"] != 100:
                    print("1245***")
                    print("error in dev_create_order")
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=response_data["message"],
                    )
                print("1246***")
                order.status = "booked"
                order.sub_status = "booked"
                order.courier_status = "BOOKED"
                print("234***")

                order.awb_number = response_data["data"][0]["AWBNo"]
                order.aggregator = "xpressbees"
                order.shipping_partner_order_id = response_data["data"][0][
                    "TokenNumber"
                ]
                print("235***")
                order.courier_partner = delivery_partner.slug
                print("236***")
                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + str("xpressbees"),
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                }

                # update the activity
                # print(6)

                order.action_history.append(new_activity)
                db.add(order)

                db.add(order)
                await db.commit()
                print("456***")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["data"][0]["AWBNo"],
                        "delivery_partner": "xpressbees",
                    },
                    message="AWB assigned successfully",
                )
        finally:
            await db.close()

    @staticmethod
    def cancel_shipment(
        order: Order_Model,
        awb_number: str,
        # credentials: Dict[str, str],
    ):
        try:
            print("xbess cancel api")

            if (
                order.courier_partner == "xpressbees"
                or order.courier_partner == "xpressbees 1kg"
                or order.courier_partner == "xpressbees 2kg"
            ):

                print("1")

                token = Xpressbees.Generate_Token(
                    {
                        "username": Xpressbees.username,
                        "password": Xpressbees.password,
                        "secretkey": Xpressbees.secretkey,
                    }
                )

            else:
                token = Xpressbees.Generate_Token(
                    {
                        "username": "admin@gnxhvy.com",
                        "password": "$gnxhvy$",
                        "secretkey": "5d7bb48c5868088f3b6f7a500d61ea893c4206ac9f517d85cffecefe67b7da71",
                    }
                )

            if "error" in token:
                logger.error(
                    "XPRESSBEES generate_token token error: %s", token["error"]
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=token["error"],
                )
            else:
                print(0)
                headers = {
                    "token": token["token"],
                    "Content-Type": "application/json",
                }
                api_url = Xpressbees.cancellation_order_url
                print(1)
                body = json.dumps(
                    {"ShippingID": awb_number, "CancellationReason": "Cancel Order"}
                )  # credentials["customer_code"]

                response = requests.request("POST", api_url, headers=headers, data=body)
                print(2)

                try:
                    response_data = response.json()
                    print(response_data)
                    print(3)
                except ValueError as e:
                    logger.error(
                        "XPRESSBEES cancel_shipment Failed to parse JSON response: %s",
                        e,
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        status=False,
                        message="Could not cancel shipment",
                    )
                # If tracking failed, return message
                if response_data["ReturnCode"] != 100:
                    print(4)
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        status=False,
                        message="Could not cancel shipment",
                    )
                print(5)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="order cancelled successfully",
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
    def track_shipment(order: Order_Model, awb_number: str):
        try:

            if (
                order.courier_partner == "xpressbees"
                or order.courier_partner == "xpressbees 2kg"
            ):

                print("1")

                token = Xpressbees.Generate_Token(
                    {
                        "username": Xpressbees.username,
                        "password": Xpressbees.password,
                        "secretkey": Xpressbees.secretkey,
                    }
                )

            elif order.courier_partner == "xpressbees 1kg":
                token = Xpressbees.Generate_Token(
                    {
                        "username": "admin@Genex1Kg.com",
                        "password": "Xpress@1234567",
                        "secretkey": "1f6a36baa1b787b29b113e235cdc1b6faf86d37d39003af72299dd7deb03d750",
                    }
                )

            else:
                token = Xpressbees.Generate_Token(
                    {
                        "username": "admin@gnxhvy.com",
                        "password": "$gnxhvy$",
                        "secretkey": "5d7bb48c5868088f3b6f7a500d61ea893c4206ac9f517d85cffecefe67b7da71",
                    }
                )

            if "error" in token:
                logger.error(
                    "XPRESSBEES generate_token token error: %s", token["error"]
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=token["error"],
                )
            else:
                logger.info("XPRESSBEES track_shipment token: %s", token)

                api_url = Xpressbees.track_order_url

                headers = {
                    "token": token["token"],
                    "Content-Type": "application/json",
                    "versionnumber": "v1",
                }
                payload = json.dumps({"AWBNumber": awb_number})  # X09384961

                logger.info(
                    "XPRESSBEES track_shipment payload ready to post: %s", payload
                )

                response = requests.request(
                    "POST", api_url, headers=headers, data=payload
                )

                try:
                    response_data = response.json()
                    print(1)
                    print(response_data)
                except ValueError as e:
                    logger.error("XPRESSBEES Failed to parse JSON response: %s", e)
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Some error occurred while tracking, please try again",
                    )

                # print("2", response_data)

                # If tracking failed, return message
                if response_data["ReturnCode"] != 100:
                    print("3")
                    logger.error(
                        "XPRESSBEES track_shipment if response is not true: %s",
                        response_data,
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=response_data,
                    )

                tracking_data = response_data.get("ShipmentLogDetails", "")
                logger.info(
                    "XPRESSBEES track_shipment api response: %s",
                    tracking_data,
                )
                print(4)

                courier_status = tracking_data[0]["ShipmentStatus"]
                updated_awb_number = ""
                activites = tracking_data
                print(5)

                db = get_db_session()

                old_status = order.status
                old_sub_status = order.sub_status
                # update the order status, and awb if different

                order.status = (
                    status_mapping[courier_status].get("status", None) or old_status
                )
                order.sub_status = (
                    status_mapping[courier_status].get("sub_status", None)
                    or old_sub_status
                )
                print(6)
                order.courier_status = (
                    status_mapping[courier_status]
                    .get("status", "")
                    .replace(" ", "_")
                    .upper()
                )
                # print(order.courier_status, "seven")
                order.awb_number = (
                    updated_awb_number if updated_awb_number else order.awb_number
                )

                # update the tracking info
                if activites:
                    new_tracking_info = [
                        {
                            "status": status_mapping.get(
                                activity.get("ShipmentStatus", ""), {}
                            ).get(
                                "sub_status", activity.get("ShipmentStatus", "").strip()
                            ),
                            "description": activity.get("Description", ""),
                            "subinfo": "",
                            "datetime": parse_datetime(
                                activity.get("ShipmentStatusDateTime", "")
                            ).strftime("%d-%m-%Y %H:%M:%S"),
                            "location": activity.get("City", ""),
                        }
                        for activity in activites
                    ]

                    # new_tracking_info.reverse()

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
    def tracking_webhook(track_req):

        try:

            from modules.shipment.shipment_service import ShipmentService

            print(track_req)

            db = get_db_session()

            awb_number = track_req.get("awb", None)

            if awb_number == None or awb_number == "" or not awb_number:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Tracking Successfull",
                )

            order = db.query(Order).filter(Order.awb_number == awb_number).first()

            if order is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Tracking Successfull",
                )

            courier_status = track_req.get("status_code")

            order.status = status_mapping[courier_status]["status"]
            order.sub_status = status_mapping[courier_status]["sub_status"]
            order.courier_status = courier_status

            new_tracking_info = {
                "status": status_mapping.get(courier_status, {}).get(
                    "status", courier_status
                ),
                "description": track_req.get("status", ""),
                "subinfo": track_req.get("status", ""),
                "datetime": parse_datetime(
                    track_req.get("status_timestamp", "")
                ).strftime("%d-%m-%Y %H:%M:%S"),
                "location": track_req["scans"][0]["location"],
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
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error in tracking",
            )

        finally:
            if db:
                db.close()

    @staticmethod
    def dev_create_reverse_order(
        order: Order_Model,
        credentials: Dict[str, str],
        delivery_partner: AggregatorCourierModel,
    ):
        try:
            print(1)
            # logger.info("Delhivery create_order token: %s", Delhivery.Token)

            client_id = context_user_data.get().client_id

            # get the location code for delhivery from the db

            db = get_db_session()

            pickup_location = (
                db.query(Pickup_Location)
                .filter(Pickup_Location.location_code == order.pickup_location_code)
                .first()
            )
            # get the delhivery location code from the db

            delhivery_pickup_location = pickup_location.courier_location_codes.get(
                "delhivery",
                None,
            )
            credentials = Xpressbees.dev_Generate_Token()

            # if no delhivery location code mapping is found for the current pickup location, create a new warehouse at delhivery
            if delhivery_pickup_location is None:

                delhivery_pickup_location = Xpressbees.create_pickup_location(
                    order.pickup_location_code, credentials, delivery_partner
                )

                # if could not create location at delhivery, throw error
                if delhivery_pickup_location.status == False:

                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Unable to place order. Please try again later",
                    )

                else:
                    delhivery_pickup_location = delhivery_pickup_location.data

            # print("location", delhivery_pickup_location)

            # return GenericResponseModel(
            #     status_code=http.HTTPStatus.BAD_REQUEST,
            #     message="test",
            # )

            credentials = Xpressbees.dev_Generate_Token()
            body = {
                "clientDetails": {
                    "clientId": "6295",
                    "clientName": "Genex",
                    "pickupVendorCode": 9,
                    "clientWarehouseId": "123",
                },
                "serviceDetails": {
                    "serviceName": "Forward",
                    "serviceMode": "SURFACE",
                    "serviceVertical": "Ecom",
                    "serviceType": "SD",
                },
                "shipmentDetails": {
                    "awbMpsGroupId": "",
                    "packageType": "packageType",
                    "orderType": "pre",
                    "partialRTOAllowed": False,
                    "allowPartialPickup": False,
                    "packageQuantity": {"value": 1, "unit": "Pck"},
                    "totalWeight": {"value": 10, "unit": "kg"},
                    "orderDetails": [
                        {
                            "orderNumber": "324324wdsfsd43538",
                            "awbNumber": "",
                            "subOrderNumber": "subOrderNumber",
                            "customerPromiseDate": "24-11-2025 10:14:13",
                            "manifestId": "manifestId",
                            "collectableAmount": {"unit": "INR", "value": 299},
                            "declaredAmount": {"value": 51000, "unit": "INR"},
                            "helpContent": {
                                "senderName": None,
                                "isOpenDelivery": None,
                                "isCommercialProperty": None,
                                "isDGShipmentType": None,
                            },
                            "pickUpInstruction": {
                                "pickupType": "Vendor",
                                "priorityRemarks": "PRIORITY_REMARKS",
                                "isPickupPriority": "1",
                                "pickupInstruction": None,
                                "pickupSlotsDate": "24-11-2025 10:14:13",
                            },
                            "packageDetails": {
                                "packageDimension": {
                                    "length": {"value": 0, "unit": "m"},
                                    "width": {"value": 0, "unit": "m"},
                                    "height": {"value": 0, "unit": "m"},
                                },
                                "packageWeight": {
                                    "physicalWeight": {"value": 0, "unit": "Kg"},
                                    "volumetricWeight": {"value": 0, "unit": "Kg"},
                                    "billableWeight": {"value": 0, "unit": "Kg"},
                                },
                            },
                            "invoiceDetails": [
                                {
                                    "invoiceNumber": "",
                                    "invoiceDate": "26-04-2023 10:16:13",
                                    "invoiceValue": 51000,
                                    "ebnExpDate": "26-04-2023 12:12:12",
                                    "ebnNumber": "123123",
                                    "billFrom": {
                                        "customerDetails": {
                                            "countryType": "ISO2",
                                            "type": "PRIMARY",
                                            "country": "Ind",
                                            "name": "seller",
                                            "addressLine": "billFromAddressline0",
                                            "pincode": 110075,
                                            "stateCountry": "maharastra",
                                            "city": None,
                                        },
                                        "contactDetails": {"type": "PRIMARY"},
                                        "tinNumber": {
                                            "taxIdentificationNumber": "27DKKPS2852A1ZM"
                                        },
                                    },
                                    "billTo": {
                                        "customerDetails": {
                                            "countryType": "ISO2",
                                            "type": "PRIMARY",
                                            "name": "",
                                            "addressLine": "",
                                            "country": "Ind",
                                        },
                                        "contactDetails": {
                                            "emailid": "billToName0@gmail.com",
                                            "type": "Primary",
                                            "contactNumber": "",
                                            "virtualNumber": None,
                                        },
                                        "tinNumber": {
                                            "taxIdentificationNumber": "27DKKPS2852A1ZM"
                                        },
                                    },
                                    "productDetails": [
                                        {
                                            "productUniqueId": "",
                                            "productName": "PRODUCT_NAME",
                                            "productValue": "productValue",
                                            "productDescription": "Tops and T-shirts",
                                            "productCategory": "Clothes & Shoes",
                                            "productQuantity": "1",
                                            "tax": [
                                                {
                                                    "taxType": "CGST1",
                                                    "taxValue": 0,
                                                    "taxPercentage": 0,
                                                },
                                                {
                                                    "taxType": "IGST",
                                                    "taxValue": 10,
                                                    "taxPercentage": 0,
                                                },
                                                {
                                                    "taxType": "SGST1",
                                                    "taxValue": 0,
                                                    "taxPercentage": 0,
                                                },
                                            ],
                                            "hsnCode": "",
                                            "preTaxValue": 10,
                                            "discount": 0,
                                            "qcDetails": {
                                                "isQualityCheck": True,
                                                "qcTemplateDetails": {
                                                    "templateId": None,
                                                    "templateCategory": None,
                                                },
                                                "textCapture": [
                                                    {
                                                        "label": None,
                                                        "type": None,
                                                        "valueToCheck": None,
                                                    }
                                                ],
                                                "pickupProductImage": [
                                                    {
                                                        "ImageUrl": "http://cdn.fc/box/11521166a.jpg",
                                                        "TextToShow": "Front Image",
                                                    }
                                                ],
                                                "captureImageRule": {
                                                    "minImage": 0,
                                                    "maxImage": 0,
                                                },
                                                "nonQcRVPType": "OpenBox1",
                                            },
                                        },
                                        {
                                            "productUniqueId": "productUniqueId2",
                                            "productName": "PRODUCT_NAME2",
                                            "productValue": "productValue2",
                                            "productDescription": "Tops and T-shirts2",
                                            "productCategory": "Clothes & Shoes2",
                                            "productQuantity": "1",
                                            "tax": [
                                                {
                                                    "taxType": "CGST1",
                                                    "taxValue": 100,
                                                    "taxPercentage": 12,
                                                },
                                                {
                                                    "taxType": "IGST",
                                                    "taxValue": 200,
                                                    "taxPercentage": 15,
                                                },
                                                {
                                                    "taxType": "SGST",
                                                    "taxValue": 100,
                                                    "taxPercentage": 10,
                                                },
                                            ],
                                            "hsnCode": "61091002",
                                            "preTaxValue": 102,
                                            "discount": 20,
                                            "qcDetails": None,
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                },
                "bufferAttribute": [],
                "shippingDetails": {
                    "dropDetails": {
                        "address": [
                            {
                                "country": "Ind",
                                "countryType": "ISO2",
                                "name": "dropname0",
                                "addressLine": "dropaddressLine0",
                                "city": "LUHARI",
                                "stateCountry": "HARYANA",
                                "landmark": "",
                                "pincode": "110075",
                                "type": "PRIMARY",
                            }
                        ],
                        "contactDetails": [
                            {
                                "emailid": "",
                                "type": "Primary",
                                "contactNumber": "1234567890",
                                "virtualNumber": None,
                            },
                            {
                                "emailid": "dropSecondary1@gmail.com",
                                "type": "Secondary",
                                "contactNumber": "1234567891",
                                "virtualNumber": None,
                            },
                        ],
                        "geoFencingInstruction": {
                            "latitude": None,
                            "longitude": None,
                            "isGeoFencingEnabled": None,
                        },
                        "securityInstructions": {"securityCode": None},
                    },
                    "pickupDetails": {
                        "address": [
                            {
                                "country": "Ind",
                                "countryType": "ISO2",
                                "name": "Shazli",
                                "addressLine": "c48b0950f3bef",
                                "city": "Kanpur Nagar",
                                "stateCountry": "Uttar Pradesh",
                                "landmark": "",
                                "pincode": "110001",
                                "type": "Primary",
                            },
                            {
                                "country": "Ind",
                                "countryType": "ISO2",
                                "name": "Shazli2",
                                "addressLine": "c48b0950fc085ef",
                                "city": "Kanpur Nagar",
                                "stateCountry": "Uttar Pradesh",
                                "landmark": "",
                                "pincode": "110075",
                                "type": None,
                            },
                        ],
                        "contactDetails": [
                            {
                                "emailid": "",
                                "type": "Primary",
                                "contactNumber": "1234567890",
                                "virtualNumber": "",
                            }
                        ],
                        "geoFencingInstruction": {
                            "latitude": None,
                            "longitude": None,
                            "isGeoFencingEnabled": None,
                        },
                        "securityInstructions": {"securityCode": None},
                    },
                    "RTODetails": {
                        "address": [
                            {
                                "name": "RAKSHIT GOYAL",
                                "addressLine": "SCO 261 First Floor BASKET",
                                "landmark": "string",
                                "city": "PANCHKULA",
                                "stateCountry": "Tamil Nadu",
                                "pincode": 110075,
                                "countryType": "ISO2",
                                "country": "ind",
                                "type": "primary",
                            }
                        ],
                        "contactDetails": [
                            {
                                "contactNumberExt": 91,
                                "contactNumber": 9465637062,
                                "virtualNumber": 0,
                                "emailid": "e@gmail.com",
                                "type": "primary",
                            }
                        ],
                        "customerTinDetails": {
                            "taxIdentificationNumber": 1230,
                            "taxIdentificationNumberType": "PERSONAL_NATIONAL",
                            "usage": 0,
                            "effictiveDate": None,
                            "expirationDate": None,
                        },
                        "geoFencingInstruction": {
                            "isGeoFencingEnabled": None,
                            "latitude": 0,
                            "longitude": 0,
                        },
                        "securityInstructions": {
                            "isGenSecurityCode": False,
                            "securityCode": 0,
                        },
                    },
                },
            }
            # print(body)
            # print(2)
            api_url = "https://stage-global-api.xbees.in/global/v1/serviceRequest"
            headers = {
                "token": credentials["token"],
                "Content-Type": "application/json",
            }
            response = requests.request("POST", api_url, headers=headers, json=body)
            # print(3)
            try:
                response_data = response.json()
                print(response_data, "Response")
            except ValueError as e:
                logger.error("Failed to parse JSON response: %s", e)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="Some error occurred while assigning AWB, please try again",
                )
            # If order creation failed at Xpressbees, return message
            if response_data["code"] != 100:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["message"],
                )
            if response_data["code"] == 100:
                # print(5)
                # update status
                order.status = "booked"
                order.sub_status = "booked"
                order.courier_status = "BOOKED"
                order.awb_number = response_data["data"][0]["AWBNo"]
                order.aggregator = "xpressbees"
                order.shipping_partner_order_id = response_data["data"][0][
                    "TokenNumber"
                ]
                order.courier_partner = delivery_partner.slug
                new_activity = {
                    "event": "Shipment Created",
                    "subinfo": "delivery partner - " + str("xpressbees"),
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                }
                order.action_history.append(new_activity)
                db.add(order)
                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "awb_number": response_data["data"][0]["AWBNo"],
                        "delivery_partner": "xpressbees",
                    },
                    message="AWB assigned successfully",
                )

            else:
                logger.error(
                    extra=context_user_data.get(),
                    msg="Xpressbees Error is status is not OK: {}".format(
                        str(response_data)
                    ),
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=response_data["data"],
                )
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Delhivery Error posting shipment: {}".format(str(e)),
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
                msg="Delhivery Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def ndr_action(
        order: Order_Model,
        awb_number: str,
        # credentials: Dict[str, str],
    ):
        try:
            print("xbess cancel api")

            if (
                order.courier_partner == "xpressbees"
                or order.courier_partner == "xpressbees 2kg"
            ):

                print("1")

                token = Xpressbees.Generate_Token(
                    {
                        "username": Xpressbees.username,
                        "password": Xpressbees.password,
                        "secretkey": Xpressbees.secretkey,
                    }
                )

            elif order.courier_partner == "xpressbees 1kg":
                token = Xpressbees.Generate_Token(
                    {
                        "username": "admin@Genex1Kg.com",
                        "password": "Xpress@1234567",
                        "secretkey": "1f6a36baa1b787b29b113e235cdc1b6faf86d37d39003af72299dd7deb03d750",
                    }
                )

            else:
                token = Xpressbees.Generate_Token(
                    {
                        "username": "admin@gnxhvy.com",
                        "password": "$gnxhvy$",
                        "secretkey": "5d7bb48c5868088f3b6f7a500d61ea893c4206ac9f517d85cffecefe67b7da71",
                    }
                )

            if "error" in token:
                logger.error(
                    "XPRESSBEES generate_token token error: %s", token["error"]
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=token["error"],
                )
            else:
                print(0)
                headers = {
                    "token": token["token"],
                    "Content-Type": "application/json",
                }
                api_url = Xpressbees.raise_ndr_url
                print(1)
                body = json.dumps(
                    {
                        "ShippingID": awb_number,
                        "DeferredDeliveryDate": (
                            datetime.now() + timedelta(hours=2)
                        ).strftime("%d-%m-%Y %H:%M:%S"),
                        "PrimaryCustomerMobileNumber": order.consignee_phone,
                        "PrimaryCustomerAddress": clean_text(order.consignee_address)
                        + clean_text(order.consignee_landmark or ""),
                        "CustomerPincode": order.consignee_pincode,
                        "Comments": "as per customer request",
                        "LastModifiedBy": "",
                    }
                )  # credentials["customer_code"]

                response = requests.request("POST", api_url, headers=headers, data=body)
                print(2)

                try:
                    response_data = response.json()
                    print(response_data)
                    print(3)
                except ValueError as e:
                    logger.error(
                        "XPRESSBEES cancel_shipment Failed to parse JSON response: %s",
                        e,
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        status=False,
                        message="Could not cancel shipment",
                    )
                # If tracking failed, return message
                if response_data["ReturnCode"] != 100:
                    print(4)
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        status=False,
                        message="Could not cancel shipment",
                    )
                print(5)
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="order cancelled successfully",
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
