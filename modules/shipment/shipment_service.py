import http
import io
import base64
import time
from sqlalchemy import extract
from fastapi import Depends
from datetime import datetime, timedelta, timezone
from sqlalchemy.dialects import postgresql  # or mysql / sqlite depending on your DB
from psycopg2 import DatabaseError
import time
from sqlalchemy.orm import joinedload
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, and_, func
import requests
from decimal import Decimal
import pytz
from pydantic import BaseModel
import math
import asyncio
import random

from context_manager.context import build_request_context

ist = pytz.timezone("Asia/Kolkata")


from xhtml2pdf import pisa

from context_manager.context import context_user_data, get_db_session

from logger import logger
from fastapi.encoders import jsonable_encoder

# schema
from schema.base import GenericResponseModel
from modules.shipment.shipment_schema import (
    CreateShipmentModel,
    ShippingChargesSchema,
    BulkCreateShipmentModel,
    AutoCreateShipmentModel,
    NewBulkCreateShipmentModel,
    ShippingChargesGetSchema,
)
from modules.orders.order_schema import Order_Model, Single_Order_Response_Model

# models
from models import (
    Pincode_Mapping,
    Order,
    Return_Order,
    Company_To_Client_Contract,
    Company_Contract,
    Client_Contract,
    Aggregator_Courier,
    Shipping_Partner,
    COD_Remittance,
    Wallet,
    Courier_Priority_Meta,
    Wallet_Logs,
    Company_To_Client_Rates,
    Company_To_Client_COD_Rates,
    Courier_Priority_Config_Setting,
    Courier_Priority_Rules,
    Courier_Priority,
    New_Company_To_Client_Rate,
)

# ✅ NEW: Import NDR model for status updates
from models.ndr import Ndr

# data
from data.Locations import metro_cities, special_zone
from data.courier_service_mapping import courier_service_mapping

# service
from modules.serviceability import ServiceabilityService
from modules.wallet import WalletService
from marketplace.shopify.shopify_service import Shopify
from shipping_partner.ats.ats import ATS
from shipping_partner.shiprocket.shiprocket import Shiprocket
from modules.shipping_notifications.shipping_notifications_service import (
    ShippingNotificaitions,
)
from modules.ndr.ndr_service import NdrService

# ✅ NEW: Import NDR history service for audit trail
from modules.ndr_history.ndr_history_service import NdrHistoryService

from components.manifest import generate_manifest


# utils
from utils.datetime import parse_datetime

PER_ORDER_CHARGE = 2


def get_courier(order: Order_Model):

    # return 28

    zone = order.zone
    applicable_weight = float(order.applicable_weight)
    courier = None

    if applicable_weight <= 0.499:

        if zone == "A":
            courier = "bluedart"
        elif zone == "B":
            courier = "delhivery"
        elif zone == "C":
            courier = "delhivery"
        elif zone == "D":
            courier = "delhivery"
        elif zone == "E":
            courier = "delhivery"

    elif applicable_weight < 0.999:

        if zone == "A":
            courier = "ecom"
        elif zone == "B":
            courier = "ecom"
        elif zone == "C":
            courier = "ecom"
        elif zone == "D":
            courier = "ecom"
        elif zone == "E":
            courier = "amazon"

    else:
        courier = "amazon"

    if courier == "bluedart":
        return 27

    if courier == "delhivery":
        return 28

    if courier == "ecom":
        return 29

    if courier == "amazon":
        return 30


def get_next_mwf(date):
    today = datetime.today().date()
    client_id = context_user_data.get().client_id

    days = 5
    if client_id == 26:
        days = 3
    elif client_id == 71:
        days = 4
    elif client_id == 139:
        days = 1

    elif client_id == 219:
        days = 1

    elif client_id == 234:
        days = 3

    elif client_id == 108:
        days = 3

    elif client_id == 211:
        days = 1

    elif client_id == 244:
        days = 1

    elif client_id == 180:
        days = 3

    elif client_id == 240:
        days = 3

    elif client_id == 93:
        days = 3

    elif client_id == 275:
        days = 2

    elif client_id == 359:
        days = 1

    elif client_id == 373:
        days = 1

    elif client_id == 374:
        days = 1

    elif client_id == 285:
        days = 3

    elif client_id == 310:
        days = 3

    elif client_id == 394:
        days = 3

    elif client_id == 380:
        days = 1

    elif client_id == 398:
        days = 4

    elif client_id == 397:
        days = 1

    elif client_id == 376:
        days = 3

    elif client_id == 406:
        days = 1

    elif client_id == 409:
        days = 1

    elif client_id == 405:
        days = 1

    elif client_id == 411:
        days = 1

    elif client_id == 414:
        days = 1

    elif client_id == 410:
        days = 3

    elif client_id == 416:
        days = 3

    elif client_id == 274:
        days = 3

    elif client_id == 408:
        days = 3

    elif client_id == 402:
        days = 4

    elif client_id == 273:
        days = 3

    elif client_id == 415:
        days = 3

    elif client_id == 425:
        days = 3
    elif client_id == 421:
        days = 1
    elif client_id == 427:
        days = 1

    elif client_id == 413:
        days = 4

    # Add D+X (days after the given date based on client_id)
    d_plus_x = date + timedelta(days=days + 1)

    # Ensure the date is not in the past
    if d_plus_x <= today:
        d_plus_x = today + timedelta(days=1)

    # Check if D+X is already Monday (0), Wednesday (2), or Friday (4)

    allowed_days = {0, 2, 4}  # Monday, Wednesday, Friday

    if (
        client_id == 139
        or client_id == 219
        or client_id == 244
        or client_id == 359
        or client_id == 373
        or client_id == 374
        or client_id == 380
        or client_id == 397
        or client_id == 406
        or client_id == 409
        or client_id == 411
        or client_id == 414
        or client_id == 421
        or client_id == 427
    ):
        allowed_days = {0, 2, 3, 4}  # Monday, Wednesday, Thursday, Friday

    if (
        client_id == 394
        or client_id == 405
        or client_id == 410
        or client_id == 416
        or client_id == 422
        or client_id == 426
    ):
        allowed_days = {0, 4}

    # Find the next available Monday, Wednesday, or Friday
    for i in range(0, 7):
        candidate_date = d_plus_x + timedelta(days=i)
        if candidate_date.weekday() in allowed_days:
            return candidate_date


def update_or_create_cod_remittance(order, db):
    delivered_status = "delivered"

    delivered_date = None
    for status_info in order.tracking_info:
        if status_info["status"] == delivered_status:
            datetime_str = status_info["datetime"]

            # Try to parse the datetime with both formats
            for date_format in ["%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                try:
                    delivered_date = datetime.strptime(datetime_str, date_format).date()
                    break  # If successful, exit the loop
                except ValueError:
                    continue  # Try the next format if ValueError occurs

            # If the datetime string doesn't match either format
            if delivered_date is None:
                raise ValueError(f"Unrecognized datetime format: {datetime_str}")

    # Calculate the next Wednesday after D+5
    remittance_date = get_next_mwf(delivered_date)

    print(delivered_date, remittance_date)

    # Check if there is already a COD remittance entry for the calculated remittance_date
    cod_remittance = (
        db.query(COD_Remittance)
        .filter(
            COD_Remittance.payout_date == remittance_date,
            COD_Remittance.client_id == order.client_id,
        )
        .first()
    )

    if cod_remittance:
        # Update existing remittance
        cod_remittance.generated_cod += order.total_amount
        cod_remittance.order_count += 1
    else:
        # Create a new COD remittance entry
        cod_remittance = COD_Remittance(
            payout_date=remittance_date,
            generated_cod=order.total_amount,
            order_count=1,
            client_id=order.client_id,
            status="pending",
        )
        db.add(cod_remittance)

    # Commit the remittance entry
    db.flush()

    print(cod_remittance.id)

    return cod_remittance.id


class TempModel(BaseModel):
    client_id: int


def manage_status_for_auto_assign_courier():
    return {
        "courier_assign_rules": "courier_assign_rules",
        "courier_priority": "courier_priority",
        "manual": "manual",
    }


class ShipmentService:

    # Function to convert HTML to PDF
    def convert_html_to_pdf(html_content):
        pdf_buffer = io.BytesIO()
        pisa_status = pisa.CreatePDF(io.StringIO(html_content), dest=pdf_buffer)

        if pisa_status.err:
            print("Error creating PDF")
            return None

        pdf_buffer.seek(0)
        return pdf_buffer

    @staticmethod
    def calculate_shipping_zone(
        pickup_pincode: int, destination_pincode: int
    ) -> GenericResponseModel:

        try:

            db = get_db_session()

            # fetch the city,state details for the pickup and destination pincodes

            pickup_location = (
                db.query(Pincode_Mapping)
                .filter(Pincode_Mapping.pincode == pickup_pincode)
                .first()
            )
            destination_location = (
                db.query(Pincode_Mapping)
                .filter(Pincode_Mapping.pincode == destination_pincode)
                .first()
            )

            if not pickup_location or not destination_location:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"zone": "D"},
                    message="Successfull",
                )

            # For A Zone -> Same city
            if pickup_location.city.lower() == destination_location.city.lower():
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"zone": "A"},
                    message="Successfull",
                )

            # For B Zone -> Same state
            if pickup_location.state.lower() == destination_location.state.lower():
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"zone": "B"},
                    message="Successfull",
                )

            # For E Zone -> Special Zones
            if (
                pickup_location.state.lower() in special_zone
                or destination_location.state.lower() in special_zone
            ):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"zone": "E"},
                    message="Successfull",
                )

            # For C Zone -> Metro to Metro
            if (
                pickup_location.city.lower() in metro_cities
                and destination_location.city.lower() in metro_cities
            ):
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"zone": "C"},
                    message="Successfull",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={"zone": "D"},
                message="Successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="could not calculate zone: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not calculate zone",
            )

    @staticmethod
    def after_sort_available_courier_list(sorted_contracts_list, order, db):
        weight = round(
            max(
                order.applicable_weight,
                order.volumetric_weight,
            ),
            3,
        )
        # print(order.order_id, "|*|weight new section|*|")
        print(len(sorted_contracts_list))
        make_dict = []
        for contract in sorted_contracts_list:
            min_chargeable_weight = 0.5
            additional_weight_bracket = 0.5
            print(1)
            applicable_weight = weight  # Coming Weight
            if applicable_weight < min_chargeable_weight:
                applicable_weight = min_chargeable_weight
            zone = order.zone or "D"
            payment_mode = order.payment_mode
            if float(applicable_weight) > float(min_chargeable_weight):
                additional_weight = float(applicable_weight) - float(
                    min_chargeable_weight
                )
                additional_bracket_count = math.ceil(
                    float(additional_weight) / float(additional_weight_bracket)
                )
            else:
                additional_bracket_count = 0
            freight_rate = (
                db.query(Company_To_Client_Rates)
                .filter(
                    Company_To_Client_Rates.contract_id == contract.id,
                    Company_To_Client_Rates.zone == zone,
                )
                .first()
            )
            print(2)
            base_freight = float(0.5)
            additional_freight = float(0.5) * additional_bracket_count
            freight = base_freight + additional_freight
            COD_freight = 0
            print(jsonable_encoder(contract), "for check absolute_rate")

            if payment_mode.lower() == "cod":
                # COD_freight = float(
                #     max(
                #         contract.absolute_rate,
                #         float(contract.percentage_rate * order.total_amount) * 0.01,
                #     )
                # )
                COD_freight = float(0.5)
            total_freight = freight + COD_freight

            tax = round(total_freight * 0.18, 2)
            print(3)
            has_sufficient_balance = WalletService.check_sufficient_balance(
                PER_ORDER_CHARGE
            )
            print(4)
            # IF BALANCE HAS NOT SUFFICIENT THEN SKIP TO ADD DICT
            if has_sufficient_balance.status == False:
                print("Skip Account Balance Is Not Sufficient")
                continue
            else:
                shipping_partner_slug = contract.shipping_partner.slug
                # print(4)
                make_dict.append(
                    {
                        "contract_id": contract.id,
                        "courier_id": contract.shipping_partner.id,
                        "courier_name": shipping_partner_slug,
                        "courier_slug": contract.shipping_partner.slug,
                        "total_freight": total_freight,
                        "cod_freight": COD_freight,
                        "tax": round(tax * 0.18, 2),
                    }
                )
            print("BEFORE SORTING", jsonable_encoder(make_dict))

        # return sorted(
        # make_dict, key=lambda x: meta_slugs.index(x["courier_slug"])
        return make_dict

        # )

    # Make Query
    @staticmethod
    def apply_rules_and_fetch_data(records, order_id, db):
        order_ship_by_rule = []
        for item in records:
            rules = item.rules
            courier_priority = item.courier_priority
            for rule in rules:
                logger.info(
                    f"Each Row for {format(str(rule))} For  order_id => {order_id}"
                )
                field = rule.get("field_name")
                rule_name = item.rule_name
                ordering_key = item.ordering_key
                operator = rule.get("operator")
                value = rule.get("value_a")
                filter_conditions = []
                print("field=>", field, "value=>", value)
                if field == "zone":
                    print("**welcomte to zone section**")
                    print(value[0], ">**Zone Section**<")
                    filter_conditions.append(Order.zone == value[0])
                elif field == "weight":
                    if operator == "bt":
                        value_b = rule.get("value_b")
                        if value and value_b:
                            filter_conditions.append(
                                Order.weight.between(float(value), float(value_b))
                            )
                    elif operator == "gt":
                        filter_conditions.append(Order.weight > float(value))
                    elif operator == "lt":
                        filter_conditions.append(Order.weight < float(value))
                    elif operator == "eq":
                        filter_conditions.append(Order.weight == float(value))
                    # print("Check weight Rule")
                    filter_conditions.append(Order.weight == float(value))
                elif field == "payment_mode":
                    filter_conditions.append(Order.payment_mode == value[0])
                elif field == "state":
                    value_lower = [v.lower() for v in value]
                    filter_conditions.append(
                        func.lower(Order.consignee_state).in_(set(value_lower))
                    )
                # Always filter by given order_id
                filter_conditions.append(Order.order_id == order_id)

                if filter_conditions:
                    # query = db.query(Order).filter(and_(*filter_conditions))
                    # compiled_query = query.statement.compile(
                    #     dialect=postgresql.dialect(),
                    #     compile_kwargs={"literal_binds": True},
                    # )
                    # print(compiled_query, "*<compiled_query>*")
                    orders_data = (
                        db.query(Order).filter(and_(*filter_conditions)).first()
                    )
                    if orders_data:
                        logger.info(
                            f">>*Matched*<< RULE NAME = {rule_name}, Field Name = {field}, And Operator Name = {operator}, And Ordering = {ordering_key}, For order_id => {order_id}"
                        )
                        courier_response = ShipmentService.courier_assign_rule(
                            orders_data.order_id, courier_priority
                        )
                        if len(courier_response) > 0:
                            logger.info(f"CONTRACT FOUND")
                            order_ship_by_rule.append(courier_response[0])
                        else:
                            print("Contract NOT Found")
                            return order_ship_by_rule
                    else:
                        logger.info(
                            f">>*Skip*<< RULE NAME = {rule_name}, Field Name = {field}, And Operator Name = {operator}, And Ordering = {ordering_key}, For order_id => {order_id}"
                        )
            # IF FIRST CASE MATHCED THEN EXIT FROM LOOP
            if len(order_ship_by_rule) > 0:
                print(
                    f"Break When Record will Matched {item.rule_name} courier_priority:{courier_priority}"
                )
                break
        return order_ship_by_rule

    @staticmethod
    def courier_assign_rule(order_id: str, courier_priority: List):
        try:
            print("welcome trigger action")
            slugs = [c["slug"] for c in courier_priority]
            client_id = context_user_data.get().client_id
            with get_db_session() as db:
                shipment_response = []
                query = (
                    db.query(Client_Contract)
                    .join(Client_Contract.shipping_partner)
                    .filter(
                        Client_Contract.client_id == client_id,
                        Shipping_Partner.slug.in_(slugs),
                    )
                    .options(joinedload(Client_Contract.shipping_partner))
                )
                #  Print the compiled SQL query with actual values
                print(
                    str(
                        query.statement.compile(compile_kwargs={"literal_binds": True})
                    ),
                    "**Query Run**",
                )
                contracts = query.all()
                # Sort By aggregator_courier SLUG WITH ORDERING
                slug_to_contract = {
                    contract.shipping_partner.slug: contract for contract in contracts
                }
                print("steps01")
                # Sort contracts based on slug order
                sorted_contracts_list = [
                    slug_to_contract[slug] for slug in slugs if slug in slug_to_contract
                ]
                print("steps02", len(sorted_contracts_list))
                if sorted_contracts_list != None and len(sorted_contracts_list) > 0:
                    order = (
                        db.query(Order)
                        .filter(
                            Order.client_id == client_id, Order.order_id == order_id
                        )
                        .first()
                    )
                    weight = round(
                        max(
                            order.applicable_weight,
                            order.volumetric_weight,
                        ),
                        3,
                    )
                    courier_abailablity = (
                        ShipmentService.after_sort_available_courier_list(
                            sorted_contracts_list, order, db
                        )
                    )
                    if len(courier_abailablity) == 0:
                        logger.info("No courier is available for this shipment.")
                        return shipment_response
                    else:
                        i_counter = 0
                        for available_courier in courier_abailablity:
                            print(
                                available_courier["contract_id"],
                                "Incomming function trigger",
                            )
                            auto_assign_freight = ShipmentService.auto_assign_awb(
                                CreateShipmentModel(
                                    order_id=order_id,
                                    contract_id=available_courier["contract_id"],
                                    total_freight=available_courier["total_freight"],
                                    cod_freight=available_courier["cod_freight"],
                                    tax=available_courier["tax"],
                                )
                            )
                            if auto_assign_freight.status == True:
                                shipment_response.append(auto_assign_freight)
                                break
                            i_counter += 1
                        return shipment_response  # Shipment Final Response
                else:
                    print("No contract available")
                    return []
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
    def auto_assign_available_courier(
        auto_shipment_params: AutoCreateShipmentModel,
    ):
        try:
            order_id = auto_shipment_params.order_id
            priority_type = auto_shipment_params.priority_type
            client_id = context_user_data.get().client_id
            db = get_db_session()
            order = (
                db.query(Order)
                .filter(Order.client_id == client_id, Order.order_id == order_id)
                .first()
            )
            if priority_type == "custom":
                Courier_Priority_res = (
                    db.query(
                        Courier_Priority_Meta.meta_slug,
                        Courier_Priority_Meta.ordering_key,
                    )
                    .filter(Courier_Priority_Meta.client_id == client_id)
                    .all()
                )
                # print(Courier_Priority_res, "||Courier_Priority_res||")
                meta_slugs = [
                    meta_slug
                    for meta_slug, ordering_key in sorted(
                        Courier_Priority_res, key=lambda x: x[1]
                    )
                ]
                print(meta_slugs, "**meta_slugs**")
                contracts = (
                    db.query(New_Company_To_Client_Rate)
                    .join(New_Company_To_Client_Rate.shipping_partner)
                    .filter(
                        New_Company_To_Client_Rate.client_id == client_id,
                        Shipping_Partner.slug.in_(meta_slugs),
                        New_Company_To_Client_Rate.isActive == True,
                    )
                    .options(
                        joinedload(
                            New_Company_To_Client_Rate.client_contract
                        ).joinedload(Client_Contract.shipping_partner)
                    )
                    .all()
                )
                # print(jsonable_encoder(contracts), "<<contracts>>")
            else:
                print("I AM SHIPING SERVICE I AM NOT CUSTOM")
                contracts = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.client_id == client_id,
                        New_Company_To_Client_Rate.isActive == True,
                    )
                    .options(
                        joinedload(
                            New_Company_To_Client_Rate.client_contract
                        ).joinedload(Client_Contract.shipping_partner),
                        joinedload(New_Company_To_Client_Rate.shipping_partner),
                    )
                    .all()
                )

            if contracts != None:
                weight = round(
                    max(
                        order.applicable_weight,
                        order.volumetric_weight,
                    ),
                    3,
                )
                # print("START >", jsonable_encoder(contracts), "<END")
                make_dict = []
                print("Total length New action", len(contracts))
                for contract in contracts:
                    min_chargeable_weight = 0.5
                    additional_weight_bracket = 0.5
                    # print(1)
                    applicable_weight = weight  # Coming Weight
                    if applicable_weight < min_chargeable_weight:
                        applicable_weight = min_chargeable_weight
                    zone = order.zone or "D"
                    payment_mode = order.payment_mode
                    if float(applicable_weight) > float(min_chargeable_weight):
                        additional_weight = float(applicable_weight) - float(
                            min_chargeable_weight
                        )
                        additional_bracket_count = math.ceil(
                            float(additional_weight) / float(additional_weight_bracket)
                        )
                    else:
                        additional_bracket_count = 0
                    zone = zone.lower()  # zone to lowercase like "zone_a"
                    base_rate_key = f"base_rate_zone_{zone}"
                    additional_rate_key = f"additional_rate_zone_{zone}"

                    base_rate = getattr(contract, base_rate_key, 0)
                    additional_rate = getattr(contract, additional_rate_key, 0)
                    print(2.2)
                    base_freight = float(base_rate)
                    additional_freight = (
                        float(additional_rate) * additional_bracket_count
                    )
                    freight = base_freight + additional_freight
                    COD_freight = 0
                    print(2.3)
                    if payment_mode.lower() == "cod":

                        COD_freight = float(
                            max(
                                contract.absolute_rate,
                                float(contract.percentage_rate * order.total_amount)
                                * 0.01,
                            )
                        )
                    print(2.4)
                    total_freight = freight + COD_freight
                    print(2.5)
                    tax = round(total_freight * 0.18, 2)
                    # print(3)
                    has_sufficient_balance = WalletService.check_sufficient_balance(
                        PER_ORDER_CHARGE
                    )
                    print(2.5)
                    # IF BALANCE HAS NOT SUFFICIENT THEN SKIP TO ADD DICT
                    if has_sufficient_balance.status == False:
                        continue
                    else:
                        print(2.6)
                        shipping_partner_slug = (
                            contract.client_contract.shipping_partner.slug
                        )
                        make_dict.append(
                            {
                                "contract_id": contract.client_contract.id,
                                "courier_id": contract.client_contract.shipping_partner.id,
                                "courier_name": shipping_partner_slug,
                                "courier_slug": contract.client_contract.shipping_partner.slug,
                                "total_freight": total_freight,
                                "cod_freight": COD_freight,
                                "tax": tax,
                            }
                        )
                if priority_type == "cheapest":
                    return sorted(make_dict, key=lambda x: x["total_freight"])
                else:
                    print("BEFORE SORTING", jsonable_encoder(make_dict))
                    print(
                        "AFTER SORTING",
                        sorted(
                            make_dict, key=lambda x: meta_slugs.index(x["courier_slug"])
                        ),
                    )
                    return sorted(
                        make_dict, key=lambda x: meta_slugs.index(x["courier_slug"])
                    )
            else:
                return []
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

        finally:
            if db:
                db.close()

    @staticmethod
    def assign_priority_wise_courier(
        priority_type: str,
        order_id: str,
    ):
        try:
            start_time = time.time()
            available_courier_list = ShipmentService.auto_assign_available_courier(
                AutoCreateShipmentModel(order_id=order_id, priority_type=priority_type)
            )
            if priority_type == "cheapest":
                print(
                    "AFTER SORT TOTAL FREIGHT WISE=>",
                    jsonable_encoder(available_courier_list),
                )
            if priority_type == "custom":
                print(
                    "AFTER SORT ORDERING COURIER WISE=>",
                    jsonable_encoder(available_courier_list),
                )
            courier_respose = {}
            auto_assign_freight_list = []
            if len(available_courier_list) == 0:
                print("There is not available courier for this shipment 124")

            else:
                i_counter = 0
                for available_courier in available_courier_list:
                    auto_assign_freight = ShipmentService.auto_assign_awb(
                        CreateShipmentModel(
                            order_id=order_id,
                            contract_id=available_courier["contract_id"],
                            total_freight=available_courier["total_freight"],
                            cod_freight=available_courier["cod_freight"],
                            tax=available_courier["tax"],
                        )
                    )
                    print(
                        auto_assign_freight,
                        "||auto_assign_freight|True Action|",
                        auto_assign_freight.status,
                    )
                    if auto_assign_freight.status == True:
                        auto_assign_freight_list.append(auto_assign_freight)
                        break
                    i_counter += 1
            end_time = time.time()
            total_duration = round(end_time - start_time, 1)  # in seconds
            print(auto_assign_freight_list, "**Final Auto assigning connect**")
            return auto_assign_freight_list
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
    def auto_assign_awb(
        shipment_params: CreateShipmentModel,
    ):
        try:
            order_id = shipment_params.order_id
            contract_id = shipment_params.contract_id
            freight = shipment_params.total_freight
            cod_freight = shipment_params.cod_freight
            tax = shipment_params.tax
            client_id = context_user_data.get().client_id
            db = get_db_session()
            order = (
                db.query(Order)
                .filter(Order.client_id == client_id, Order.order_id == order_id)
                .first()
            )
            if order.status != "new":
                print("AWB already assigned from auto_assign_awb function")
                db.close()
                return "AWB already assigned"
            else:
                print(contract_id, "<<<contract_id")
                client_contract = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.client_contract_id == contract_id
                    )
                    .options(
                        # Eager load the related Company_Contract and its Shipping_Partner
                        joinedload(
                            New_Company_To_Client_Rate.client_contract
                        ).joinedload(Client_Contract.shipping_partner)
                    )
                    .first()
                )
                shipping_partner_slug = (
                    client_contract.client_contract.shipping_partner.slug
                )
                # # print(shipping_partner_slug, "shipping_partner_slug")
                shipping_partner_slug = "xpressbees"  # FOR DEVELOPMENT
                shipping_partner = courier_service_mapping[shipping_partner_slug]
                # # FOR DEVELOPMENT
                if shipping_partner_slug == "xpressbees":
                    print(
                        "welcome to xpressbees courier dev courier",
                        client_contract.shipping_partner,
                    )
                    shipment_response = shipping_partner.dev_create_order(
                        order,
                        client_contract.client_contract.credentials,
                        client_contract.client_contract.shipping_partner,
                    )

                # PRODUCTION
                # if shipping_partner_slug == "logistify":
                #     order.forward_freight = freight["freight"]
                #     order.forward_cod_charge = freight["cod_charges"]
                #     order.forward_tax = freight["tax_amount"]

                #     db.add(order)
                #     db.commit()

                # PRODUCTION
                # if shipping_partner_slug == "ekart":

                #     shipment_response = shipping_partner.create_order(
                #         order,
                #         client_contract.company_contract.credentials,
                #         client_contract.aggregator_courier,
                #         client_contract.company_contract,
                #     )

                # else:
                #     shipment_response = shipping_partner.create_order(
                #         order,
                #         client_contract.company_contract.credentials,
                #         client_contract.aggregator_courier,
                #     )

                if shipment_response.status == True:
                    if order.source == "shopify":
                        Shopify.update_order_fulfillment_status(
                            order_id=order.marketplace_order_id,
                            awb_number=order.awb_number,
                            store_id=order.store_id,
                        )
                    order.booking_date = datetime.now(timezone.utc)
                    is_processing = shipment_response.data.get("processing", None)
                    order.forward_freight = freight
                    order.forward_cod_charge = cod_freight
                    order.forward_tax = tax
                    total_freight = freight + cod_freight + tax
                    if is_processing:
                        db.close()
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.OK,
                            data={"is_processing": True},
                            message="Orders are being processed",
                        )
                    WalletService.deduct_money(
                        PER_ORDER_CHARGE, shipment_response.data["awb_number"]
                    )
                    if order.payment_mode.lower() == "cod":
                        WalletService.add_provisional_cod(order.total_amount)
                    db.flush()
                    db.commit()
                db.close()
                # print(
                #     f"FINAL OUTOUT ACTION TRIGGER{jsonable_encoder(shipment_response)}"
                # )
                return shipment_response

            # return "success"

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

        finally:
            if db:
                db.close()

    def model_to_dict(instance):
        return {
            column.name: getattr(instance, column.name)
            for column in instance.__table__.columns
        }

    @staticmethod
    def assign_awb(
        shipment_params: CreateShipmentModel,
    ):

        try:
            order_id = shipment_params.order_id
            courier_id = shipment_params.contract_id
            client_id = context_user_data.get().client_id
            db = get_db_session()
            order = (
                db.query(Order)
                .filter(Order.client_id == client_id, Order.order_id == order_id)
                .first()
            )
            if order.status != "new":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    status=True,
                    data={
                        "awb_number": order.awb_number or "",
                        "delivery_partner": order.courier_partner or "",
                    },
                    message="AWB already assigned",
                )
            query = (
                db.query(New_Company_To_Client_Rate)
                .filter(
                    New_Company_To_Client_Rate.id == courier_id,
                    New_Company_To_Client_Rate.client_id == client_id,
                    New_Company_To_Client_Rate.isActive == True,
                )
                .options(
                    joinedload(New_Company_To_Client_Rate.client_contract).joinedload(
                        Client_Contract.shipping_partner
                    )
                )
            )
            # Print SQL with bound values substituted
            compiled_query = query.statement.compile(
                dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
            )
            print("\n=== Generated SQL Query ===\n")
            print(compiled_query)
            print("\n===========================\n")
            client_contract = query.first()
            if client_contract is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Invalid Courier Id",
                )
            freight = ServiceabilityService.calculate_freight(
                order_id=order_id,
                min_chargeable_weight=0.5,
                additional_weight_bracket=0.5,
                contract_id=courier_id,
                contract_data=ShipmentService.model_to_dict(client_contract),
                # contract_data=client_contract,
            )
            print(">>>>")
            print(jsonable_encoder(freight))
            print("<<<<")
            # total_freight = (
            #     freight["freight"] + freight["cod_charges"] + freight["tax_amount"]
            # )
            # check if the wallet has sufficient balance or not
            has_sufficient_balance = WalletService.check_sufficient_balance(
                PER_ORDER_CHARGE
            )

            # if the wallet does not have sufficient balance, throw error
            if has_sufficient_balance.status == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=has_sufficient_balance.message,
                )
            # print(client_contract.client_contract.shipping_partner.slug, "Slug")
            shipping_partner_slug = (
                client_contract.client_contract.shipping_partner.slug
            )

            shipping_partner_slug = "xpressbees"  # FOR DEVELOPMENT MODE ONLY
            shipping_partner = courier_service_mapping[shipping_partner_slug]
            # FOR DEVELOPMENT START>
            if shipping_partner_slug == "xpressbees":
                shipment_response = shipping_partner.dev_create_order(
                    order,
                    client_contract.client_contract.credentials,
                    client_contract.client_contract.shipping_partner,
                )
            # FOR DEVELOPMENT <END

            # if shipping_partner_slug == "logistify":
            #     # store the freight data in db
            #     order.forward_freight = freight["freight"]
            #     order.forward_cod_charge = freight["cod_charges"]
            #     order.forward_tax = freight["tax_amount"]

            #     db.add(order)
            #     db.commit()

            # if shipping_partner_slug == "ekart":

            #     shipment_response = shipping_partner.create_order(
            #         order,
            #         client_contract.client_contract.credentials,
            #         client_contract.aggregator_courier,
            #         client_contract.company_contract,
            #     )

            # else:
            #     shipment_response = shipping_partner.create_order(
            #         order,
            #         client_contract.client_contract.credentials,
            #         client_contract.client_contract.aggregator_courier,
            #     )

            # if the shipment is created successfully, i.e, the awb is assigned, deduct from wallet
            if shipment_response.status == True:

                order.booking_date = datetime.now(timezone.utc)
                order.shipment_booking_error = None

                # ShippingNotificaitions.send_notification(order, "order_shipped")

                if order.source == "shopify":
                    Shopify.update_order_fulfillment_status(
                        order_id=order.marketplace_order_id,
                        awb_number=order.awb_number,
                        store_id=order.store_id,
                    )

                is_processing = shipment_response.data.get("processing", None)

                order.booking_date = datetime.now(timezone.utc)

                # store the freight data in db
                order.forward_freight = freight["freight"]
                order.forward_cod_charge = freight["cod_charges"]
                order.forward_tax = freight["tax_amount"]

                db.add(order)

                if is_processing:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        data={"is_processing": True},
                        message="Orders are being processed",
                    )

                WalletService.deduct_money(
                    PER_ORDER_CHARGE, shipment_response.data["awb_number"]
                )

                # if the payment of the order is COD, add the cod amount to the provisional COD in wallet
                if order.payment_mode.lower() == "cod":
                    WalletService.add_provisional_cod(order.total_amount)

                db.flush()

            # if the shipment is not created successfully, i.e, the awb is not assigned, store the error message in db
            if shipment_response.status == False:
                order.shipment_booking_error = (
                    client_contract.client_contract.slug
                    + " : "
                    + shipment_response.message
                )
                db.add(order)
                db.flush()

            db.commit()

            return shipment_response

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

        finally:
            if db.is_active:
                db.close()

    @staticmethod
    def dev_assign_awb(courier_id: int, order: Order_Model):

        try:

            order_id = order.order_id

            client_id = context_user_data.get().client_id

            db = get_db_session()

            if order.status != "new":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    status=True,
                    data={
                        "awb_number": order.awb_number or "",
                        "delivery_partner": order.courier_partner or "",
                    },
                    message="AWB already assigned",
                )

            freight = ServiceabilityService.calculate_freight(
                order_id=order_id,
                min_chargeable_weight=0.5,
                additional_weight_bracket=0.5,
                contract_id=courier_id,
            )

            total_freight = (
                freight["freight"] + freight["cod_charges"] + freight["tax_amount"]
            )

            if courier_id == 491:
                awb = "77"
                awb = awb + "".join([str(random.randint(0, 9)) for _ in range(9)])
                coruier_parnter = "Bluedart"

            elif courier_id == 492:
                awb = "800"
                awb = awb + "".join([str(random.randint(0, 9)) for _ in range(8)])
                coruier_parnter = "Bluedart-Air"

            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid courier ",
                )

            order.status = "booked"
            order.sub_status = "shipment booked"
            order.courier_status = "BOOKED"

            order.awb_number = awb
            order.aggregator = "dev"
            order.courier_partner = coruier_parnter

            shipment_response = GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                data={
                    "awb_number": order.awb_number,
                    "delivery_partner": coruier_parnter,
                },
                message="AWB assigned successfully",
            )

            print(order.awb_number)

            # if the shipment is created successfully, i.e, the awb is assigned, deduct from wallet
            if shipment_response.status == True:

                order.booking_date = datetime.now(timezone.utc)

                WalletService.deduct_money(
                    PER_ORDER_CHARGE, shipment_response.data["awb_number"]
                )

                db.add(order)
                db.flush()
                db.commit()

            time.sleep(0.5)
            return shipment_response

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

        finally:
            if db:
                db.close()

    @staticmethod
    def assign_reverse_awb(
        shipment_params: CreateShipmentModel,
    ):

        try:

            order_id = shipment_params.order_id
            courier_id = shipment_params.contract_id

            client_id = context_user_data.get().client_id

            db = get_db_session()

            order = (
                db.query(Return_Order)
                .filter(
                    Return_Order.client_id == client_id,
                    Return_Order.order_id == order_id,
                )
                .first()
            )

            if order.status != "new":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Shipment already processed",
                )

            client_contract = (
                db.query(New_Company_To_Client_Rate)
                .filter(New_Company_To_Client_Rate.id == courier_id)
                .options(
                    joinedload(New_Company_To_Client_Rate.company_contract).joinedload(
                        Company_Contract.shipping_partner
                    ),
                    joinedload(New_Company_To_Client_Rate.aggregator_courier),
                )
                .first()
            )

            if client_id == 82 and courier_id == 419 and order.zone == "E":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Non Serviceable Pincode",
                )

            # freight = ServiceabilityService.calculate_freight(
            #     order_id=order_id,
            #     min_chargeable_weight=client_contract.aggregator_courier.min_chargeable_weight,
            #     additional_weight_bracket=client_contract.aggregator_courier.additional_weight_bracket,
            #     contract_id=courier_id,
            #     rate_type="reverse",
            # )
            freight = ServiceabilityService.calculate_freight(
                order_id=order_id,
                min_chargeable_weight=client_contract.aggregator_courier.min_chargeable_weight,
                additional_weight_bracket=client_contract.aggregator_courier.additional_weight_bracket,
                contract_id=courier_id,
                contract_data=ShipmentService.model_to_dict(client_contract),
                rate_type="reverse",
            )

            total_freight = (
                freight["freight"] + freight["cod_charges"] + freight["tax_amount"]
            )

            # return False
            # check if the wallet has sufficient balance or not
            has_sufficient_balance = WalletService.check_sufficient_balance(
                PER_ORDER_CHARGE
            )

            # if the wallet does not have sufficient balance, throw error
            if has_sufficient_balance.status == False:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=has_sufficient_balance.message,
                )

            # shipping_partner_slug = "xpressbees"
            shipping_partner_slug = (
                client_contract.company_contract.shipping_partner.slug
            )

            # create the shipment based on the courier parnter selected
            shipping_partner = courier_service_mapping[shipping_partner_slug]

            if shipping_partner_slug == "logistify":
                # store the freight data in db
                order.forward_freight = freight["freight"]
                order.forward_cod_charge = freight["cod_charges"]
                order.forward_tax = freight["tax_amount"]

                db.add(order)
                db.commit()
            # Use the create order function of the required shipping partner service
            shipment_response = shipping_partner.create_reverse_order(
                order,
                client_contract.company_contract.credentials,
                client_contract.aggregator_courier,
            )

            # if the shipment is created successfully, i.e, the awb is assigned, deduct from wallet
            if shipment_response.status == True:
                order.booking_date = datetime.now(timezone.utc)
                try:
                    if client_id == 93:
                        body = {
                            "awb": order.awb_number,
                            "current_status": "booked",
                            "order_id": order.order_id,
                            "current_timestamp": datetime.now().strftime(
                                "%d-%m-%Y %H:%M:%S"
                            ),
                            "shipment_status": order.sub_status,
                            "scans": [],
                        }

                        response = requests.post(
                            url="https://wtpzsmej1h.execute-api.ap-south-1.amazonaws.com/prod/webhook/bluedart",
                            verify=True,
                            timeout=10,
                            json=body,
                        )

                        print(response.json())

                except:
                    pass

                is_processing = shipment_response.data.get("processing", None)

                order.booking_date = datetime.now(timezone.utc)

                # store the freight data in db
                order.forward_freight = freight["freight"]
                order.forward_cod_charge = freight["cod_charges"]
                order.forward_tax = freight["tax_amount"]

                db.add(order)

                if is_processing:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        data={"is_processing": True},
                        message="Orders are being processed",
                    )

                WalletService.deduct_money(
                    PER_ORDER_CHARGE, shipment_response.data["awb_number"]
                )

                # if the payment of the order is COD, add the cod amount to the provisional COD in wallet
                if order.payment_mode.lower() == "cod":
                    WalletService.add_provisional_cod(order.total_amount)

                db.flush()
                db.commit()

            return shipment_response

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

        finally:
            if db:
                db.close()

    @staticmethod
    def bulk_assign_awbs(
        shipment_params: BulkCreateShipmentModel,
    ):

        try:
            print("**WELCOME TO B ULK ASSIGN AWB NUMBER**")

            order_ids = shipment_params.order_ids
            client_id = context_user_data.get().client_id

            db = get_db_session()

            total_shipment_count = len(shipment_params.order_ids)
            posted_shipment_count = 0

            courier_id = shipment_params.courier_id

            client_contract = (
                db.query(Company_To_Client_Contract)
                .filter(Company_To_Client_Contract.id == courier_id)
                .options(
                    joinedload(Company_To_Client_Contract.company_contract).joinedload(
                        Company_Contract.shipping_partner
                    )
                )
                .options(joinedload(Company_To_Client_Contract.aggregator_courier))
                .first()
            )

            for order_id in order_ids:

                order = (
                    db.query(Order)
                    .filter(Order.client_id == client_id, Order.order_id == order_id)
                    .first()
                )

                if order.status != "new":
                    continue

                freight = ServiceabilityService.calculate_freight(
                    order_id=order_id,
                    min_chargeable_weight=client_contract.aggregator_courier.min_chargeable_weight,
                    additional_weight_bracket=client_contract.aggregator_courier.additional_weight_bracket,
                    contract_id=courier_id,
                )

                # total_freight = (
                #     freight["freight"] + freight["cod_charges"] + freight["tax_amount"]
                # )

                # check if the wallet has sufficient balance or not
                has_sufficient_balance = WalletService.check_sufficient_balance(
                    PER_ORDER_CHARGE
                )

                # if the wallet does not have sufficient balance, throw error
                if has_sufficient_balance.status == False:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        data={
                            "total_shipments": total_shipment_count,
                            "posted_count": posted_shipment_count,
                        },
                        message=has_sufficient_balance.message,
                    )

                shipping_partner_slug = (
                    client_contract.company_contract.shipping_partner.slug
                )

                print(shipping_partner_slug)

                # create the shipment based on the courier parnter selected
                shipping_partner = courier_service_mapping[shipping_partner_slug]

                # Use the create order function of the required shipping partner service
                if shipping_partner_slug == "ekart":

                    shipment_response = shipping_partner.create_order(
                        order,
                        client_contract.company_contract.credentials,
                        client_contract.aggregator_courier,
                        client_contract.company_contract,
                    )

                else:
                    shipment_response = shipping_partner.create_order(
                        order,
                        client_contract.company_contract.credentials,
                        client_contract.aggregator_courier,
                    )

                # if the shipment is created successfully, i.e, the awb is assigned, deduct from wallet
                if shipment_response.status == True:

                    # ShippingNotificaitions.send_notification(order, "order_shipped")

                    if order.source == "shopify":
                        Shopify.update_order_fulfillment_status(
                            order_id=order.marketplace_order_id,
                            awb_number=order.awb_number,
                            store_id=order.store_id,
                        )

                    is_processing = shipment_response.data.get("processing", None)
                    posted_shipment_count += 1

                    order.booking_date = datetime.now(timezone.utc)

                    # store the freight data in db
                    order.forward_freight = freight["freight"]
                    order.forward_cod_charge = freight["cod_charges"]
                    order.forward_tax = freight["tax_amount"]

                    db.add(order)

                    if is_processing:
                        continue

                    WalletService.deduct_money(
                        PER_ORDER_CHARGE, shipment_response.data["awb_number"]
                    )

                    # if the payment of the order is COD, add the cod amount to the provisional COD in wallet
                    # if order.payment_mode.lower() == "cod":
                    #     WalletService.add_provisional_cod(order.total_amount)

                    db.flush()
                    db.commit()

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                data={
                    "total_shipments": total_shipment_count,
                    "posted_count": posted_shipment_count,
                },
                message="success",
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
    def dev_bulk_assign_awbs(
        shipment_params: NewBulkCreateShipmentModel,
    ):
        try:
            order_ids = shipment_params.order_ids
            courier_type = shipment_params.courier_type
            total_shipment_count = len(shipment_params.order_ids)
            courier_id = shipment_params.courier_id
            client_id = context_user_data.get().client_id
            company_id = context_user_data.get().company_id
            posted_shipment_count = 0
            db = get_db_session()
            shipments_to_create = []
            if len(manage_status_for_auto_assign_courier().get(courier_type, "")) > 0:
                if courier_type == "courier_assign_rules":
                    for order_id in order_ids:
                        courier_priority_rules = (
                            db.query(Courier_Priority_Rules)
                            .filter(
                                and_(
                                    Courier_Priority_Rules.status == True,
                                    Courier_Priority_Rules.client_id == client_id,
                                )
                            )
                            .order_by(Courier_Priority_Rules.ordering_key.asc())
                            .all()
                        )
                        if (
                            len(courier_priority_rules) > 0
                            and courier_priority_rules != None
                        ):
                            print(
                                "WELCOME TO apply_rules_and_fetch_data READY TO TRIGGER"
                            )
                            apply_rules_response = (
                                ShipmentService.apply_rules_and_fetch_data(
                                    courier_priority_rules, order_id, db
                                )
                            )
                            print("apply_rules_response=>", apply_rules_response)
                            if (
                                len(apply_rules_response) > 0
                                and apply_rules_response != None
                            ):
                                shipments_to_create.append(apply_rules_response[0])

                    if len(shipments_to_create) > 0:
                        return GenericResponseModel(
                            status=True,
                            status_code=http.HTTPStatus.OK,
                            data=shipments_to_create,
                            message="Success",
                        )
                    else:
                        return GenericResponseModel(
                            status=False,
                            status_code=http.HTTPStatus.CONFLICT,
                            message="There are No Rule Matched",
                        )

                if courier_type == "courier_priority":
                    for order_id in order_ids:
                        courier_priority = (
                            db.query(Courier_Priority.priority_type)
                            .filter(Courier_Priority.client_id == client_id)
                            .first()
                        )
                        if courier_priority != None:
                            assign_priority_wise_courier = (
                                ShipmentService.assign_priority_wise_courier(
                                    courier_priority[0], order_id
                                )
                            )

                            if (
                                len(assign_priority_wise_courier) > 0
                                and assign_priority_wise_courier != None
                            ):
                                shipments_to_create.append(
                                    assign_priority_wise_courier[0]
                                )
                    if len(shipments_to_create) > 0:
                        return GenericResponseModel(
                            status=True,
                            status_code=http.HTTPStatus.OK,
                            data=shipments_to_create,
                            message="Success",
                        )
                    else:
                        return GenericResponseModel(
                            status=False,
                            status_code=http.HTTPStatus.CONFLICT,
                            message="There are Courier Selected",
                        )

                if courier_type == "manual":
                    client_contract = (
                        db.query(New_Company_To_Client_Rate)
                        .filter(
                            New_Company_To_Client_Rate.id == courier_id,
                            New_Company_To_Client_Rate.client_id == client_id,
                        )
                        .options(
                            joinedload(
                                New_Company_To_Client_Rate.client_contract
                            ).joinedload(Client_Contract.shipping_partner)
                        )
                        .first()
                    )
                    for order_id in order_ids:
                        order = (
                            db.query(Order)
                            .filter(
                                Order.client_id == client_id,
                                Order.order_id == order_id,
                            )
                            .first()
                        )
                        if order.status != "new":
                            continue
                        freight = ServiceabilityService.calculate_freight(
                            order_id=order_id,
                            min_chargeable_weight=0.5,
                            additional_weight_bracket=0.5,
                            contract_id=courier_id,
                            contract_data=ShipmentService.model_to_dict(
                                client_contract
                            ),
                            # contract_data=client_contract,
                        )
                        # total_freight = (
                        #     freight["freight"]
                        #     + freight["cod_charges"]
                        #     + freight["tax_amount"]
                        # )
                        # check if the wallet has sufficient balance or not
                        has_sufficient_balance = WalletService.check_sufficient_balance(
                            PER_ORDER_CHARGE
                        )
                        # if the wallet does not have sufficient balance, throw error
                        if has_sufficient_balance.status == False:
                            return GenericResponseModel(
                                status_code=http.HTTPStatus.BAD_REQUEST,
                                data={
                                    "total_shipments": total_shipment_count,
                                    "posted_count": posted_shipment_count,
                                },
                                message=has_sufficient_balance.message,
                            )
                        shipping_partner_slug = (
                            client_contract.client_contract.shipping_partner.slug
                        )
                        shipping_partner_slug = "xpressbees"
                        # create the shipment based on the courier parnter selected
                        shipping_partner = courier_service_mapping[
                            shipping_partner_slug
                        ]
                        # FOR DEVELOPMENT
                        if shipping_partner_slug == "xpressbees":
                            print(
                                "welcome to xpressbees courier dev courier",
                                client_contract.shipping_partner,
                            )
                            shipment_response = shipping_partner.dev_create_order(
                                order,
                                client_contract.client_contract.credentials,
                                client_contract.client_contract.shipping_partner,
                            )

                        # Use the create order function of the required shipping partner service
                        # if shipping_partner_slug == "ekart":

                        #     shipment_response = shipping_partner.create_order(
                        #         order,
                        #         client_contract.client_contract.credentials,
                        #         client_contract.client_contract.shipping_partner,
                        #         client_contract.client_contract.client_contract,
                        #     )

                        # else:
                        #     shipment_response = shipping_partner.create_order(
                        #         order,
                        #         client_contract.client_contract.credentials,
                        #         client_contract.client_contract.shipping_partner,
                        #     )

                        # if the shipment is created successfully, i.e, the awb is assigned, deduct from wallet
                        if shipment_response.status == True:

                            order.shipment_booking_error = None
                            db.add(order)
                            db.flush()
                            if order.source == "shopify":
                                Shopify.update_order_fulfillment_status(
                                    order_id=order.marketplace_order_id,
                                    awb_number=order.awb_number,
                                    store_id=order.store_id,
                                )
                            is_processing = shipment_response.data.get(
                                "processing", None
                            )
                            posted_shipment_count += 1
                            order.booking_date = datetime.now(timezone.utc)
                            # store the freight data in db
                            order.forward_freight = freight["freight"]
                            order.forward_cod_charge = freight["cod_charges"]
                            order.forward_tax = freight["tax_amount"]
                            db.add(order)
                            if is_processing:
                                continue
                            WalletService.deduct_money(
                                PER_ORDER_CHARGE, shipment_response.data["awb_number"]
                            )
                            # if the payment of the order is COD, add the cod amount to the provisional COD in wallet
                            if order.payment_mode.lower() == "cod":
                                WalletService.add_provisional_cod(order.total_amount)
                            db.flush()
                            db.commit()
                        if shipment_response.status == False:
                            order.shipment_booking_error = (
                                client_contract.aggregator_courier.slug
                                + " : "
                                + shipment_response.message
                            )
                            db.add(order)
                    db.commit()
                    return GenericResponseModel(
                        status=True,
                        status_code=http.HTTPStatus.OK,
                        data={
                            "total_shipments": total_shipment_count,
                            "posted_count": posted_shipment_count,
                        },
                        message="success",
                    )
            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    message="An error occurred while posting the shipment.",
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
    def track_shipment(
        awb_number: str,
    ):

        try:

            client_id = context_user_data.get().client_id

            if client_id == 93:
                return GenericResponseModel(
                    status=False, status_code=401, message="Unauthorized"
                )

            db = get_db_session()
            print("ready to print")
            # Get the order from the database using the AWB number
            order = (
                db.query(Order)
                .filter(Order.client_id == client_id, Order.awb_number == awb_number)
                .first()
            )
            print("ready to print 2")
            client_id = order.client_id

            context_user_data.set(TempModel(**{"client_id": client_id}))
            print("ready to print 3")
            # Check if the order exists
            if order is None or order.awb_number is None:

                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid AWB number",
                )
            print("ready to print 4", order.aggregator)
            shipping_partner_slug = order.aggregator
            shipping_partner = courier_service_mapping[shipping_partner_slug]

            print(shipping_partner_slug)

            # Use the create order function of the required shipping partner service
            tracking_response = shipping_partner.track_shipment(
                order=order,
                awb_number=order.awb_number,
            )
            # print(jsonable_encoder(tracking_response))
            current_status = tracking_response.data.get("current_status", None)

            print(current_status)

            # if current_status == None:
            #     continue

            db.commit()

            if current_status == "booked" and order.is_label_generated == True:
                order.status = "pickup"
                order.booked = "pickup pending"
                db.commit()

            if order.status == "pickup" and order.is_label_generated == False:
                order.status = "booked"
                order.sub_status = "booked"
                db.commit()

            # JUST UPDATE NDR STATUS
            if current_status == "delivered":
                print("Here is you can update the status of NDR")

            if current_status == "delivered" and order.delivered_date is None:
                delivered_date = None
                for track in order.tracking_info:
                    if track["status"] == "delivered":
                        delivered_date = track["datetime"]
                        order.delivered_date = parse_datetime(delivered_date)
                        db.add(order)
                        db.flush()

                        break

            # if a cod Order has just been delivered, add it to the cod remittance cycle
            # also move amount from provisional to realised cod
            if (
                current_status == "delivered"
                and order.payment_mode.lower() == "cod"
                and order.cod_remittance_cycle_id == None
            ):

                remittance_id = update_or_create_cod_remittance(order, db)
                order.cod_remittance_cycle_id = remittance_id

                wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

                freight_ded = 0

                wallet.cod_amount += order.total_amount

                wallet.provisional_cod_amount -= order.total_amount

                date = None

                for track in order.tracking_info:
                    if track["status"] == "delivered":
                        date = track["datetime"]
                        break

                date = parse_datetime(date)

                log = {
                    "datetime": date,
                    "transaction_type": "COD Amount",
                    "credit": order.total_amount,
                    "debit": 0,
                    "wallet_balance_amount": wallet.amount,
                    "cod_balance_amount": wallet.cod_amount,
                    "reference": "awb - " + order.awb_number,
                    "client_id": client_id,
                    "wallet_id": wallet.id,
                }

                log = Wallet_Logs(**log)
                db.add(log)
                db.flush()

                # Update the COD remittance for this order cycle
                cod_remittance = (
                    db.query(COD_Remittance)
                    .filter(COD_Remittance.id == remittance_id)
                    .first()
                )

                # Update the database
                db.add(order)
                db.add(wallet)
                if cod_remittance:
                    db.add(cod_remittance)

                db.flush()

            # # if the order has just moved to RTO, add the COD money to the wallet, and apply RTO charges
            if current_status == "RTO" and order.rto_freight == None:

                contract = (
                    db.query(Company_To_Client_Contract)
                    .join(Company_To_Client_Contract.aggregator_courier)
                    .filter(
                        Company_To_Client_Contract.client_id == client_id,
                        Aggregator_Courier.slug == order.courier_partner,
                    )
                    .options(joinedload(Company_To_Client_Contract.aggregator_courier))
                    .first()
                )

                print("contarzcing id")
                print(contract.id)

                rto_freight = ServiceabilityService.calculate_rto_freight(
                    order_id=order.order_id,
                    min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                    additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                    contract_id=contract.id,
                )

                wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

                date = None

                print(order.tracking_info)

                for track in order.tracking_info:
                    if (
                        track["status"] == "RTO"
                        or track["status"] == "RTO in transit"
                        or track["status"] == "RTO initiated"
                    ):
                        date = track["datetime"]
                        break

                print(date)

                date = datetime.now(timezone.utc)

                WalletService.update_wallet(
                    transaction_type="RTO Charge",
                    credit=order.forward_cod_charge * Decimal(1.18),
                    debit=rto_freight["rto_freight"] + rto_freight["rto_tax"],
                    reference=order.awb_number,
                )

                if order.payment_mode.lower() == "cod":
                    wallet.provisional_cod_amount -= order.total_amount

                order.rto_freight = rto_freight["rto_freight"]
                order.rto_tax = rto_freight["rto_tax"]
                order.forward_cod_charge = 0
                order.forward_tax = float(order.forward_freight) * 0.18

                db.add(order)
                db.flush()
                db.commit()
                # except:
                #     continue

            ndr_list = []

            # for act in order.tracking_info:
            #     print(act["status"], "Current status")
            #     if act["status"] == "NDR":
            #         ndr_list.append(act)
            #     if act["status"] == "delivered":
            #         ndr_list.append(act)

            # if len(ndr_list) > 0:
            #     print("MY STATUS IS  is ndr", ndr_list[0]["status"])
            #     NdrService.create_ndr(ndr_list, order)
            # else:
            #     print("ndr is empty ", order.tracking_info)

            for track in reversed(order.tracking_info):

                if track["status"] == "out for pickup" and not order.first_ofp_date:
                    order.first_ofp_date = parse_datetime(track["datetime"])

                if (
                    track["status"] == "picked up"
                    or track["status"] == "pickup completed"
                ) and not order.pickup_completion_date:
                    order.pickup_completion_date = parse_datetime(track["datetime"])

                if track["status"] == "out for delivery" and not order.first_ofd_date:
                    order.first_ofd_date = parse_datetime(track["datetime"])

                if track["status"] == "RTO delivered" and not order.rto_delivered_date:
                    order.rto_delivered_date = parse_datetime(track["datetime"])

            order.last_update_date = parse_datetime(order.tracking_info[0]["datetime"])

            return tracking_response

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
    def external_track_shipment(
        awb_number: str,
    ):

        try:

            db = get_db_session()

            order = db.query(Order).filter(Order.awb_number == awb_number).first()

            if not order:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="AWB Not Found",
                )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="successfull",
                data=Single_Order_Response_Model(**order.to_model().model_dump()),
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

        finally:
            if db:
                db.close()

    @staticmethod
    def external_track_only_shipment(
        awb_number: str,
    ):

        try:

            db = get_db_session()

            order = db.query(Order).filter(Order.awb_number == awb_number).first()

            if not order:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="AWB Not Found",
                )

            body = {
                "awb": order.awb_number,
                "current_status": order.sub_status,
                "order_id": order.order_id,
                "current_timestamp": (
                    order.tracking_info[0]["datetime"]
                    if order.tracking_info
                    else order.booking_date.strftime("%d-%m-%Y %H:%M:%S")
                ),
                "shipment_status": order.sub_status,
                "scans": (
                    [
                        {
                            "datetime": activity["datetime"],
                            "status": activity["status"],
                            "location": activity["location"],
                        }
                        for activity in order.tracking_info
                    ]
                    if order.tracking_info
                    else []
                ),
            }

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="successfull",
                data=body,
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
    def atrack_shipment(
        awb_number: str,
    ):

        try:

            # client_id = context_user_data.get().client_id

            db = get_db_session()

            # order = (
            #     db.query(Order)
            #     .filter(Order.client_id == client_id, Order.awb_number == awb_number)
            #     .first()
            # )

            error_ids = []

            today = datetime.now(timezone.utc).date()

            # Get start and end of yesterday in UTC
            yesterday_start = datetime.combine(
                today - timedelta(days=60), datetime.min.time(), tzinfo=timezone.utc
            )
            yesterday_end = datetime.combine(
                today - timedelta(days=1), datetime.max.time(), tzinfo=timezone.utc
            )

            awbs = [
                "152763150008926",
                "152763150008816",
                "152763150007797",
                "152763150007432",
                "152763150006674",
                "152763150006615",
            ]
            orders = (
                db.query(Order)
                .filter(
                    Order.client_id.in_([413]),
                    # Order.cod_remittance_cycle_id == None,
                    # Order.booking_date != None,
                    # Order.status == "pickup",
                    # Order.cod_remittance_cycle_id == None,
                    # Order.cod_remittance_cycle_id == None,
                    # Add more conditions if needed
                    # Order.order_date > "2025-07-01 00:00:00.000 +0530",
                    # Order.status == "delivered",
                    # Order.cod_remittance_cycle_id == None,
                    # Order.rto_initiated_date == None,
                    # Order.aggregator != "dtdc",
                    # Order.edd == None,
                    # Order.delivered_date == None,
                    # Order.aggregator == "ats",
                    # Order.payment_mode == "prepaid",
                    # Order.courier_partner == "xpressbees 1kg",\
                    # Order.awb_number.in_(awbs),
                    # Order.booking_date >= yesterday_start,
                    # Order.booking_date <= yesterday_end,
                    # Order.status == "pickup",
                    # Order.courier_partner == "ekart 0.5kg",
                    # Order.payment_mode == "COD",
                    # Order.cod_remittance_cycle_id == None,
                    # Order.aggregator != "shiprocket2",
                    # Order.aggregator != "ecom-express",
                    # Order.aggregator == "shiprocket",
                    # Order.aggregator != "shipmozo",
                    # Order.aggregator == "ekart",
                    # Order.awb_number.like("SIT%"),
                    # Order.edd == None,
                    # Order.aggregator == "zippyy",
                    # Order.aggregator != "shiprocket",
                    # Order.aggregator != "dtdc",
                    # Order.aggregator == "shiperfecto",
                    # Order.status == "NDR",
                    Order.cod_remittance_cycle_id == None,
                    # Order.payment_mode != "prepaid",
                    # Order.courier_partner.in_(["ekart 2kg"]),
                    # Order.aggregator.in_(["delhivery", "ats", 'xpressbees']),
                    # Order.aggregator == "shiperfecto",
                    # Order.status == "delivered",
                    # Order.payment_mode != "prepaid",
                    # Order.cod_remittance_cycle_id == None,
                    # Order.payment_mode == "cod",
                    # Order.status == "pickup",
                    # Order.awb_number == 'SF2080745940WAO',
                    # Order.rto_initiated_date == None,
                    # Order.status == "RTO",
                    # Order.status.in_(["in transit", "out for delivery", "NDR"]),
                    # Order.edd == None,
                    # # Order.order_id < "144214",
                    # Order.order_date > "2025-07-30 05:30:00.000 +0530",
                    # Order.order_date <= "2025-08-14 16:32:09.000 +0530",
                )
                .filter(
                    Order.status == "delivered",
                    # Order.status != "new",
                    # Order.status != "cancelled",
                    # Order.sub_status != "RTO delivered",
                    # Order.status != "lost",
                    # Order.status != "RTO",
                    # Order.status != "lost",
                    # # Order.client_id == 25,
                    # Order.aggregator\
                    # == "dtdc",
                )
                .order_by(asc(Order.order_date))
                .all()
            )

            print(len(orders), "ORDERS")

            count = 0

            for order in orders:

                # time.sleep(1)

                print(order.order_id)
                try:

                    client_id = order.client_id

                    context_user_data.set(TempModel(**{"client_id": client_id}))

                    # ShipmentService.update_orders(order)

                    # continue

                    #
                    if (
                        order is None
                        or order.awb_number is None
                        # or order.status == "delivered"
                        # or order.status == "cancelled"
                        # or order.sub_status == "RTO delivered"
                    ):
                        continue
                        # Return error response
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            message="Invalid AWB number",
                        )

                    # ndr_list = []

                    # for act in order.tracking_info:
                    #     if act["status"] == "NDR":
                    #         ndr_list.append(act)

                    # if len(ndr_list) > 0:
                    #     NdrService.create_ndr(ndr_list, order)
                    # else:
                    #     print("ndr is empty ")

                    # continue

                    print(order.awb_number)
                    print(order.order_id)

                    shipping_partner_slug = order.aggregator
                    shipping_partner = courier_service_mapping[shipping_partner_slug]

                    # Use the create order function of the required shipping partner service
                    tracking_response = shipping_partner.track_shipment(
                        order=order,
                        awb_number=order.awb_number,
                    )

                    if not tracking_response or not tracking_response.status:
                        error_ids.append(order.awb_number)
                        print("TRAKCING REPOSNE ISFSDFSDFSDF", tracking_response)
                        continue

                    current_status = tracking_response.data.get("current_status", None)

                    # ndr_list = tracking_response.data.get("data", None)

                    # current_status = order.status

                    print(current_status)

                    # continue

                    if current_status == None:
                        continue

                    if current_status == "delivered" and order.delivered_date is None:
                        delivered_date = None
                        for track in order.tracking_info:
                            if track["status"] == "delivered":
                                delivered_date = track["datetime"]
                                break

                        order.delivered_date = parse_datetime(delivered_date)
                        db.add(order)
                        db.flush()

                    if current_status == "booked" and order.is_label_generated == True:
                        order.status = "pickup"
                        order.booked = "pickup pending"
                        db.commit()

                    if order.status == "pickup" and order.is_label_generated == False:
                        order.status = "booked"
                        order.sub_status = "booked"
                        db.commit()

                    # if a cod Order has just been delivered, add it to the cod remittance cycle
                    # also move amount from provisional to realised cod
                    if (
                        current_status == "delivered"
                        and order.payment_mode.lower() == "cod"
                        and order.cod_remittance_cycle_id == None
                    ):

                        remittance_id = update_or_create_cod_remittance(order, db)
                        order.cod_remittance_cycle_id = remittance_id

                        wallet = (
                            db.query(Wallet)
                            .filter(Wallet.client_id == client_id)
                            .first()
                        )

                        freight_ded = 0

                        wallet.cod_amount += order.total_amount

                        wallet.provisional_cod_amount -= order.total_amount

                        date = None

                        for track in order.tracking_info:
                            if track["status"] == "delivered":
                                date = track["datetime"]
                                break

                        date = parse_datetime(date)

                        log = {
                            "datetime": date,
                            "transaction_type": "COD Amount",
                            "credit": order.total_amount,
                            "debit": 0,
                            "wallet_balance_amount": wallet.amount,
                            "cod_balance_amount": wallet.cod_amount,
                            "reference": "awb - " + order.awb_number,
                            "client_id": client_id,
                            "wallet_id": wallet.id,
                        }

                        log = Wallet_Logs(**log)
                        db.add(log)
                        db.flush()
                        # db.commit()

                        # Update the COD remittance for this order cycle
                        cod_remittance = (
                            db.query(COD_Remittance)
                            .filter(COD_Remittance.id == remittance_id)
                            .first()
                        )

                        # Update the database
                        db.add(order)
                        db.add(wallet)
                        if cod_remittance:
                            db.add(cod_remittance)

                        db.flush()

                        # db.commit()

                    # # if the order has just moved to RTO, add the COD money to the wallet, and apply RTO charges
                    if current_status == "RTO" and order.rto_freight == None:

                        contract = (
                            db.query(Company_To_Client_Contract)
                            .join(Company_To_Client_Contract.aggregator_courier)
                            .filter(
                                Company_To_Client_Contract.client_id == client_id,
                                Aggregator_Courier.slug == order.courier_partner,
                            )
                            .options(
                                joinedload(
                                    Company_To_Client_Contract.aggregator_courier
                                )
                            )
                            .first()
                        )

                        print("contarzcing id")
                        print(contract.id)

                        rto_freight = ServiceabilityService.calculate_rto_freight(
                            order_id=order.order_id,
                            min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                            additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                            contract_id=contract.id,
                        )

                        wallet = (
                            db.query(Wallet)
                            .filter(Wallet.client_id == client_id)
                            .first()
                        )

                        date = None

                        print(rto_freight)

                        for track in reversed(order.tracking_info):
                            if (
                                track["status"] == "RTO"
                                or track["status"] == "RTO in transit"
                                or track["status"] == "RTO initiated"
                                or track["status"] == "RTO delivered"
                            ):

                                date = track["datetime"]
                                print(date)
                                order.rto_initiated_date = parse_datetime(date)

                                break

                        # date = parse_datetime(datetime.now())

                        date = datetime.now(timezone.utc)

                        WalletService.update_wallet(
                            transaction_type="RTO Charge",
                            credit=order.forward_cod_charge * Decimal(1.18),
                            debit=rto_freight["rto_freight"] + rto_freight["rto_tax"],
                            reference=order.awb_number,
                        )

                        if order.payment_mode.lower() == "cod":
                            wallet.provisional_cod_amount -= order.total_amount

                        order.rto_freight = rto_freight["rto_freight"]
                        order.rto_tax = rto_freight["rto_tax"]
                        order.forward_cod_charge = 0
                        order.forward_tax = float(order.forward_freight) * 0.18

                        db.add(order)
                        db.flush()
                        db.commit()

                    # try:
                    #     # ✅ COMPREHENSIVE NDR STATUS HANDLING IN POST_TRACKING

                    #     # Define order_current_status at the beginning for use throughout the NDR processing
                    #     order_current_status = (
                    #         current_status.lower() if current_status else ""
                    #     )

                    #     print(
                    #         f"🔄 Starting NDR processing for order {order.order_id} with status: {order_current_status}"
                    #     )

                    #     # 1. Process NDR events (create new NDR records) - Fixed: Check status in tracking events properly
                    #     ndr_list = []
                    #     for act in order.tracking_info:
                    #         # ✅ FIX: Check for NDR status variations like in backfill
                    #         if act.get("status", "").lower() in [
                    #             "ndr",
                    #             "non delivery report",
                    #             "customer not available",
                    #             "address issue",
                    #             "refused by customer",
                    #         ]:
                    #             ndr_list.append(act)
                    #             print(
                    #                 f"   📦 Found NDR event: {act.get('status')} at {act.get('datetime')}"
                    #             )

                    #     if len(ndr_list) > 0:
                    #         print(
                    #             f"Processing {len(ndr_list)} NDR events for order {order.order_id}"
                    #         )
                    #         try:
                    #             ndr_result = NdrService.create_ndr(ndr_list, order)
                    #             if (
                    #                 ndr_result
                    #                 and hasattr(ndr_result, "status")
                    #                 and not ndr_result.status
                    #             ):
                    #                 logger.warning(
                    #                     f"NDR processing failed for order {order.order_id}: {ndr_result.message}"
                    #                 )
                    #             else:
                    #                 logger.info(
                    #                     f"NDR processing completed for order {order.order_id}"
                    #                 )
                    #         except Exception as ndr_creation_error:
                    #             logger.error(
                    #                 f"Error creating NDR for order {order.order_id}: {ndr_creation_error}"
                    #             )
                    #     else:
                    #         print("No NDR events found in tracking history")

                    #     # ✅ NEW: Handle reattempt to NDR again scenario (in post_tracking)
                    #     # If current status is NDR and there was a previous reattempt, increment attempt count
                    #     if order_current_status in [
                    #         "ndr",
                    #         "non delivery report",
                    #         "customer not available",
                    #         "address issue",
                    #         "refused by customer",
                    #     ]:
                    #         try:
                    #             print(
                    #                 f"   🔄 Checking for existing REATTEMPT NDR for order {order.order_id}"
                    #             )

                    #             # Check if there's an existing NDR in REATTEMPT status
                    #             existing_reattempt_ndr = (
                    #                 db.query(Ndr)
                    #                 .filter(
                    #                     Ndr.order_id == order.id,
                    #                     Ndr.client_id == client_id,
                    #                     Ndr.status == "REATTEMPT",
                    #                 )
                    #                 .first()
                    #             )

                    #             if existing_reattempt_ndr:
                    #                 print(
                    #                     f"   ✅ Found REATTEMPT NDR in post_tracking for order {order.order_id}, converting to NDR with incremented attempt"
                    #                 )

                    #                 # Increment attempt count and change status back to take_action
                    #                 existing_reattempt_ndr.status = "take_action"
                    #                 existing_reattempt_ndr.attempt += 1

                    #                 # Update with latest tracking info
                    #                 latest_tracking = (
                    #                     order.tracking_info[0]
                    #                     if order.tracking_info
                    #                     else {}
                    #                 )
                    #                 if (
                    #                     latest_tracking
                    #                     and "datetime" in latest_tracking
                    #                 ):
                    #                     try:
                    #                         existing_reattempt_ndr.datetime = (
                    #                             latest_tracking["datetime"]
                    #                         )
                    #                     except Exception as date_error:
                    #                         logger.warning(
                    #                             f"Failed to update datetime: {date_error}"
                    #                         )

                    #                 existing_reattempt_ndr.reason = f"Reattempt failed - {latest_tracking.get('description', 'NDR again')}"
                    #                 existing_reattempt_ndr.updated_at = datetime.now(
                    #                     timezone.utc
                    #                 )

                    #                 db.add(existing_reattempt_ndr)
                    #                 db.flush()

                    #                 logger.info(
                    #                     f"Reattempt to NDR processed in post_tracking for order {order.order_id}, attempt count: {existing_reattempt_ndr.attempt}"
                    #                 )

                    #                 # ✅ FIX: Create history entry like in webhook method
                    #                 try:
                    #                     from modules.ndr_history.ndr_history_service import (
                    #                         NdrHistoryService,
                    #                     )

                    #                     NdrHistoryService.create_ndr_history(
                    #                         (
                    #                             [latest_tracking]
                    #                             if latest_tracking
                    #                             else []
                    #                         ),
                    #                         order.id,
                    #                         existing_reattempt_ndr.id,
                    #                     )
                    #                 except Exception as history_error:
                    #                     logger.error(
                    #                         f"Failed to create NDR history for reattempt to NDR: {history_error}"
                    #                     )

                    #         except Exception as e:
                    #             logger.error(
                    #                 f"Error processing reattempt to NDR in post_tracking for order {order.order_id}: {e}"
                    #             )

                    #     # 2. ✅ NEW: Handle status transitions for existing NDR records
                    #     # Check if current status is RTO/Delivered and update existing NDR
                    #     if order_current_status in ["delivered", "rto"]:
                    #         try:
                    #             print(
                    #                 f"   🎯 Processing {order_current_status.upper()} status for order {order.order_id}"
                    #             )

                    #             existing_ndr = (
                    #                 db.query(Ndr)
                    #                 .filter(
                    #                     Ndr.order_id == order.id,
                    #                     Ndr.client_id == client_id,
                    #                 )
                    #                 .first()
                    #             )

                    #             if existing_ndr and existing_ndr.status not in [
                    #                 "DELIVERED",
                    #                 "RTO",
                    #             ]:
                    #                 print(
                    #                     f"   ✅ Updating NDR status from {existing_ndr.status} to {order_current_status.upper()} for order {order.order_id}"
                    #                 )

                    #                 # Update NDR status to final state
                    #                 if order_current_status == "delivered":
                    #                     existing_ndr.status = "DELIVERED"
                    #                 elif order_current_status == "rto":
                    #                     existing_ndr.status = "RTO"

                    #                 # Update with latest tracking info
                    #                 latest_tracking = (
                    #                     order.tracking_info[0]
                    #                     if order.tracking_info
                    #                     else {}
                    #                 )
                    #                 if latest_tracking.get("datetime"):
                    #                     existing_ndr.datetime = latest_tracking[
                    #                         "datetime"
                    #                     ]

                    #                 existing_ndr.updated_at = datetime.now(timezone.utc)
                    #                 db.add(existing_ndr)
                    #                 db.flush()

                    #                 logger.info(
                    #                     f"NDR status updated to {order_current_status.upper()} for order {order.order_id}"
                    #                 )

                    #                 # ✅ FIX: Create history entry for final status like in webhook method
                    #                 try:
                    #                     from modules.ndr_history.ndr_history_service import (
                    #                         NdrHistoryService,
                    #                     )

                    #                     NdrHistoryService.create_ndr_history(
                    #                         (
                    #                             [latest_tracking]
                    #                             if latest_tracking
                    #                             else []
                    #                         ),
                    #                         order.id,
                    #                         existing_ndr.id,
                    #                     )
                    #                 except Exception as history_error:
                    #                     logger.error(
                    #                         f"Failed to create NDR history for final status: {history_error}"
                    #                     )
                    #             else:
                    #                 if existing_ndr:
                    #                     print(
                    #                         f"   ⏭️  NDR already in final state: {existing_ndr.status}"
                    #                     )
                    #                 else:
                    #                     print(
                    #                         f"   ⚠️  No existing NDR found for {order_current_status} status"
                    #                     )

                    #         except Exception as e:
                    #             logger.error(
                    #                 f"Error updating NDR for delivered/RTO in post_tracking: {e}"
                    #             )

                    #     # 3. Check if current status triggers auto-reattempt
                    #     elif order_current_status in [
                    #         "out for delivery",
                    #         "ofd",
                    #         "in transit",
                    #         "intransit",
                    #         "in_transit",
                    #     ]:
                    #         try:
                    #             print(
                    #                 f"   🚚 Processing auto-reattempt for {order_current_status} status"
                    #             )

                    #             existing_ndr = (
                    #                 db.query(Ndr)
                    #                 .filter(
                    #                     Ndr.order_id == order.id,
                    #                     Ndr.client_id == client_id,
                    #                     Ndr.status
                    #                     == "take_action",  # Only for pending NDRs
                    #                 )
                    #                 .first()
                    #             )

                    #             if existing_ndr:
                    #                 print(
                    #                     f"   ✅ Auto-reattempt triggered in post_tracking for order {order.order_id}"
                    #                 )

                    #                 existing_ndr.status = "REATTEMPT"
                    #                 existing_ndr.attempt += 1

                    #                 # Update with latest tracking info
                    #                 latest_tracking = (
                    #                     order.tracking_info[0]
                    #                     if order.tracking_info
                    #                     else {}
                    #                 )
                    #                 if latest_tracking.get("datetime"):
                    #                     existing_ndr.datetime = latest_tracking[
                    #                         "datetime"
                    #                     ]
                    #                 if latest_tracking.get("description"):
                    #                     existing_ndr.reason = f"Auto reattempt - {latest_tracking['description']}"

                    #                 existing_ndr.updated_at = datetime.now(timezone.utc)
                    #                 db.add(existing_ndr)
                    #                 db.flush()

                    #                 logger.info(
                    #                     f"Auto-reattempt processed in post_tracking for order {order.order_id}"
                    #                 )

                    #                 # ✅ FIX: Create history entry for auto-reattempt
                    #                 try:
                    #                     from modules.ndr_history.ndr_history_service import (
                    #                         NdrHistoryService,
                    #                     )

                    #                     NdrHistoryService.create_ndr_history(
                    #                         (
                    #                             [latest_tracking]
                    #                             if latest_tracking
                    #                             else []
                    #                         ),
                    #                         order.id,
                    #                         existing_ndr.id,
                    #                     )
                    #                 except Exception as history_error:
                    #                     logger.error(
                    #                         f"Failed to create NDR history for auto-reattempt: {history_error}"
                    #                     )
                    #             else:
                    #                 print(
                    #                     f"   ⚠️  No pending NDR found for auto-reattempt"
                    #                 )

                    #         except Exception as e:
                    #             logger.error(
                    #                 f"Error processing auto-reattempt in post_tracking: {e}"
                    #             )

                    #     print(f"✅ NDR processing completed for order {order.order_id}")

                    # except Exception as e:
                    #     logger.error(
                    #         extra=context_user_data.get(),
                    #         msg=f"Error processing NDR for order {order.order_id if order else 'Unknown'}: {str(e)}",
                    #     )
                    #     print(
                    #         f"❌ Error processing NDR for order {order.order_id if order else 'Unknown'}: {e}"
                    #     )

                    # db.commit()

                    # try:

                    #     for track in reversed(order.tracking_info):

                    #         # if (
                    #         #     track["status"] == "out for pickup"
                    #         #     and not order.first_ofp_date
                    #         # ):
                    #         #     order.first_ofp_date = parse_datetime(track["datetime"])

                    #         # if (
                    #         #     track["status"] == "picked up"
                    #         #     or track["status"] == "pickup completed"
                    #         # ) and not order.pickup_completion_date:
                    #         #     order.pickup_completion_date = parse_datetime(
                    #         #         track["datetime"]
                    #         #     )

                    #         # if (
                    #         #     track["status"] == "out for delivery"
                    #         #     and not order.first_ofd_date
                    #         # ):
                    #         #     order.first_ofd_date = parse_datetime(track["datetime"])

                    #         if (
                    #             track["status"] == "RTO"
                    #             or track["status"] == "RTO in transit"
                    #             or track["status"] == "RTO initiated"
                    #             or track["status"] == "RTO delivered"
                    #         ) and order.rto_initiated_date is None:
                    #             order.rto_initiated_date = parse_datetime(
                    #                 track["datetime"]
                    #             )

                    #         if (
                    #             track["status"] == "delivered"
                    #             and order.delivered_date is None
                    #         ):
                    #             order.delivered_date = parse_datetime(track["datetime"])

                    #         # if (
                    #         #     track["status"] == "RTO delivered"
                    #         #     and not order.rto_delivered_date
                    #         # ):
                    #         #     order.rto_delivered_date = parse_datetime(
                    #         #         track["datetime"]
                    #         #     )

                    #     order.last_update_date = parse_datetime(
                    #         order.tracking_info[0]["datetime"]
                    #     )
                    #     db.add(order)
                    #     db.flush()

                    #     db.commit()

                    # except:
                    #     pass

                except:
                    count = count + 1
                    error_ids.append(order.awb_number)
                    continue

            db.commit()

            print("COUNT", count)
            print("ERROR IDS", error_ids)

            return True

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

        # finally:
        #     if db:
        #         db.close()

    @staticmethod
    def cancel_shipments(
        awb_numbers: List[str],
    ):

        try:
            for awb_number in awb_numbers:

                try:

                    client_id = context_user_data.get().client_id

                    count = 0

                    db = get_db_session()

                    order = (
                        db.query(Order)
                        .filter(
                            Order.client_id == client_id, Order.awb_number == awb_number
                        )
                        .first()
                    )

                    if order is None:
                        # Return error response
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            message="Invalid AWB number",
                        )

                    if order.status not in {
                        "new",
                        "booked",
                    } and not (  # Allow "new" & "booked"
                        order.status == "pickup"
                        and order.sub_status not in {"picked up", "pickup completed"}
                    ):  # Allow "pickup" only if sub_status is NOT these
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            message="Cannot cancel shipment in transit",
                        )

                    shipping_partner_slug = order.aggregator
                    shipping_partner = courier_service_mapping[shipping_partner_slug]

                    try:
                        # Use the create order function of the required shipping partner service
                        cancel_response = shipping_partner.cancel_shipment(
                            order=order, awb_number=awb_number
                        )

                        print(cancel_response)
                    except:
                        pass

                    if cancel_response.status == False:
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            message="Could not cancel shipment",
                        )

                    order.status = "new"
                    order.sub_status = "new"

                    order.is_label_generated = False
                    order.cancel_count = order.cancel_count + 1

                    order.tracking_info = []

                    new_activity = {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "message": "Shipment Cancelled",
                        "data": {
                            "aggregator": order.aggregator,
                            "courier_partner": order.courier_partner,
                            "awb_number": awb_number,
                        },
                        "user_data": context_user_data.get().id,
                    }
                    order.action_history.append(new_activity)

                    order.aggregator = None
                    order.courier_partner = None
                    order.courier_status = None
                    order.awb_number = None
                    order.shipping_partner_order_id = None
                    order.shipping_partner_shipping_id = None

                    total_freight_to_refund = (
                        order.forward_freight
                        + order.forward_cod_charge
                        + order.forward_tax
                    )

                    WalletService.update_wallet(
                        transaction_type="Shipment Cancelled",
                        credit=total_freight_to_refund,
                        debit=0,
                        reference="awb - " + awb_number,
                    )

                    order.forward_freight = None
                    order.forward_cod_charge = None
                    order.forward_tax = None

                    db.add(order)
                    db.flush()
                    db.commit()

                except:
                    continue

            return GenericResponseModel(
                status=True, status_code=200, message="cancelled successfully"
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error Cancelling shipment: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not cancel shipment.",
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
    def download_manifest(order_ids: List[str]):
        try:

            with get_db_session() as db:

                client_id = context_user_data.get().client_id

                # fetch the order details from the order
                orders = (
                    db.query(Order)
                    .filter(Order.order_id.in_(order_ids), Order.client_id == client_id)
                    .order_by(desc(Order.created_at))
                    .options(joinedload(Order.pickup_location))
                )

                manifest_html = generate_manifest(orders)
                pdf_buffer = ShipmentService.convert_html_to_pdf(manifest_html)

                pdf_buffer.seek(0)

                # Return the PDF as a downloadable file
                return base64.b64encode(pdf_buffer.getvalue()).decode("utf-8")

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="could not calculate zone: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not calculate zone",
            )

    @staticmethod
    def get_shipping_charges(shipping_charges: ShippingChargesGetSchema):

        try:

            client_id = context_user_data.get().client_id
            company_id = context_user_data.get().company_id

            db = get_db_session()

            # print(jsonable_encoder(shipping_charges))

            # destruct the filters
            page_number = shipping_charges.selectedPageNumber
            batch_size = shipping_charges.batchSize

            # total_count = query.count()
            # Step 1: Build base query
            base_query = (
                db.query(Order)
                .filter(
                    Order.client_id == client_id,
                    Order.company_id == company_id,
                    Order.status.notin_(["new", "cancelled"]),
                )
                .order_by(desc(Order.order_date))
            )

            # Step 2: Count total matching records
            total_count = base_query.count()

            # Step 3: Apply pagination
            offset_value = (page_number - 1) * batch_size
            orders = base_query.offset(offset_value).limit(batch_size).all()

            # Step 4: Map to schema
            shipping_charges = [
                ShippingChargesSchema(**order.to_model().model_dump())
                for order in orders
            ]

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders fetched Successfully",
                data={
                    "shipping_charges": shipping_charges,
                    "total_count": total_count,
                },
                status=True,
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="could not calculate zone: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not calculate zone",
            )

    @staticmethod
    def update_orders(order):

        try:

            db = get_db_session()

            track = order.tracking_info

            if track is None or len(track) == 0:
                return

            booking_date = track[0]["datetime"]

            booking_date = parse_datetime(booking_date)

            order.booking_date = booking_date

            date = None

            for t in track:
                if t["status"].lower() == "delivered":
                    date = t["datetime"]
                    break

            if date:
                delivered_date = parse_datetime(date)
                order.delivered_date = delivered_date

            db.add(order)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders fetched Successfully",
                status=True,
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fecthing Order: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetchin the Orders.",
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
    def atrack_shipment(awb_number: str):
        """
        Backfill NDR data for existing shipments that are currently in NDR status
        but don't have proper NDR records. This method will:
        1. Find all orders with current status = NDR
        2. Analyze their tracking history to calculate attempt counts
        3. Create proper NDR records with correct attempt counts
        4. Create NDR history entries
        """

        try:
            print("🔄 Starting NDR Data Backfill Process...")
            print("=" * 60)

            db = get_db_session()

            # Step 1: Find all orders currently in NDR status
            ndr_orders = (
                db.query(Order)
                .filter(
                    Order.status.in_(
                        [
                            "NDR",
                            "ndr",
                            "non delivery report",
                            "customer not available",
                            "address issue",
                            "refused by customer",
                        ]
                    ),
                    Order.awb_number.isnot(None),  # Must have AWB number
                )
                .order_by(Order.order_date.desc())
                # .limit(1000)  # Process in batches to avoid overwhelming the system
                .all()
            )

            print(f"📊 Found {len(ndr_orders)} orders currently in NDR status")

            backfill_stats = {
                "processed": 0,
                "created_ndr_records": 0,
                "updated_existing_records": 0,
                "errors": 0,
                "skipped": 0,
            }

            for order in ndr_orders:
                try:
                    backfill_stats["processed"] += 1
                    client_id = order.client_id

                    print(
                        f"\n🔍 Processing Order {order.order_id} (AWB: {order.awb_number})"
                    )

                    # Step 2: Check if NDR record already exists
                    existing_ndr = (
                        db.query(Ndr)
                        .filter(Ndr.order_id == order.id, Ndr.client_id == client_id)
                        .first()
                    )

                    if existing_ndr:
                        print(
                            f"   ⏭️  NDR record already exists for order {order.order_id}, skipping"
                        )
                        backfill_stats["skipped"] += 1
                        continue

                    # Step 3: Analyze tracking history to calculate attempt count
                    tracking_history = (
                        order.tracking_info if order.tracking_info else []
                    )
                    ndr_attempt_count = ShipmentService._calculate_ndr_attempt_count(
                        tracking_history
                    )

                    print(f"   📈 Calculated attempt count: {ndr_attempt_count}")

                    # Step 4: Find the most recent NDR event in tracking history
                    latest_ndr_event = None
                    for event in reversed(tracking_history):
                        if event.get("status", "").lower() in [
                            "ndr",
                            "non delivery report",
                            "customer not available",
                            "address issue",
                            "refused by customer",
                        ]:
                            latest_ndr_event = event
                            break

                    if not latest_ndr_event:
                        print(
                            f"   ⚠️  No NDR event found in tracking history for order {order.order_id}"
                        )
                        backfill_stats["errors"] += 1
                        continue

                    # Step 5: Create NDR record
                    ndr_data = {
                        "order_id": order.id,
                        "client_id": client_id,
                        "awb": order.awb_number,
                        "status": "take_action",  # Default status for backfilled NDRs
                        "datetime": latest_ndr_event.get(
                            "datetime", datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                        ),
                        "attempt": ndr_attempt_count,
                        "reason": latest_ndr_event.get(
                            "description", "Customer not available - Backfilled NDR"
                        ),
                        "alternate_phone_number": None,
                        "address": None,
                    }

                    new_ndr = Ndr.create_db_entity(ndr_data)
                    new_ndr.created_at = datetime.now(timezone.utc)
                    new_ndr.updated_at = datetime.now(timezone.utc)

                    db.add(new_ndr)
                    db.flush()  # Get the NDR ID

                    print(f"   ✅ Created NDR record with ID {new_ndr.id}")
                    backfill_stats["created_ndr_records"] += 1

                    # Step 6: Create NDR history entries for all NDR events in tracking
                    try:
                        ndr_events = [
                            event
                            for event in tracking_history
                            if event.get("status", "").lower()
                            in [
                                "ndr",
                                "non delivery report",
                                "customer not available",
                                "address issue",
                                "refused by customer",
                            ]
                        ]

                        if ndr_events:
                            from modules.ndr_history.ndr_history_service import (
                                NdrHistoryService,
                            )

                            NdrHistoryService.create_ndr_history(
                                ndr_events, order.id, new_ndr.id
                            )
                            print(
                                f"   📝 Created history entries for {len(ndr_events)} NDR events"
                            )

                    except Exception as history_error:
                        print(f"   ⚠️  Failed to create NDR history: {history_error}")
                        # Continue processing even if history creation fails

                    # Commit after each successful NDR creation
                    db.commit()

                    if backfill_stats["processed"] % 10 == 0:
                        print(
                            f"   📊 Progress: {backfill_stats['processed']} orders processed"
                        )

                except Exception as order_error:
                    print(
                        f"   ❌ Error processing order {order.order_id}: {order_error}"
                    )
                    backfill_stats["errors"] += 1
                    db.rollback()
                    continue

            # Final statistics
            print(f"\n🎉 NDR Backfill Process Complete!")
            print(f"📊 Statistics:")
            print(f"   Total Orders Processed: {backfill_stats['processed']}")
            print(f"   NDR Records Created: {backfill_stats['created_ndr_records']}")
            print(f"   Orders Skipped (already had NDR): {backfill_stats['skipped']}")
            print(f"   Errors: {backfill_stats['errors']}")

            return {
                "status": True,
                "message": "NDR backfill completed successfully",
                "stats": backfill_stats,
            }

        except Exception as e:
            print(f"❌ Critical error in NDR backfill process: {e}")
            return {
                "status": False,
                "message": f"NDR backfill failed: {str(e)}",
                "stats": backfill_stats if "backfill_stats" in locals() else {},
            }

        finally:
            if "db" in locals():
                db.close()

    @staticmethod
    def _calculate_ndr_attempt_count(tracking_history):
        """
        Calculate NDR attempt count by analyzing tracking history
        Logic:
        1. Count all NDR events
        2. Count delivery attempts (out for delivery events followed by NDR)
        3. Count reattempt cycles
        """

        if not tracking_history:
            return 1

        ndr_count = 0
        delivery_attempts = 0
        last_was_ofd = False

        for event in tracking_history:
            status = event.get("status", "").lower()

            # Count NDR events
            if status in [
                "ndr",
                "non delivery report",
                "customer not available",
                "address issue",
                "refused by customer",
            ]:
                ndr_count += 1

                # If previous event was out for delivery, this counts as a delivery attempt
                if last_was_ofd:
                    delivery_attempts += 1

                last_was_ofd = False

            # Track out for delivery events
            elif status in ["out for delivery", "ofd"]:
                last_was_ofd = True

            else:
                last_was_ofd = False

        # Attempt count is the maximum of NDR events or delivery attempts, minimum 1
        attempt_count = max(ndr_count, delivery_attempts, 1)

        print(
            f"   🔍 Tracking analysis: {ndr_count} NDR events, {delivery_attempts} delivery attempts → {attempt_count} attempts"
        )

        return attempt_count

    @staticmethod
    def webhook_track_shipment(awb_number: str, credentials=None):

        try:

            with get_db_session() as db:

                order = db.query(Order).filter(Order.awb_number == awb_number).first()

                client_id = order.client_id

                context_user_data.set(TempModel(**{"client_id": client_id}))

                #
                if order is None or order.awb_number is None:
                    # continue
                    # Return error response
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid AWB number",
                    )

                print(order.awb_number)
                print(order.order_id)

                shipping_partner_slug = order.aggregator
                shipping_partner = courier_service_mapping[shipping_partner_slug]

                # Use the create order function of the required shipping partner service

                if credentials:
                    tracking_response = shipping_partner.track_shipment(
                        order=order,
                        awb_number=order.awb_number,
                        credentials=credentials,
                    )

                else:

                    tracking_response = shipping_partner.track_shipment(
                        order=order, awb_number=order.awb_number
                    )

                current_status = tracking_response.data.get("current_status", None)

                print(current_status)

                # if current_status == None:
                #     continue

                # if (
                #     order.sub_status == "picked up"
                #     or order.sub_status == "pickup completed"
                # ) and client_id == 89:
                #     ShippingNotificaitions.send_notification(order, "order_shipped")

                try:

                    if client_id == 93:

                        body = {
                            "awb": order.awb_number,
                            "current_status": order.sub_status,
                            "order_id": order.order_id,
                            "current_timestamp": (
                                order.tracking_info[0]["datetime"]
                                if order.tracking_info
                                else order.booking_date.strftime("%d-%m-%Y %H:%M:%S")
                            ),
                            "shipment_status": order.sub_status,
                            "scans": [
                                {
                                    "datetime": activity["datetime"],
                                    "status": activity["status"],
                                    "location": activity["location"],
                                }
                                for activity in order.tracking_info
                            ],
                        }

                        response = requests.post(
                            url="https://wtpzsmej1h.execute-api.ap-south-1.amazonaws.com/prod/webhook/bluedart",
                            verify=True,
                            timeout=10,
                            json=body,
                        )

                        print(response.json())
                except:
                    pass

                if current_status == "booked" and order.is_label_generated == True:
                    order.status = "pickup"
                    order.booked = "pickup pending"
                    db.commit()

                if order.status == "pickup" and order.is_label_generated == False:
                    order.status = "booked"
                    order.sub_status = "booked"
                    db.commit()

                if current_status == "delivered" and order.delivered_date is None:
                    delivered_date = None
                    for track in order.tracking_info:
                        if track["status"] == "delivered":
                            delivered_date = track["datetime"]
                            break

                    order.delivered_date = parse_datetime(delivered_date)
                    db.add(order)
                    db.flush()

                # if a cod Order has just been delivered, add it to the cod remittance cycle
                # also move amount from provisional to realised cod
                if (
                    current_status == "delivered"
                    and order.payment_mode.lower() == "cod"
                    and order.cod_remittance_cycle_id == None
                ):

                    remittance_id = update_or_create_cod_remittance(order, db)
                    order.cod_remittance_cycle_id = remittance_id

                    wallet = (
                        db.query(Wallet).filter(Wallet.client_id == client_id).first()
                    )

                    freight_ded = 0

                    wallet.cod_amount += order.total_amount

                    wallet.provisional_cod_amount -= order.total_amount

                    date = None

                    for track in order.tracking_info:
                        if track["status"] == "delivered":
                            date = track["datetime"]
                            break

                    date = parse_datetime(date)

                    log = {
                        "datetime": date,
                        "transaction_type": "COD Amount",
                        "credit": order.total_amount,
                        "debit": 0,
                        "wallet_balance_amount": wallet.amount,
                        "cod_balance_amount": wallet.cod_amount,
                        "reference": "awb - " + order.awb_number,
                        "client_id": client_id,
                        "wallet_id": wallet.id,
                    }

                    log = Wallet_Logs(**log)
                    db.add(log)
                    db.flush()

                    # Update the COD remittance for this order cycle
                    cod_remittance = (
                        db.query(COD_Remittance)
                        .filter(COD_Remittance.id == remittance_id)
                        .first()
                    )

                    # Update the database
                    db.add(order)
                    db.add(wallet)
                    if cod_remittance:
                        db.add(cod_remittance)

                    db.flush()

                # # if the order has just moved to RTO, add the COD money to the wallet, and apply RTO charges
                if current_status == "RTO" and order.rto_freight == None:

                    contract = (
                        db.query(Company_To_Client_Contract)
                        .join(Company_To_Client_Contract.aggregator_courier)
                        .filter(
                            Company_To_Client_Contract.client_id == client_id,
                            Aggregator_Courier.slug == order.courier_partner,
                        )
                        .options(
                            joinedload(Company_To_Client_Contract.aggregator_courier)
                        )
                        .first()
                    )

                    print("contarzcing id")
                    print(contract.id)

                    rto_freight = ServiceabilityService.calculate_rto_freight(
                        order_id=order.order_id,
                        min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                        additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                        contract_id=contract.id,
                    )

                    wallet = (
                        db.query(Wallet).filter(Wallet.client_id == client_id).first()
                    )

                    date = None

                    print(order.tracking_info)

                    for track in order.tracking_info:
                        if (
                            track["status"] == "RTO"
                            or track["status"] == "RTO in transit"
                        ):
                            date = track["datetime"]
                            break

                    print(date)

                    date = datetime.now(timezone.utc)

                    WalletService.update_wallet(
                        transaction_type="RTO Charge",
                        credit=order.forward_cod_charge * Decimal(1.18),
                        debit=round(
                            rto_freight["rto_freight"] + rto_freight["rto_tax"], 2
                        ),
                        reference=order.awb_number,
                    )

                    if order.payment_mode.lower() == "cod":
                        wallet.provisional_cod_amount -= order.total_amount

                    order.rto_freight = round(rto_freight["rto_freight"], 2)
                    order.rto_tax = round(rto_freight["rto_tax"], 2)
                    order.forward_cod_charge = 0
                    order.forward_tax = round(float(order.forward_freight) * 0.18, 2)

                    db.add(order)
                    db.flush()
                    db.commit()
                    # except:
                    #     continue

                # ✅ COMPREHENSIVE NDR STATUS HANDLING - ISOLATED ERROR HANDLING
                # Wrap all NDR processing in try-catch to prevent failures from affecting other tracking operations
                try:
                    print(f"Starting NDR processing for order {order.order_id}")

                    # Handle different tracking scenarios for NDR flow

                    # 1. Process NDR events (create new NDR records)
                    ndr_list = []
                    for act in order.tracking_info:
                        if act.get("status") == "NDR":
                            ndr_list.append(act)

                    if len(ndr_list) > 0:
                        print(
                            f"Processing {len(ndr_list)} NDR events for order {order.order_id}"
                        )
                        try:
                            ndr_result = NdrService.create_ndr(ndr_list, order)
                            if (
                                ndr_result
                                and hasattr(ndr_result, "status")
                                and not ndr_result.status
                            ):
                                logger.warning(
                                    f"NDR processing failed for order {order.order_id}: {ndr_result.message}"
                                )
                            else:
                                logger.info(
                                    f"NDR processing completed for order {order.order_id}"
                                )
                        except Exception as ndr_error:
                            logger.error(
                                f"Error processing NDR creation for order {order.order_id}: {ndr_error}"
                            )

                    # ✅ NEW: Handle reattempt to NDR again scenario
                    # If current status is NDR and there was a previous reattempt, increment attempt count
                    if current_status.lower() in [
                        "ndr",
                        "non delivery report",
                        "customer not available",
                        "address issue",
                        "refused by customer",
                    ]:
                        try:
                            # Check if there's an existing NDR in REATTEMPT status
                            existing_reattempt_ndr = (
                                db.query(Ndr)
                                .filter(
                                    Ndr.order_id == order.id,
                                    Ndr.client_id == client_id,
                                    Ndr.status == "REATTEMPT",
                                )
                                .first()
                            )

                            if existing_reattempt_ndr:
                                print(
                                    f"Found REATTEMPT NDR for order {order.order_id}, converting to NDR with incremented attempt"
                                )

                                # Increment attempt count and change status back to take_action
                                existing_reattempt_ndr.status = "take_action"
                                existing_reattempt_ndr.attempt += 1

                                # Update with latest tracking info
                                latest_tracking = (
                                    order.tracking_info[0]
                                    if order.tracking_info
                                    else {}
                                )
                                if latest_tracking and "datetime" in latest_tracking:
                                    try:
                                        existing_reattempt_ndr.datetime = (
                                            latest_tracking["datetime"]
                                        )
                                    except:
                                        pass

                                existing_reattempt_ndr.reason = f"Reattempt failed - {latest_tracking.get('description', 'NDR again')}"
                                existing_reattempt_ndr.updated_at = datetime.now(
                                    timezone.utc
                                )

                                db.commit()

                                logger.info(
                                    f"Reattempt to NDR processed for order {order.order_id}, attempt count: {existing_reattempt_ndr.attempt}"
                                )

                                # Create history entry
                                try:
                                    NdrHistoryService.create_ndr_history(
                                        [latest_tracking],
                                        order.id,
                                        existing_reattempt_ndr.id,
                                    )
                                except Exception as history_error:
                                    logger.error(
                                        f"Failed to create NDR history for reattempt to NDR: {history_error}"
                                    )

                        except Exception as e:
                            logger.error(
                                f"Error processing reattempt to NDR for order {order.order_id}: {e}"
                            )

                    # 2. ✅ NEW: Check if current status is RTO/Delivered and there was an NDR before
                    current_status_lower = (
                        current_status.lower() if current_status else ""
                    )

                    if current_status_lower in ["delivered", "rto"]:
                        try:
                            # Check if there's an existing NDR record for this order
                            existing_ndr = (
                                db.query(Ndr)
                                .filter(
                                    Ndr.order_id == order.id, Ndr.client_id == client_id
                                )
                                .first()
                            )

                            if existing_ndr and existing_ndr.status not in [
                                "DELIVERED",
                                "RTO",
                            ]:
                                print(
                                    f"Found existing NDR for order {order.order_id}, updating status to {current_status_lower.upper()}"
                                )

                                # Update NDR status to match the order status
                                if current_status_lower == "delivered":
                                    existing_ndr.status = "DELIVERED"
                                    logger.info(
                                        f"Updated NDR status to DELIVERED for order {order.order_id}"
                                    )
                                elif current_status_lower == "rto":
                                    existing_ndr.status = "RTO"
                                    logger.info(
                                        f"Updated NDR status to RTO for order {order.order_id}"
                                    )

                                # Update timestamp and attempt info
                                latest_tracking = (
                                    order.tracking_info[0]
                                    if order.tracking_info
                                    else {}
                                )
                                if latest_tracking.get("datetime"):
                                    existing_ndr.datetime = latest_tracking["datetime"]

                                existing_ndr.updated_at = datetime.now(timezone.utc)
                                db.add(existing_ndr)
                                db.flush()

                                # Create history entry for the status change
                                try:
                                    NdrHistoryService.create_ndr_history(
                                        [latest_tracking], order.id, existing_ndr.id
                                    )
                                except Exception as history_error:
                                    logger.error(
                                        f"Failed to create NDR history for status update: {history_error}"
                                    )

                        except Exception as e:
                            logger.error(
                                f"Error updating NDR status for delivered/RTO order {order.order_id}: {e}"
                            )

                    # 3. ✅ NEW: Check if current status is OFD/In-Transit and there was an NDR before (Auto Reattempt)
                    elif current_status_lower in [
                        "out for delivery",
                        "ofd",
                        "in transit",
                        "intransit",
                        "in_transit",
                    ]:
                        try:
                            # Check if there's an existing NDR record that's not already in reattempt
                            existing_ndr = (
                                db.query(Ndr)
                                .filter(
                                    Ndr.order_id == order.id,
                                    Ndr.client_id == client_id,
                                    Ndr.status.in_(
                                        ["take_action"]
                                    ),  # Only trigger for NDRs that haven't been addressed
                                )
                                .first()
                            )

                            if existing_ndr:
                                print(
                                    f"Auto-reattempt triggered for order {order.order_id} - status changed from NDR to {current_status}"
                                )

                                # Auto-update to REATTEMPT status
                                existing_ndr.status = "REATTEMPT"
                                existing_ndr.attempt += 1

                                # Update with latest tracking info
                                latest_tracking = (
                                    order.tracking_info[0]
                                    if order.tracking_info
                                    else {}
                                )
                                if latest_tracking.get("datetime"):
                                    existing_ndr.datetime = latest_tracking["datetime"]
                                if latest_tracking.get("description"):
                                    existing_ndr.reason = f"Auto reattempt - {latest_tracking['description']}"

                                existing_ndr.updated_at = datetime.now(timezone.utc)
                                db.add(existing_ndr)
                                db.flush()

                                logger.info(
                                    f"Auto-reattempt processed for order {order.order_id}, attempt: {existing_ndr.attempt}"
                                )

                                # Create history entry for the auto-reattempt
                                try:
                                    NdrHistoryService.create_ndr_history(
                                        [latest_tracking], order.id, existing_ndr.id
                                    )
                                except Exception as history_error:
                                    logger.error(
                                        f"Failed to create NDR history for auto-reattempt: {history_error}"
                                    )

                        except Exception as e:
                            logger.error(
                                f"Error processing auto-reattempt for order {order.order_id}: {e}"
                            )

                    else:
                        print(
                            f"No NDR status updates needed for order {order.order_id} with status {current_status}"
                        )

                    logger.info(
                        f"NDR processing completed successfully for order {order.order_id}"
                    )

                except Exception as ndr_processing_error:
                    # ✅ CRITICAL: Isolate NDR failures from affecting other tracking operations
                    logger.error(
                        f"❌ NDR processing failed for order {order.order_id}: {ndr_processing_error}"
                    )
                    logger.error(
                        f"🔄 Continuing with other tracking operations despite NDR failure"
                    )
                    # Continue processing other tracking operations even if NDR fails
                    pass

                # ✅ CONTINUE WITH OTHER TRACKING OPERATIONS (ISOLATED FROM NDR FAILURES)
                try:
                    print(
                        f"Processing standard tracking operations for order {order.order_id}"
                    )

                    for track in reversed(order.tracking_info):

                        if (
                            track["status"] == "out for pickup"
                            and not order.first_ofp_date
                        ):
                            order.first_ofp_date = parse_datetime(track["datetime"])

                        if (
                            track["status"] == "picked up"
                            or track["status"] == "pickup completed"
                        ) and not order.pickup_completion_date:
                            order.pickup_completion_date = parse_datetime(
                                track["datetime"]
                            )

                        if (
                            track["status"] == "out for delivery"
                            and not order.first_ofd_date
                        ):
                            order.first_ofd_date = parse_datetime(track["datetime"])

                        if (
                            track["status"] == "RTO delivered"
                            and not order.rto_delivered_date
                        ):
                            order.rto_delivered_date = parse_datetime(track["datetime"])

                    order.last_update_date = parse_datetime(
                        order.tracking_info[0]["datetime"]
                    )
                    db.add(order)
                    db.flush()

                    logger.info(
                        f"Standard tracking operations completed successfully for order {order.order_id}"
                    )

                except Exception as tracking_error:
                    logger.error(
                        f"❌ Standard tracking operations failed for order {order.order_id}: {tracking_error}"
                    )
                    logger.info(f"🔄 Continuing despite standard tracking failure")
                    # Continue with commit even if standard tracking fails
                    pass

                db.commit()
                db.close()

                return True

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error posting shipment: {}".format(str(e)),
            )

            db.close()

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

            db.close()

            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

        finally:
            # db = get_db_session()
            if db:
                db.close()

    @staticmethod
    def post_tracking(order: Order_Model):

        try:
            current_status = order.status
            client_id = order.client_id

            context_user_data.set(TempModel(**{"client_id": client_id}))

            print(current_status)

            if current_status == None:
                return

            print(1)

            db = get_db_session()

            if current_status == "booked" and order.is_label_generated == True:
                order.status = "pickup"
                order.booked = "pickup pending"

            if order.status == "pickup" and order.is_label_generated == False:
                order.status = "booked"
                order.sub_status = "booked"

            # if a cod Order has just been delivered, add it to the cod remittance cycle
            # also move amount from provisional to realised cod
            if (
                current_status == "delivered"
                and order.payment_mode.lower() == "cod"
                and order.cod_remittance_cycle_id == None
            ):

                print("inside")

                remittance_id = update_or_create_cod_remittance(order, db)
                order.cod_remittance_cycle_id = remittance_id

                wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

                freight_ded = 0

                wallet.cod_amount += order.total_amount

                wallet.provisional_cod_amount -= order.total_amount

                date = None

                for track in order.tracking_info:
                    if track["status"] == "delivered":
                        date = track["datetime"]
                        break

                date = parse_datetime(date)

                order.delivered_date = date

                log = {
                    "datetime": date,
                    "transaction_type": "COD Amount",
                    "credit": order.total_amount,
                    "debit": 0,
                    "wallet_balance_amount": wallet.amount,
                    "cod_balance_amount": wallet.cod_amount,
                    "reference": "awb - " + order.awb_number,
                    "client_id": client_id,
                    "wallet_id": wallet.id,
                }

                log = Wallet_Logs(**log)
                db.add(log)
                db.flush()

                # Update the COD remittance for this order cycle
                cod_remittance = (
                    db.query(COD_Remittance)
                    .filter(COD_Remittance.id == remittance_id)
                    .first()
                )

                # Update the database
                db.add(order)
                db.add(wallet)
                if cod_remittance:
                    db.add(cod_remittance)

                db.flush()

                # db.commit()

            # # if the order has just moved to RTO, add the COD money to the wallet, and apply RTO charges
            if current_status == "RTO" and order.rto_freight == None:

                contract = (
                    db.query(Company_To_Client_Contract)
                    .join(Company_To_Client_Contract.aggregator_courier)
                    .filter(
                        Company_To_Client_Contract.client_id == client_id,
                        Aggregator_Courier.slug == order.courier_partner,
                    )
                    .options(joinedload(Company_To_Client_Contract.aggregator_courier))
                    .first()
                )

                print("contarzcing id")
                print(contract.id)

                rto_freight = ServiceabilityService.calculate_rto_freight(
                    order_id=order.order_id,
                    min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                    additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                    contract_id=contract.id,
                )

                wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

                date = None

                print(rto_freight)

                for track in order.tracking_info:
                    if (
                        track["status"] == "RTO"
                        or track["status"] == "RTO in transit"
                        or track["status"] == "RTO initiated"
                    ):
                        date = track["datetime"]
                        order.rto_initiated_date = parse_datetime(date)
                        break

                # date = parse_datetime(datetime.now())

                date = datetime.now(timezone.utc)

                WalletService.update_wallet(
                    transaction_type="RTO Charge",
                    credit=order.forward_cod_charge * Decimal(1.18),
                    debit=rto_freight["rto_freight"] + rto_freight["rto_tax"],
                    reference=order.awb_number,
                )

                if order.payment_mode.lower() == "cod":
                    wallet.provisional_cod_amount -= order.total_amount

                order.rto_freight = rto_freight["rto_freight"]
                order.rto_tax = rto_freight["rto_tax"]
                order.forward_cod_charge = 0
                order.forward_tax = float(order.forward_freight) * 0.18

                db.add(order)
                db.flush()

                # db.commit()

            try:
                # ✅ COMPREHENSIVE NDR STATUS HANDLING IN POST_TRACKING

                # Define order_current_status at the beginning for use throughout the NDR processing
                order_current_status = current_status.lower() if current_status else ""

                print(
                    f"🔄 Starting NDR processing for order {order.order_id} with status: {order_current_status}"
                )

                # 1. Process NDR events (create new NDR records) - Fixed: Check status in tracking events properly
                ndr_list = []
                for act in order.tracking_info:
                    # ✅ FIX: Check for NDR status variations like in backfill
                    if act.get("status", "").lower() in [
                        "ndr",
                        "non delivery report",
                        "customer not available",
                        "address issue",
                        "refused by customer",
                    ]:
                        ndr_list.append(act)
                        print(
                            f"   📦 Found NDR event: {act.get('status')} at {act.get('datetime')}"
                        )

                if len(ndr_list) > 0:
                    print(
                        f"Processing {len(ndr_list)} NDR events for order {order.order_id}"
                    )
                    try:
                        ndr_result = NdrService.create_ndr(ndr_list, order)
                        if (
                            ndr_result
                            and hasattr(ndr_result, "status")
                            and not ndr_result.status
                        ):
                            logger.warning(
                                f"NDR processing failed for order {order.order_id}: {ndr_result.message}"
                            )
                        else:
                            logger.info(
                                f"NDR processing completed for order {order.order_id}"
                            )
                    except Exception as ndr_creation_error:
                        logger.error(
                            f"Error creating NDR for order {order.order_id}: {ndr_creation_error}"
                        )
                else:
                    print("No NDR events found in tracking history")

                # ✅ NEW: Handle reattempt to NDR again scenario (in post_tracking)
                # If current status is NDR and there was a previous reattempt, increment attempt count
                if order_current_status in [
                    "ndr",
                    "non delivery report",
                    "customer not available",
                    "address issue",
                    "refused by customer",
                ]:
                    try:
                        print(
                            f"   🔄 Checking for existing REATTEMPT NDR for order {order.order_id}"
                        )

                        # Check if there's an existing NDR in REATTEMPT status
                        existing_reattempt_ndr = (
                            db.query(Ndr)
                            .filter(
                                Ndr.order_id == order.id,
                                Ndr.client_id == client_id,
                                Ndr.status == "REATTEMPT",
                            )
                            .first()
                        )

                        if existing_reattempt_ndr:
                            print(
                                f"   ✅ Found REATTEMPT NDR in post_tracking for order {order.order_id}, converting to NDR with incremented attempt"
                            )

                            # Increment attempt count and change status back to take_action
                            existing_reattempt_ndr.status = "take_action"
                            existing_reattempt_ndr.attempt += 1

                            # Update with latest tracking info
                            latest_tracking = (
                                order.tracking_info[0] if order.tracking_info else {}
                            )
                            if latest_tracking and "datetime" in latest_tracking:
                                try:
                                    existing_reattempt_ndr.datetime = latest_tracking[
                                        "datetime"
                                    ]
                                except Exception as date_error:
                                    logger.warning(
                                        f"Failed to update datetime: {date_error}"
                                    )

                            existing_reattempt_ndr.reason = f"Reattempt failed - {latest_tracking.get('description', 'NDR again')}"
                            existing_reattempt_ndr.updated_at = datetime.now(
                                timezone.utc
                            )

                            db.add(existing_reattempt_ndr)
                            db.flush()

                            logger.info(
                                f"Reattempt to NDR processed in post_tracking for order {order.order_id}, attempt count: {existing_reattempt_ndr.attempt}"
                            )

                            # ✅ FIX: Create history entry like in webhook method
                            try:
                                from modules.ndr_history.ndr_history_service import (
                                    NdrHistoryService,
                                )

                                NdrHistoryService.create_ndr_history(
                                    [latest_tracking] if latest_tracking else [],
                                    order.id,
                                    existing_reattempt_ndr.id,
                                )
                            except Exception as history_error:
                                logger.error(
                                    f"Failed to create NDR history for reattempt to NDR: {history_error}"
                                )

                    except Exception as e:
                        logger.error(
                            f"Error processing reattempt to NDR in post_tracking for order {order.order_id}: {e}"
                        )

                # 2. ✅ NEW: Handle status transitions for existing NDR records
                # Check if current status is RTO/Delivered and update existing NDR
                if order_current_status in ["delivered", "rto"]:
                    try:
                        print(
                            f"   🎯 Processing {order_current_status.upper()} status for order {order.order_id}"
                        )

                        existing_ndr = (
                            db.query(Ndr)
                            .filter(
                                Ndr.order_id == order.id, Ndr.client_id == client_id
                            )
                            .first()
                        )

                        if existing_ndr and existing_ndr.status not in [
                            "DELIVERED",
                            "RTO",
                        ]:
                            print(
                                f"   ✅ Updating NDR status from {existing_ndr.status} to {order_current_status.upper()} for order {order.order_id}"
                            )

                            # Update NDR status to final state
                            if order_current_status == "delivered":
                                existing_ndr.status = "DELIVERED"
                            elif order_current_status == "rto":
                                existing_ndr.status = "RTO"

                            # Update with latest tracking info
                            latest_tracking = (
                                order.tracking_info[0] if order.tracking_info else {}
                            )
                            if latest_tracking.get("datetime"):
                                existing_ndr.datetime = latest_tracking["datetime"]

                            existing_ndr.updated_at = datetime.now(timezone.utc)
                            db.add(existing_ndr)
                            db.flush()

                            logger.info(
                                f"NDR status updated to {order_current_status.upper()} for order {order.order_id}"
                            )

                            # ✅ FIX: Create history entry for final status like in webhook method
                            try:
                                from modules.ndr_history.ndr_history_service import (
                                    NdrHistoryService,
                                )

                                NdrHistoryService.create_ndr_history(
                                    [latest_tracking] if latest_tracking else [],
                                    order.id,
                                    existing_ndr.id,
                                )
                            except Exception as history_error:
                                logger.error(
                                    f"Failed to create NDR history for final status: {history_error}"
                                )
                        else:
                            if existing_ndr:
                                print(
                                    f"   ⏭️  NDR already in final state: {existing_ndr.status}"
                                )
                            else:
                                print(
                                    f"   ⚠️  No existing NDR found for {order_current_status} status"
                                )

                    except Exception as e:
                        logger.error(
                            f"Error updating NDR for delivered/RTO in post_tracking: {e}"
                        )

                # 3. Check if current status triggers auto-reattempt
                elif order_current_status in [
                    "out for delivery",
                    "ofd",
                    "in transit",
                    "intransit",
                    "in_transit",
                ]:
                    try:
                        print(
                            f"   🚚 Processing auto-reattempt for {order_current_status} status"
                        )

                        existing_ndr = (
                            db.query(Ndr)
                            .filter(
                                Ndr.order_id == order.id,
                                Ndr.client_id == client_id,
                                Ndr.status == "take_action",  # Only for pending NDRs
                            )
                            .first()
                        )

                        if existing_ndr:
                            print(
                                f"   ✅ Auto-reattempt triggered in post_tracking for order {order.order_id}"
                            )

                            existing_ndr.status = "REATTEMPT"
                            existing_ndr.attempt += 1

                            # Update with latest tracking info
                            latest_tracking = (
                                order.tracking_info[0] if order.tracking_info else {}
                            )
                            if latest_tracking.get("datetime"):
                                existing_ndr.datetime = latest_tracking["datetime"]
                            if latest_tracking.get("description"):
                                existing_ndr.reason = (
                                    f"Auto reattempt - {latest_tracking['description']}"
                                )

                            existing_ndr.updated_at = datetime.now(timezone.utc)
                            db.add(existing_ndr)
                            db.flush()

                            logger.info(
                                f"Auto-reattempt processed in post_tracking for order {order.order_id}"
                            )

                            # ✅ FIX: Create history entry for auto-reattempt
                            try:
                                from modules.ndr_history.ndr_history_service import (
                                    NdrHistoryService,
                                )

                                NdrHistoryService.create_ndr_history(
                                    [latest_tracking] if latest_tracking else [],
                                    order.id,
                                    existing_ndr.id,
                                )
                            except Exception as history_error:
                                logger.error(
                                    f"Failed to create NDR history for auto-reattempt: {history_error}"
                                )
                        else:
                            print(f"   ⚠️  No pending NDR found for auto-reattempt")

                    except Exception as e:
                        logger.error(
                            f"Error processing auto-reattempt in post_tracking: {e}"
                        )

                print(f"✅ NDR processing completed for order {order.order_id}")

            except Exception as e:
                logger.error(
                    extra=context_user_data.get(),
                    msg=f"Error processing NDR for order {order.order_id if order else 'Unknown'}: {str(e)}",
                )
                print(
                    f"❌ Error processing NDR for order {order.order_id if order else 'Unknown'}: {e}"
                )

            for track in reversed(order.tracking_info):

                if track["status"] == "out for pickup" and not order.first_ofp_date:
                    order.first_ofp_date = parse_datetime(track["datetime"])

                if (
                    track["status"] == "picked up"
                    or track["status"] == "pickup completed"
                ) and not order.pickup_completion_date:
                    order.pickup_completion_date = parse_datetime(track["datetime"])

                if track["status"] == "out for delivery" and not order.first_ofd_date:
                    order.first_ofd_date = parse_datetime(track["datetime"])

                if (
                    track["status"] == "RTO"
                    or track["status"] == "RTO in transit"
                    or track["status"] == "RTO delivered"
                ) and order.rto_initiated_date is None:
                    order.rto_initiated_date = parse_datetime(track["datetime"])

                if track["status"] == "delivered" and order.delivered_date is None:
                    order.delivered_date = parse_datetime(track["datetime"])

                if track["status"] == "RTO delivered" and not order.rto_delivered_date:
                    order.rto_delivered_date = parse_datetime(track["datetime"])

            order.last_update_date = parse_datetime(order.tracking_info[0]["datetime"])

            try:

                if client_id == 93:

                    body = {
                        "awb": order.awb_number,
                        "current_status": order.sub_status,
                        "order_id": order.order_id,
                        "current_timestamp": (
                            order.tracking_info[0]["datetime"]
                            if order.tracking_info
                            else order.booking_date.strftime("%d-%m-%Y %H:%M:%S")
                        ),
                        "shipment_status": order.sub_status,
                        "scans": [
                            {
                                "datetime": activity["datetime"],
                                "status": activity["status"],
                                "location": activity["location"],
                            }
                            for activity in order.tracking_info
                        ],
                    }

                    response = requests.post(
                        url="https://wtpzsmej1h.execute-api.ap-south-1.amazonaws.com/prod/webhook/bluedart",
                        verify=True,
                        timeout=10,
                        json=body,
                    )

                    print(response.json())
            except:
                pass

            db.add(order)
            db.flush()
            db.commit()

            return

        except Exception as e:
            # Log other unhandled exceptions

            print(str(e))
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )

        finally:
            if db:
                db.close()

    @staticmethod
    def dev_assign_return_awb(return_order, courier_id: int):
        """Development method for assigning AWB to return orders using shadowfax"""

        try:
            import random
            import time
            from datetime import datetime, timezone

            client_id = context_user_data.get().client_id
            db = get_db_session()

            if return_order.status != "new":
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    status=True,
                    data={
                        "awb_number": return_order.awb_number or "",
                        "delivery_partner": return_order.courier_partner or "",
                    },
                    message="Return AWB already assigned",
                )

            # Use shadowfax courier (ID 24) for return orders
            if courier_id == 24:
                # Generate AWB in format: R12 + 8 random digits + WAO
                prefix = "R12"
                middle = "".join(str(random.randint(0, 9)) for _ in range(8))
                suffix = "WAO"
                awb = f"{prefix}{middle}{suffix}"
                courier_partner = "Shadowfax-Reverse"
            else:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid courier for return orders. Only Shadowfax (ID: 24) is supported.",
                )

            # Update return order status and details
            return_order.status = "pickup"
            return_order.sub_status = "pickup pending"
            return_order.courier_status = "BOOKED"
            return_order.awb_number = awb
            return_order.aggregator = "shadowfax"
            return_order.courier_partner = courier_partner
            return_order.booking_date = datetime.now(timezone.utc)

            shipment_response = GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                data={
                    "awb_number": return_order.awb_number,
                    "delivery_partner": courier_partner,
                },
                message="Return AWB assigned successfully",
            )

            print(f"Return AWB assigned: {return_order.awb_number}")

            # Save the updated return order
            db.add(return_order)
            db.flush()
            db.commit()

            return shipment_response

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Error assigning return AWB: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while assigning the return AWB.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error in dev_assign_return_awb: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

        finally:
            if db:
                db.close()
