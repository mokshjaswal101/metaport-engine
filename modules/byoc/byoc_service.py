import http
from datetime import datetime, timezone
import os
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from psycopg2 import DatabaseError
from sqlalchemy.orm import joinedload
from context_manager.context import context_user_data, get_db_session
from logger import logger
from sqlalchemy import or_
from io import BytesIO
import pandas as pd
import base64
import httpx
import asyncio

# from context_manager.context import context_user_data, get_db_session

# models
from models import New_Company_To_Client_Rate, Aggregator_Courier
from models import (
    New_Company_To_Client_Rate,
    Company_Contract,
    Client_Contract,
    Shipping_Partner,
    Courier_Blocked_Pincode,
)

# schema
from schema.base import GenericResponseModel
from data.courier_service_mapping import courier_service_mapping

from .byoc_schema import (
    CourierFilterRequest,
    BlockedPincodeRequest,
    RemoveBlockedPincodeRequest,
    GetBlockedPincodesRequest,
)


# service
# from modules.wallet_logs.wallet_logs_service import WalletLogsService


async def verify_delhivery_token(token: str):
    headers = {"Authorization": f"Token {token}"}
    print(headers, "<<headers>>")
    params = {"filter_codes": "110001"}  # Valid pincode for testing

    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://track.delhivery.com/api/cmu/pincode/",
            headers=headers,
            params=params,
        )
    print(response, "<<response>>")
    if response.status_code == 200:
        return {"status": True, "message": "Token is valid"}
    else:
        return {
            "status": False,
            "message": "Invalid token or unauthorized access",
            "status_code": response.status_code,
            "response": response.text,
        }


async def is_shadowfax_token_valid(token: str) -> dict:
    url = "https://shadowfax.in/api/v3/pincodes"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
    if response.status_code == 200:
        return {"status_code": 200, "status": True, "message": "Token is valid"}
    elif response.status_code in [401, 403]:
        return {
            "status_code": 400,
            "status": False,
            "message": "Invalid or expired token",
        }
    else:
        return {
            "status_code": 400,
            "status": False,
            "message": "Invalid",
        }


class ManageCourierForClient:
    @staticmethod
    async def authenticate_contract(add_courier_payload):
        print(add_courier_payload.courier_slug, "<<add_courier_payload>>")

        shipping_partner = courier_service_mapping[add_courier_payload.courier_slug]
        print(add_courier_payload.courier_slug, "**Data action**")
        shipment_response = shipping_partner.Add_Contract_Generate_Token(
            add_courier_payload.data
        )
        print(shipment_response, "<<shipment_response>>")
        return shipment_response

    @staticmethod
    def get_courier_for_client(filters: CourierFilterRequest):
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                query = (
                    db.query(New_Company_To_Client_Rate)
                    .options(joinedload(New_Company_To_Client_Rate.shipping_partner))
                    .filter(New_Company_To_Client_Rate.client_id == client_id)
                )

                # Filter by name/slug (search)
                if filters.search:
                    pattern = f"%{filters.search.lower()}%"
                    query = query.filter(
                        or_(
                            New_Company_To_Client_Rate.shipping_partner.has(
                                Shipping_Partner.name.ilike(pattern)
                            ),
                            New_Company_To_Client_Rate.shipping_partner.has(
                                Shipping_Partner.slug.ilike(pattern)
                            ),
                        )
                    )

                # # Filter by exact courier slug
                if filters.courier:
                    query = query.filter(
                        New_Company_To_Client_Rate.shipping_partner.has(
                            Shipping_Partner.slug == filters.courier
                        )
                    )

                # Filter by mode (Surface, Air, etc.)
                if filters.mode:
                    query = query.filter(
                        New_Company_To_Client_Rate.shipping_partner.has(
                            Shipping_Partner.mode == filters.mode
                        )
                    )

                # Future: filter by weight if applicable

                records = query.all()

                result = [
                    {
                        "uuid": r.uuid,
                        "isActive": r.isActive,
                        "rate_type": r.rate_type,
                        "name": (r.shipping_partner.name if r.shipping_partner else ""),
                        "slug": (r.shipping_partner.slug if r.shipping_partner else ""),
                        "mode": (r.shipping_partner.mode if r.shipping_partner else ""),
                    }
                    for r in records
                ]

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data=result,
                    status=True,
                    message="Courier fetched successfully",
                )

        except DatabaseError as e:
            logger.error(extra=context_user_data.get(), msg=f"DB Error: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Unable to fetch couriers",
            )
        except Exception as e:
            logger.error(
                extra=context_user_data.get(), msg=f"Unhandled error: {str(e)}"
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Unable to fetch couriers",
            )

    @staticmethod
    def courier_status_change(courier_Status):
        try:
            with get_db_session() as db:
                courier_record = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(New_Company_To_Client_Rate.uuid == courier_Status.id)
                    .first()
                )
                if not courier_record:
                    return GenericResponseModel(
                        status=False,
                        message="Courier Not Found",
                    )
                courier_record.isActive = courier_Status.status
                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data=[],
                    status=True,
                    message="Courier status changed successfully",
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
                message="Unable to get balance",
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
                message="Unable to get balance",
            )

    @staticmethod
    def export_Couriers(filters: CourierFilterRequest) -> GenericResponseModel:
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                query = (
                    db.query(New_Company_To_Client_Rate)
                    .options(joinedload(New_Company_To_Client_Rate.aggregator_courier))
                    .filter(New_Company_To_Client_Rate.client_id == client_id)
                )

                # Apply filters only if provided
                if filters.search:
                    pattern = f"%{filters.search.lower()}%"
                    query = query.filter(
                        or_(
                            New_Company_To_Client_Rate.aggregator_courier.has(
                                Aggregator_Courier.name.ilike(pattern)
                            ),
                            New_Company_To_Client_Rate.aggregator_courier.has(
                                Aggregator_Courier.slug.ilike(pattern)
                            ),
                        )
                    )

                if filters.courier:
                    query = query.filter(
                        New_Company_To_Client_Rate.aggregator_courier.has(
                            Aggregator_Courier.slug == filters.courier
                        )
                    )

                if filters.mode and filters.mode.lower() != "all":
                    query = query.filter(
                        New_Company_To_Client_Rate.aggregator_courier.has(
                            Aggregator_Courier.mode == filters.mode
                        )
                    )

                # Fetch records (all if no filters applied)
                records = query.all()

                # Prepare export data
                couriers_data = [
                    {
                        "Courier Name": (
                            r.aggregator_courier.name if r.aggregator_courier else ""
                        ),
                        "Mode": (
                            r.aggregator_courier.mode if r.aggregator_courier else ""
                        ),
                        "Courier Type": r.rate_type,
                        "Status": "Active" if r.isActive else "Inactive",
                    }
                    for r in records
                ]

                if not couriers_data:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="No couriers found for the given filters.",
                    )

                # Convert to Excel
                df = pd.DataFrame(couriers_data)
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Couriers")
                output.seek(0)

                file_data_base64 = base64.b64encode(output.getvalue()).decode("utf-8")

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Export successful",
                    data=file_data_base64,
                )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(), msg=f"Export Couriers Error: {str(e)}"
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Failed to export couriers",
            )

    @staticmethod
    def get_courier_stats() -> GenericResponseModel:
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                query = (
                    db.query(New_Company_To_Client_Rate)
                    .options(joinedload(New_Company_To_Client_Rate.shipping_partner))
                    .filter(New_Company_To_Client_Rate.client_id == client_id)
                )

                all_couriers = query.all()

                total_couriers = len(all_couriers)
                total_active = sum(1 for c in all_couriers if c.isActive)

                total_forward = sum(
                    1
                    for c in all_couriers
                    if c.rate_type and c.rate_type.lower() == "forward"
                )

                total_rto = sum(
                    1
                    for c in all_couriers
                    if c.rate_type and c.rate_type.lower() == "rto"
                )

                stats = {
                    "total_couriers": total_couriers,
                    "total_active_couriers": total_active,
                    "total_forward_couriers": total_forward,
                    "total_rto_couriers": total_rto,
                }

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Courier stats fetched successfully",
                    data=stats,
                )

        except Exception as e:
            logger.error(extra=context_user_data.get(), msg=f"Stats error: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch courier stats",
            )

    def all_contracts():
        try:
            with get_db_session() as db:
                courier_slugs = [
                    "shiperfecto",
                    "shipmozo",
                    "delhivery",
                    "dtdc",
                    "amazon",
                    "xpressbees",
                    "ecom-express",
                    "shiprocket",
                    "ats",
                    "ekart",
                    "shadowfax",
                    "bluedart",
                    "zippyy",
                ]

                #  Extract the unique slugs (keys)
                valid_slugs = list(set(courier_service_mapping.keys()))
                shipping_partner_list = (
                    db.query(
                        Shipping_Partner.id.label("shipping_partner_id"),
                        Shipping_Partner.name.label("partner_name"),
                        Shipping_Partner.slug.label("partner_slug"),
                        Shipping_Partner.is_aggregator.label("is_aggregator"),
                    )
                    .filter(
                        Shipping_Partner.is_deleted == False,
                        Shipping_Partner.slug.in_(courier_slugs),
                    )
                    .all()
                )
                result = [
                    {
                        "id": spl.shipping_partner_id,
                        "partner_name": spl.partner_name,
                        "partner_slug": spl.partner_slug,
                    }
                    for spl in shipping_partner_list
                ]
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data=result,
                    status=True,
                    message="Get all courier contracts list",
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
                message="Unable to get balance",
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
                message="Unable to get balance",
            )

    def add_or_update_rate(db, client_id, Single_rate_upload, contract_id, id):
        """
        Create or update rate for a client and courier.
        """
        try:
            print(Single_rate_upload, "**Single rate card**", contract_id, id)
            #  Check if rate already exists
            existing_rate = (
                db.query(New_Company_To_Client_Rate)
                .filter(
                    New_Company_To_Client_Rate.client_id == client_id,
                    New_Company_To_Client_Rate.company_contract_id == contract_id,
                )
                .first()
            )
            #  Common fields
            rate_fields = dict(
                percentage_rate=Single_rate_upload.rates.percentage_rate,
                absolute_rate=Single_rate_upload.rates.absolute_rate,
                base_rate_zone_a=Single_rate_upload.rates.base_rate_zone_a,
                base_rate_zone_b=Single_rate_upload.rates.base_rate_zone_b,
                base_rate_zone_c=Single_rate_upload.rates.base_rate_zone_c,
                base_rate_zone_d=Single_rate_upload.rates.base_rate_zone_d,
                base_rate_zone_e=Single_rate_upload.rates.base_rate_zone_e,
                additional_rate_zone_a=Single_rate_upload.rates.additional_rate_zone_a,
                additional_rate_zone_b=Single_rate_upload.rates.additional_rate_zone_b,
                additional_rate_zone_c=Single_rate_upload.rates.additional_rate_zone_c,
                additional_rate_zone_d=Single_rate_upload.rates.additional_rate_zone_d,
                additional_rate_zone_e=Single_rate_upload.rates.additional_rate_zone_e,
                rto_base_rate_zone_a=Single_rate_upload.rates.rto_base_rate_zone_a,
                rto_base_rate_zone_b=Single_rate_upload.rates.rto_base_rate_zone_b,
                rto_base_rate_zone_c=Single_rate_upload.rates.rto_base_rate_zone_c,
                rto_base_rate_zone_d=Single_rate_upload.rates.rto_base_rate_zone_d,
                rto_base_rate_zone_e=Single_rate_upload.rates.rto_base_rate_zone_e,
                rto_additional_rate_zone_a=Single_rate_upload.rates.rto_additional_rate_zone_a,
                rto_additional_rate_zone_b=Single_rate_upload.rates.rto_additional_rate_zone_b,
                rto_additional_rate_zone_c=Single_rate_upload.rates.rto_additional_rate_zone_c,
                rto_additional_rate_zone_d=Single_rate_upload.rates.rto_additional_rate_zone_d,
                rto_additional_rate_zone_e=Single_rate_upload.rates.rto_additional_rate_zone_e,
                rate_type="forward",
                isActive=False,
                client_id=client_id,
                company_id=1,
                company_contract_id=contract_id,
                shipping_partner_id=id,
            )
            # print(rate_fields, "rates shows")
            if existing_rate:
                pass
            else:
                new_rate = New_Company_To_Client_Rate(**rate_fields)
                db.add(new_rate)
                print(f" Created new rate for {Single_rate_upload.courier_slug}")
            print("trigger commit")
            db.commit()
            return True

        except Exception as e:
            db.rollback()
            print(f" Error creating/updating rate: {str(e)}")
            return False

    @staticmethod
    async def add_contracts(add_courier_payload):
        """
        Add or update courier contract with credentials.
        Always creates a rate entry with all fields set to 0.
        """
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                # 1️⃣ Validate credentials (for non-predefined couriers)
                if add_courier_payload.courier_slug not in [
                    "dtdc",
                    "bluedart",
                    "amazon",
                    "ecom-express",
                    "shadowfax",
                    "shipmozo",
                    "delhivery",
                ]:
                    check_auth = await ManageCourierForClient.authenticate_contract(
                        add_courier_payload
                    )
                    if check_auth.get("status_code") != 200:
                        return GenericResponseModel(
                            status_code=http.HTTPStatus.BAD_REQUEST,
                            status=False,
                            message="Invalid credentials",
                        )

                # 2️⃣ Create or update contract
                existing_contract = (
                    db.query(Client_Contract)
                    .filter(
                        Client_Contract.client_id == client_id,
                        Client_Contract.shipping_partner_id == add_courier_payload.id,
                    )
                    .first()
                )

                if existing_contract:
                    existing_contract.credentials = add_courier_payload.data
                    db.commit()
                    contract_id = existing_contract.id
                    logger.info(
                        extra=context_user_data.get(),
                        msg=f"Updated contract for courier: {add_courier_payload.courier_slug}",
                    )
                else:
                    new_contract = Client_Contract(
                        client_id=client_id,
                        shipping_partner_id=add_courier_payload.id,
                        credentials=add_courier_payload.data,
                        tracking_series=0,
                        isActive=False,
                    )
                    db.add(new_contract)
                    db.commit()
                    contract_id = new_contract.id
                    logger.info(
                        extra=context_user_data.get(),
                        msg=f"Created new contract for courier: {add_courier_payload.courier_slug}",
                    )

                # 3️⃣ Always create or update rate entry with 0 values
                existing_rate = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.client_id == client_id,
                        New_Company_To_Client_Rate.client_contract_id == contract_id,
                    )
                    .first()
                )

                # Default rate fields (all set to 0)
                rate_fields = {
                    "percentage_rate": 0,
                    "absolute_rate": 0,
                    "base_rate_zone_a": 0,
                    "base_rate_zone_b": 0,
                    "base_rate_zone_c": 0,
                    "base_rate_zone_d": 0,
                    "base_rate_zone_e": 0,
                    "additional_rate_zone_a": 0,
                    "additional_rate_zone_b": 0,
                    "additional_rate_zone_c": 0,
                    "additional_rate_zone_d": 0,
                    "additional_rate_zone_e": 0,
                    "rto_base_rate_zone_a": 0,
                    "rto_base_rate_zone_b": 0,
                    "rto_base_rate_zone_c": 0,
                    "rto_base_rate_zone_d": 0,
                    "rto_base_rate_zone_e": 0,
                    "rto_additional_rate_zone_a": 0,
                    "rto_additional_rate_zone_b": 0,
                    "rto_additional_rate_zone_c": 0,
                    "rto_additional_rate_zone_d": 0,
                    "rto_additional_rate_zone_e": 0,
                    "rate_type": "forward",
                    "isActive": False,
                    "client_id": client_id,
                    "company_id": 1,
                    "client_contract_id": contract_id,
                    "shipping_partner_id": add_courier_payload.id,
                }

                if existing_rate:
                    # Update existing rate (reset to 0 values)
                    for field, value in rate_fields.items():
                        setattr(existing_rate, field, value)
                    logger.info(
                        extra=context_user_data.get(),
                        msg=f"Updated rate entry for {add_courier_payload.courier_slug}",
                    )
                else:
                    # Create new rate entry
                    new_rate = New_Company_To_Client_Rate(**rate_fields)
                    db.add(new_rate)
                    logger.info(
                        extra=context_user_data.get(),
                        msg=f"Created new rate entry for {add_courier_payload.courier_slug}",
                    )

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Contract saved successfully",
                )

        except DatabaseError as e:
            db.rollback()
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error in add_contracts: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Database error while saving contract",
            )
        except Exception as e:
            db.rollback()
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error in add_contracts: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Failed to save contract",
            )

    @staticmethod
    def get_contract_by_id(get_single_courier):
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id
                print(get_single_courier.id, "**Single rate card**")
                get_single_contracts = (
                    db.query(
                        Client_Contract.shipping_partner_id,
                        Client_Contract.credentials,
                    )
                    .filter(
                        Client_Contract.shipping_partner_id == get_single_courier.id,
                        Client_Contract.client_id == client_id,
                    )
                    .first()
                )
                if get_single_contracts == None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Record not found",
                    )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data=get_single_contracts[1],
                    status=True,
                    message="Successfully",
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
                message="Unable to get balance",
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
                message="Unable to get balance",
            )

    @staticmethod
    def single_contract_rate(Single_rate_upload):
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id
                print(Single_rate_upload.courier_name, "**Single rate card**")
                aggregator = (
                    db.query(Aggregator_Courier.id)
                    .filter(
                        Aggregator_Courier.slug == Single_rate_upload.courier_name,
                        # Aggregator_Courier.isactive == True,
                    )
                    .first()
                )
                print(aggregator, "dfsdfsdfsdf")
                new_rate = New_Company_To_Client_Rate(
                    percentage_rate=Single_rate_upload.cod_percentage,
                    absolute_rate=Single_rate_upload.cod_rate,
                    base_rate_zone_a=Single_rate_upload.forward_a,
                    base_rate_zone_b=Single_rate_upload.forward_b,
                    base_rate_zone_c=Single_rate_upload.forward_c,
                    base_rate_zone_d=Single_rate_upload.forward_d,
                    base_rate_zone_e=Single_rate_upload.forward_e,
                    additional_rate_zone_a=Single_rate_upload.forward_a,
                    additional_rate_zone_b=Single_rate_upload.forward_b,
                    additional_rate_zone_c=Single_rate_upload.forward_c,
                    additional_rate_zone_d=Single_rate_upload.forward_d,
                    additional_rate_zone_e=Single_rate_upload.forward_e,
                    rto_base_rate_zone_a=Single_rate_upload.rto_a,
                    rto_base_rate_zone_b=Single_rate_upload.rto_b,
                    rto_base_rate_zone_c=Single_rate_upload.rto_c,
                    rto_base_rate_zone_d=Single_rate_upload.rto_d,
                    rto_base_rate_zone_e=Single_rate_upload.rto_e,
                    rto_additional_rate_zone_a=Single_rate_upload.rto_a,
                    rto_additional_rate_zone_b=Single_rate_upload.rto_b,
                    rto_additional_rate_zone_c=Single_rate_upload.rto_c,
                    rto_additional_rate_zone_d=Single_rate_upload.rto_d,
                    rto_additional_rate_zone_e=Single_rate_upload.rto_e,
                    rate_type="forward",
                    isActive=False,
                    client_id=client_id,
                    company_id=1,
                    company_contract_id=Single_rate_upload.contract_id,
                    aggregator_courier_id=aggregator[0],
                )

                db.add(new_rate)
                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data=[],
                    status=True,
                    message="Contract Added",
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
                message="Unable to get balance",
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
                message="Unable to get balance",
            )

    # ============================================
    # COURIER SETTINGS - PINCODE BLOCKING
    # ============================================

    @staticmethod
    def get_blocked_pincodes(request: GetBlockedPincodesRequest) -> GenericResponseModel:
        """Get all blocked pincodes for a specific courier"""
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                # Get the courier rate record
                courier_rate = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.uuid == request.courier_uuid,
                        New_Company_To_Client_Rate.client_id == client_id,
                    )
                    .first()
                )

                if not courier_rate:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Courier not found",
                    )

                # Get blocked pincodes for this courier
                blocked_pincodes = (
                    db.query(Courier_Blocked_Pincode)
                    .filter(
                        Courier_Blocked_Pincode.client_id == client_id,
                        Courier_Blocked_Pincode.courier_rate_id == courier_rate.id,
                        Courier_Blocked_Pincode.is_deleted == False,
                    )
                    .all()
                )

                result = [
                    {
                        "pincode": bp.pincode,
                        "reason": bp.reason,
                        "created_at": bp.created_at.isoformat() if bp.created_at else None,
                    }
                    for bp in blocked_pincodes
                ]

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Blocked pincodes fetched successfully",
                    data=result,
                )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching blocked pincodes: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Failed to fetch blocked pincodes",
            )

    @staticmethod
    def add_blocked_pincodes(request: BlockedPincodeRequest) -> GenericResponseModel:
        """Add blocked pincodes for a specific courier"""
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                # Get the courier rate record
                courier_rate = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.uuid == request.courier_uuid,
                        New_Company_To_Client_Rate.client_id == client_id,
                    )
                    .first()
                )

                if not courier_rate:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Courier not found",
                    )

                # Get existing blocked pincodes to avoid duplicates
                existing_pincodes = (
                    db.query(Courier_Blocked_Pincode.pincode)
                    .filter(
                        Courier_Blocked_Pincode.client_id == client_id,
                        Courier_Blocked_Pincode.courier_rate_id == courier_rate.id,
                        Courier_Blocked_Pincode.is_deleted == False,
                    )
                    .all()
                )
                existing_set = {p[0] for p in existing_pincodes}

                # Add new blocked pincodes
                added_count = 0
                for pincode in request.pincodes:
                    pincode_str = str(pincode).strip()
                    if pincode_str and pincode_str not in existing_set:
                        new_blocked = Courier_Blocked_Pincode(
                            client_id=client_id,
                            courier_rate_id=courier_rate.id,
                            pincode=pincode_str,
                            reason=request.reason,
                        )
                        db.add(new_blocked)
                        added_count += 1

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message=f"Successfully blocked {added_count} pincode(s)",
                    data={"added_count": added_count},
                )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error adding blocked pincodes: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Failed to add blocked pincodes",
            )

    @staticmethod
    def remove_blocked_pincodes(request: RemoveBlockedPincodeRequest) -> GenericResponseModel:
        """Remove blocked pincodes for a specific courier"""
        try:
            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                # Get the courier rate record
                courier_rate = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.uuid == request.courier_uuid,
                        New_Company_To_Client_Rate.client_id == client_id,
                    )
                    .first()
                )

                if not courier_rate:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Courier not found",
                    )

                # Soft delete the specified pincodes
                removed_count = 0
                for pincode in request.pincodes:
                    pincode_str = str(pincode).strip()
                    blocked = (
                        db.query(Courier_Blocked_Pincode)
                        .filter(
                            Courier_Blocked_Pincode.client_id == client_id,
                            Courier_Blocked_Pincode.courier_rate_id == courier_rate.id,
                            Courier_Blocked_Pincode.pincode == pincode_str,
                            Courier_Blocked_Pincode.is_deleted == False,
                        )
                        .first()
                    )
                    if blocked:
                        blocked.is_deleted = True
                        removed_count += 1

                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message=f"Successfully unblocked {removed_count} pincode(s)",
                    data={"removed_count": removed_count},
                )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error removing blocked pincodes: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Failed to remove blocked pincodes",
            )
