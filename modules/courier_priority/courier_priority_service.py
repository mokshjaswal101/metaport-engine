import http
import json
from psycopg2 import DatabaseError
from typing import Dict
import requests
from sqlalchemy import asc, desc, text, func, and_
from datetime import datetime
import base64
import hmac
import hashlib
import time
from datetime import datetime
from psycopg2 import DatabaseError
from fastapi import HTTPException
from sqlalchemy.orm import joinedload
from pytz import timezone
from uuid import UUID
from fastapi.encoders import jsonable_encoder
from context_manager.context import context_user_data, get_db_session
from logger import logger
from sqlalchemy.dialects import postgresql

from modules.shipment.shipment_schema import (
    CreateShipmentModel,
    AutoCreateShipmentModel,
)

# service
# from models.order_confirmation_msg_config import Order_Confirmation_Msg_Config

from models import (
    Courier_Priority_Meta,
    Courier_Priority,
    Company_To_Client_Contract,
    New_Company_To_Client_Rate,
    Order,
    Aggregator_Courier,
    Courier_Priority_Rules,
    Courier_Priority_Config_Setting,
)

# service
from modules.serviceability import ServiceabilityService

from modules.shipment import ShipmentService

# schema
from schema.base import GenericResponseModel
from modules.courier_priority.courier_priority_schema import (
    addClientMetaOptionsRequest,
    Courier_Response_Model,
    meta_options_model,
    Assigned_Courier_Response_Model,
    addRulesAndCourierPriority,
    Courier_Rules_Response_Schema,
    Courier_Rules_status,
    Update_Rule_Model,
    Courier_Config_Settings_Model,  # FOR COUEIER SETTINGS
    Courier_Deactivate_Model,  # SERCICE DEACTIVATE (OFF)
)


class CourierPriorityService:
    @staticmethod
    def check_contract(client_id):
        try:
            with get_db_session() as db:
                contracts = (
                    db.query(Company_To_Client_Contract)
                    .filter(
                        Company_To_Client_Contract.client_id == client_id,
                        Company_To_Client_Contract.isActive == True,
                    )
                    .options(joinedload(Company_To_Client_Contract.aggregator_courier))
                    .all()
                )

                if contracts != None:

                    freight = ServiceabilityService.check_price(
                        client_id=client_id,
                        contracts=contracts,
                    )
                    # print(jsonable_encoder(freight))
                    logger.info(
                        msg="contract rate: {}".format(str(freight)),
                    )
                    # print(jsonable_encoder(freight), "**freight**")
                    return "freight"
                else:
                    return "None"
                # print(freight, "||*|freight|*|12||")
        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error excess rate: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the excess.",
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

    @classmethod
    def add_courier_priority(this, reording: addClientMetaOptionsRequest):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                company_id = str(user_data.company_id)
                # courier_config_settings = (
                #     db.query(Courier_Priority_Config_Setting)
                #     .filter(
                #         Courier_Priority.client_id == client_id,
                #         Courier_Priority.company_id == company_id,
                #     )
                #     .first()
                # )
                courier_priority = (
                    db.query(Courier_Priority)
                    .filter(
                        Courier_Priority.client_id == client_id,
                        Courier_Priority.company_id == company_id,
                    )
                    .first()
                )
                if courier_priority is not None:
                    courier_priority.priority_type = reording.type
                else:
                    # COURIER PRIORITY SAVE SETTINGS START>>
                    # courier_config_settings = Courier_Priority_Config_Setting(
                    #     company_id=company_id,
                    #     client_id=client_id,
                    #     courier_method=reording.type,
                    #     status=True,
                    # )
                    # db.add(courier_config_settings)
                    # SAVE SETTINGS FOR COURIER PRIORITY  << END
                    courier_priority = Courier_Priority(
                        priority_type=reording.type,
                        client_id=client_id,
                        company_id=company_id,  #
                    )
                    db.add(courier_priority)
                db.commit()
                last_inserted_id = courier_priority.id

                if reording.type == "custom":
                    record_deleted = (
                        db.query(Courier_Priority_Meta)
                        .filter(Courier_Priority_Meta.client_id == client_id)
                        .delete()
                    )
                    db_items = [
                        Courier_Priority_Meta(
                            meta_slug=item.key,
                            ordering_key=item.ordering_key,
                            client_id=client_id,
                            company_id=company_id,
                            courier_type_id=last_inserted_id,
                            meta_value=item.value,
                        )
                        for index, item in enumerate(reording.reording)
                    ]
                    db.bulk_save_objects(db_items)
                    db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Courier Priority Updated Successfully",
                )
        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    @classmethod
    def add_rules_courier_priority(this, addRule: addRulesAndCourierPriority):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                company_id = str(user_data.company_id)
                # Get the current max ordering_key for the given client
                max_ordering_key = (
                    db.query(func.max(Courier_Priority_Rules.ordering_key))
                    .filter(Courier_Priority_Rules.client_id == client_id)
                    .scalar()
                ) or 0  # Default to 0 if none exist

                courier_priority_rules = Courier_Priority_Rules(
                    rule_name=addRule.name,
                    client_id=client_id,
                    ordering_key=max_ordering_key + 1,
                    rules=jsonable_encoder(addRule.rules),
                    courier_priority=jsonable_encoder(addRule.courier_priority),
                    status=True,
                )
                db.add(courier_priority_rules)
                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Courier Priority Rules and allocation Successfully Saved",
                )
        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    @classmethod
    def all_courier_rules_service(this):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                all_courier_list = (
                    db.query(Courier_Priority_Rules)
                    .filter(Courier_Priority_Rules.client_id == client_id)
                    .order_by(Courier_Priority_Rules.ordering_key.asc())
                    .all()
                )
                # print(jsonable_encoder(all_courier_list))

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                data=[
                    Courier_Rules_Response_Schema(**jsonable_encoder(item))
                    for item in all_courier_list
                ],
                message="All courier rules service List",
            )

        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    @classmethod
    def update_rule_order_service(this, update_rule_order: Update_Rule_Model):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                for ordering in update_rule_order.ordering:
                    db.query(Courier_Priority_Rules).filter(
                        and_(
                            Courier_Priority_Rules.uuid == ordering.uuid,
                            Courier_Priority_Rules.client_id == client_id,
                        )
                    ).update(
                        {Courier_Priority_Rules.ordering_key: ordering.rule_order},
                        synchronize_session=False,
                    )
                db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                data=update_rule_order,
                message="Rule updated Successfully",
            )

        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    @classmethod
    def get_courier_priority_list(this):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)

                fetched_contracts = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.isActive == True,
                        New_Company_To_Client_Rate.client_id == client_id,
                    )
                    .options(
                        joinedload(New_Company_To_Client_Rate.shipping_partner),
                    )
                    .all()
                )

                # print(jsonable_encoder(fetched_contracts), "||fetched_contracts||")
                Client_meta_exist = (
                    db.query(Courier_Priority)
                    .filter(Courier_Priority.client_id == client_id)
                    .options(joinedload(Courier_Priority.meta_options))
                    .all()
                )

                Courier_Config_Settings = (
                    db.query(Courier_Priority_Config_Setting)
                    .filter(
                        Courier_Priority_Config_Setting.client_id == client_id,
                        Courier_Priority_Config_Setting.status == True,
                    )
                    .all()
                )

                # FOR TESTING PURPOSE
                for priority in Client_meta_exist:
                    print(f"Courier: {priority.priority_type}")
                    for meta in priority.meta_options:
                        print(f"  {meta.ordering_key}: {meta.meta_value}")
                # print(jsonable_encoder(Courier_Config_Settings))

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "assigned_list": [
                        Assigned_Courier_Response_Model(
                            **jsonable_encoder(item.shipping_partner, exclude_none=True)
                        )
                        for item in fetched_contracts
                        if item.shipping_partner
                    ],
                    "priority_courier": [
                        Courier_Response_Model(
                            **{
                                **jsonable_encoder(item, exclude={"meta_options"}),
                                "meta_options": [
                                    meta_options_model(
                                        meta_slug=meta.meta_slug,
                                        meta_value=meta.meta_value,
                                        ordering_key=meta.ordering_key,
                                    )
                                    for meta in item.meta_options
                                ],
                            }
                        )
                        for item in Client_meta_exist
                    ],
                    "config_settings": [
                        Courier_Config_Settings_Model(**jsonable_encoder(setting))
                        for setting in Courier_Config_Settings
                    ],
                },
                message="Priority list ",
            )
        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    @classmethod
    def courier_roles_update(this, status_update: Courier_Rules_status):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                status_upate = (
                    db.query(Courier_Priority_Rules)
                    .filter(Courier_Priority_Rules.uuid == status_update.uuid)
                    .first()
                )
                if status_upate:
                    status_upate.status = status_update.status
                    db.add(status_upate)
                db.commit()
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Status Sucessfully Updated",
                )

        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    @classmethod
    def courier_deactivate_service(this, courier_method: Courier_Deactivate_Model):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                user_data = context_user_data.get()
                company_id = str(user_data.company_id)
                print("courier_deactivate_service>", client_id, courier_method)
                Courier_Config_Settings = (
                    db.query(Courier_Priority_Config_Setting)
                    .filter(
                        Courier_Priority_Config_Setting.client_id == client_id,
                        Courier_Priority_Config_Setting.company_id == company_id,
                    )
                    .all()
                )
                # print(0)
                print(Courier_Config_Settings, "<<Courier_Config_Settings>>")
                if Courier_Config_Settings != None:
                    print(1)
                    # Check if any dict has key 'courier_method' and value == 'courier-assign-rule'
                    found = any(
                        item.courier_method == courier_method.courier_method
                        for item in Courier_Config_Settings
                    )
                    print("<<Found>>", found)
                    if not found:
                        for record in Courier_Config_Settings:
                            record.status = False  # Set to False

                        courier_config_settings = Courier_Priority_Config_Setting(
                            company_id=company_id,
                            client_id=client_id,
                            courier_method=courier_method.courier_method,
                            status=True,
                        )
                        db.add(courier_config_settings)
                        db.commit()  # Save changes
                        print("Updated all statuses to False")
                    else:
                        print(
                            f"IF SETTING NOT FOUND METHOD=>{courier_method.courier_method} STATUS=>{courier_method.status}",
                        )
                        for record in Courier_Config_Settings:
                            if record.courier_method == courier_method.courier_method:
                                record.status = courier_method.status
                            else:
                                record.status = False  # Set to False
                            # courier_assign_rules
                        db.add(record)
                        db.commit()  # Save changes
                        print("courier_assign_rule found — no update needed.")
                else:
                    print("IF SETTING NOT FOUND")
                    # If no settings found, insert new one
                    new_setting = Courier_Priority_Config_Setting(
                        company_id=company_id,
                        client_id=client_id,
                        courier_method="courier_assign_rule",
                        status=True,
                    )
                    db.add(new_setting)
                    db.commit()
                    print("No existing config found. Added new courier_assign_rule.")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Status Changed Sucessfully",
                )

        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    # For Testing Purpose
    @staticmethod
    def cheapest():
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                start_time = time.time()
                print("Hi")
                # available_courier_list = ShipmentService.auto_assign_available_courier(
                #     AutoCreateShipmentModel(
                #         order_id="00014",
                #     )
                # )
                # courier_respose = {}
                # if len(available_courier_list) == 0:
                #     print("There is not available courier for this shipment")
                #     courier_respose = {
                #         "error": "There is not available courier for this order"
                #     }
                # else:
                #     for available_courier in available_courier_list:
                #         print(jsonable_encoder(available_courier))
                #         auto_assign_freight = ShipmentService.auto_assign_awb(
                #             CreateShipmentModel(
                #                 order_id="1234567sadasas16Request-11",
                #                 courier_id=available_courier["courier_id"],
                #                 total_freight=available_courier["total_freight"],
                #                 cod_freight=available_courier["cod_freight"],
                #                 tax=available_courier["tax"],
                #             )
                #         )
                #         if auto_assign_freight == None:
                #             continue
                #         if auto_assign_freight == "success":
                #             courier_respose = {"success": auto_assign_freight}
                #             break
                #         else:
                #             courier_respose = {"failed": auto_assign_freight}
                #             continue
                # # End timer
                # end_time = time.time()
                # total_duration = round(end_time - start_time, 1)  # in seconds
                # print(
                #     courier_respose,
                #     f"Order Booked Successfully total_duration{total_duration} in second",
                # )
                # return courier_respose

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=[],
                    message="Courier Priority Fetched Sucessfully ",
                )

        except DatabaseError as e:
            logger.error(
                msg="Error retrieving orders: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

    @classmethod
    def courier_config_settings(this):
        try:
            with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = str(user_data.client_id)
                print("Welcome To Courier_Config_Settings Sections", client_id)
                # Courier_Priority_Config_Setting
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="courier_config_settings ",
                )
        except DatabaseError as e:
            logger.error(
                msg="Courier Config Settings: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )
