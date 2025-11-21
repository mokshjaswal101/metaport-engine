import http
from decimal import Decimal
from sqlalchemy import desc
from datetime import datetime, timezone
import os
from sqlalchemy.future import select
from psycopg2 import DatabaseError

from context_manager.context import context_user_data, get_db_session
from logger import logger

# models
from models import (
    Wallet,
    ShippingNotificationsRate,
    ShippingNotificationsSetting,
    ShippingNotificationLogs,
)

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from modules.shipping_notifications.shipping_notifications_schema import (
    ShippingNotificationsRateBaseModel,
    ShippingNotificationSettingBaseModel,
    ShippingNotificationsLogsModel,
)

# service
from modules.whatsapp.whatsapp_service import WhatsappService


defaultRates = {"shipping_notifications": 0.79, "cod_confirmation": 1.79}


class ShippingNotificaitions:

    # @staticmethod
    # def get_notifications_balance():

    #     try:

    #         client_id = context_user_data.get().client_id

    #         with get_db_session() as db:

    #             wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

    #             if wallet is None:
    #                 # Return error response
    #                 return GenericResponseModel(
    #                     status_code=http.HTTPStatus.BAD_REQUEST,
    #                     message="Wallet not found",
    #                 )

    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.OK,
    #                 status=True,
    #                 data={"notifications_balance": wallet.shipping_notifications},
    #                 message="successfull",
    #             )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Error getting balance: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Unable to get balance",
    #         )

    #     except Exception as e:
    #         # Log other unhandled exceptions
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Unhandled error: {}".format(str(e)),
    #         )
    #         # Return a general internal server error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Unable to get balance",
    #         )

    @staticmethod
    async def get_notifications_balance() -> GenericResponseModel:
        """Get the shipping notifications balance for the logged-in client asynchronously."""
        try:
            client_id = context_user_data.get().client_id

            # Use async DB session
            async with get_db_session() as db:  # db should be AsyncSession
                result = await db.execute(
                    select(Wallet).filter(Wallet.client_id == client_id)
                )
                wallet = result.scalars().first()

                if wallet is None:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        status=False,
                        message="Wallet not found",
                    )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"notifications_balance": wallet.shipping_notifications},
                    message="Successful",
                )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error getting balance: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Unable to get balance",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Unable to get balance",
            )

    @staticmethod
    def send_notification(order: Order_Model, notification_type: str):

        try:

            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Wallet not found",
                )

            notifications_balance = wallet.shipping_notifications

            if notifications_balance < 0.79:

                print("Not enough balance")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Insufficient Balance",
                )

            WhatsappService.send_message(order, notification_type)

            wallet.shipping_notifications = float(wallet.shipping_notifications) - 0.79
            db.add(wallet)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={"notifications_balance": wallet.shipping_notifications},
                message="successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error getting balance: {}".format(str(e)),
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
    def send_order_confirmation(order: Order_Model):

        try:

            client_id = context_user_data.get().client_id

            db = get_db_session()

            wallet = db.query(Wallet).filter(Wallet.client_id == client_id).first()

            notification_rates = (
                db.query(ShippingNotificationsRate)
                .filter(ShippingNotificationsRate.client_id == client_id)
                .first()
            )

            order_confirmation_rate = notification_rates.cod_confirmation

            if wallet is None:
                # Return error response
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Wallet not found",
                )

            notifications_balance = wallet.shipping_notifications

            if notifications_balance < order_confirmation_rate:

                print("Not enough balance")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Insufficient Balance",
                )

            # WhatsappService.send_order_confirmation_message(order)

            log = {
                "order_id": order.id,
                "direction": "sent",
                "sent_at": datetime.now(timezone.utc),
                "message_type": "order_confirmation",
                "cost": order_confirmation_rate,
                "status": "sent",
            }

            message_log = ShippingNotificationLogs(**log)
            db.add(message_log)

            wallet.shipping_notifications = float(
                wallet.shipping_notifications
            ) - float(order_confirmation_rate)
            db.add(wallet)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={"notifications_balance": wallet.shipping_notifications},
                message="successfull",
            )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error getting balance: {}".format(str(e)),
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

    # @staticmethod
    # def get_notification_rates():

    #     try:

    #         client_id = context_user_data.get().client_id

    #         with get_db_session() as db:

    #             notification_rates = (
    #                 db.query(ShippingNotificationsRate)
    #                 .filter(ShippingNotificationsRate.client_id == client_id)
    #                 .first()
    #             )

    #             if not notification_rates:
    #                 return GenericResponseModel(
    #                     status_code=http.HTTPStatus.OK,
    #                     status=True,
    #                     data={
    #                         "notification_rates": {
    #                             "shipping_notifications": defaultRates[
    #                                 "shipping_notifications"
    #                             ],
    #                             "cod_confirmation": defaultRates["cod_confirmation"],
    #                         }
    #                     },
    #                     message="successfull",
    #                 )

    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.OK,
    #                 status=True,
    #                 data={
    #                     "notification_rates": ShippingNotificationsRateBaseModel(
    #                         **notification_rates.to_model().model_dump()
    #                     )
    #                 },
    #                 message="successfull",
    #             )

    #     except DatabaseError as e:
    #         # Log database error
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Error getting balance: {}".format(str(e)),
    #         )

    #         # Return error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Unable to get balance",
    #         )

    #     except Exception as e:
    #         # Log other unhandled exceptions
    #         logger.error(
    #             extra=context_user_data.get(),
    #             msg="Unhandled error: {}".format(str(e)),
    #         )
    #         # Return a general internal server error response
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Unable to get balance",
    #         )

    @staticmethod
    async def get_notification_rates() -> GenericResponseModel:
        """Get the shipping notification rates asynchronously."""
        try:
            client_id = context_user_data.get().client_id

            async with get_db_session() as db:  # db should be AsyncSession
                result = await db.execute(
                    select(ShippingNotificationsRate).filter(
                        ShippingNotificationsRate.client_id == client_id
                    )
                )
                notification_rates = result.scalars().first()

                if not notification_rates:
                    # Return default rates if none found
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        status=True,
                        data={
                            "notification_rates": {
                                "shipping_notifications": defaultRates[
                                    "shipping_notifications"
                                ],
                                "cod_confirmation": defaultRates["cod_confirmation"],
                            }
                        },
                        message="successful",
                    )

                # Convert DB model to Pydantic BaseModel
                rates_model = ShippingNotificationsRateBaseModel(
                    **notification_rates.to_model().model_dump()
                )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"notification_rates": rates_model},
                    message="successful",
                )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error getting notification rates: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Unable to get notification rates",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Unable to get notification rates",
            )

    # @staticmethod
    # def get_notifications_settings():
    #     try:
    #         client_id = context_user_data.get().client_id

    #         with get_db_session() as db:
    #             notifications_settings = (
    #                 db.query(ShippingNotificationsSetting)
    #                 .filter(ShippingNotificationsSetting.client_id == client_id)
    #                 .first()
    #             )

    #             if not notifications_settings:
    #                 # Return default settings instead of NOT_FOUND
    #                 return GenericResponseModel(
    #                     status_code=http.HTTPStatus.OK,
    #                     status=True,
    #                     data={
    #                         "notification_settings": {
    #                             "order_processed": False,
    #                             "order_shipped": False,
    #                             "order_out_for_delivery": False,
    #                             "order_delivered": False,
    #                         }
    #                     },
    #                     message="Default settings applied",
    #                 )

    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.OK,
    #                 status=True,
    #                 data={
    #                     "notification_settings": ShippingNotificationSettingBaseModel(
    #                         **notifications_settings.to_model().model_dump()
    #                     )
    #                 },
    #                 message="Success",
    #             )

    #     except Exception as e:
    #         logger.error(
    #             f"Unexpected error while fetching notification settings: {str(e)}",
    #             extra=context_user_data.get(),
    #         )
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             status=False,
    #             message="Unable to get settings",
    #         )
    @staticmethod
    async def get_notifications_settings() -> GenericResponseModel:
        """Fetch shipping notification settings asynchronously."""
        try:
            client_id = context_user_data.get().client_id

            # Use async DB session
            async with get_db_session() as db:  # get_db_session should return AsyncSession
                result = await db.execute(
                    select(ShippingNotificationsSetting).filter(
                        ShippingNotificationsSetting.client_id == client_id
                    )
                )
                notifications_settings = result.scalars().first()

                if not notifications_settings:
                    # Return default settings if none exist
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.OK,
                        status=True,
                        data={
                            "notification_settings": {
                                "order_processed": False,
                                "order_shipped": False,
                                "order_out_for_delivery": False,
                                "order_delivered": False,
                            }
                        },
                        message="Default settings applied",
                    )

                # Convert DB model to Pydantic model
                settings_model = ShippingNotificationSettingBaseModel(
                    **notifications_settings.to_model().model_dump()
                )

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"notification_settings": settings_model},
                    message="Success",
                )

        except Exception as e:
            logger.error(
                f"Unexpected error while fetching notification settings: {str(e)}",
                extra=context_user_data.get(),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Unable to get settings",
            )

    # @staticmethod
    # def update_notification_settings(settings: ShippingNotificationSettingBaseModel):
    #     try:
    #         client_id = context_user_data.get().client_id

    #         with get_db_session() as db:
    #             # Check if notification settings exist for the client
    #             notification_settings = (
    #                 db.query(ShippingNotificationsSetting)
    #                 .filter(ShippingNotificationsSetting.client_id == client_id)
    #                 .first()
    #             )

    #             if not notification_settings:
    #                 # Create a new entry if not found
    #                 notification_settings = ShippingNotificationsSetting(
    #                     client_id=client_id,
    #                     order_processed=settings.order_processed,
    #                     order_shipped=settings.order_shipped,
    #                     order_out_for_delivery=settings.order_out_for_delivery,
    #                     order_delivered=settings.order_delivered,
    #                 )
    #                 db.add(notification_settings)
    #             else:
    #                 # Update existing settings
    #                 notification_settings.order_processed = settings.order_processed
    #                 notification_settings.order_shipped = settings.order_shipped
    #                 notification_settings.order_out_for_delivery = (
    #                     settings.order_out_for_delivery
    #                 )
    #                 notification_settings.order_delivered = settings.order_delivered

    #             db.commit()

    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.OK,
    #                 status=True,
    #                 data={"message": "Notification settings updated successfully"},
    #             )

    #     except Exception as e:
    #         logger.error(
    #             f"Error updating notification settings: {str(e)}",
    #             extra=context_user_data.get(),
    #         )

    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             message="Failed to update notification settings.",
    #         )
    @staticmethod
    async def update_notification_settings(
        settings: ShippingNotificationSettingBaseModel,
    ) -> GenericResponseModel:
        """Create or update shipping notification settings asynchronously."""
        try:
            client_id = context_user_data.get().client_id

            # Use async DB session
            async with get_db_session() as db:  # AsyncSession
                # Check if notification settings exist
                result = await db.execute(
                    select(ShippingNotificationsSetting).filter(
                        ShippingNotificationsSetting.client_id == client_id
                    )
                )
                notification_settings = result.scalars().first()

                if not notification_settings:
                    # Create new entry
                    notification_settings = ShippingNotificationsSetting(
                        client_id=client_id,
                        order_processed=settings.order_processed,
                        order_shipped=settings.order_shipped,
                        order_out_for_delivery=settings.order_out_for_delivery,
                        order_delivered=settings.order_delivered,
                    )
                    db.add(notification_settings)
                else:
                    # Update existing settings
                    notification_settings.order_processed = settings.order_processed
                    notification_settings.order_shipped = settings.order_shipped
                    notification_settings.order_out_for_delivery = (
                        settings.order_out_for_delivery
                    )
                    notification_settings.order_delivered = settings.order_delivered

                # Commit asynchronously
                await db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"message": "Notification settings updated successfully"},
                )

        except Exception as e:
            logger.error(
                f"Error updating notification settings: {str(e)}",
                extra=context_user_data.get(),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to update notification settings.",
            )
