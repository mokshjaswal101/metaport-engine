from typing import List, Optional
from datetime import datetime
import http
from logger import logging

from context_manager.context import get_db_session, context_user_data

from models import ChannelMaster, ClientChannelIntegration, IntegrationSyncLog

from modules.channels.channel_schema import (
    ChannelMasterCreate,
    ChannelMasterUpdate,
    ChannelMasterResponse,
    ClientChannelIntegrationCreate,
    ClientChannelIntegrationUpdate,
    ClientChannelIntegrationResponse,
    IntegrationSyncLogCreate,
    IntegrationSyncLogResponse,
    TestConnectionRequest,
    TestConnectionResponse,
)
from schema.base import GenericResponseModel


class ChannelService:

    @staticmethod
    def get_all_channels(include_inactive: bool = False) -> GenericResponseModel:
        """Get all available channels"""
        try:
            db = get_db_session()
            query = db.query(ChannelMaster)
            if not include_inactive:
                query = query.filter(ChannelMaster.is_active == True)

            channels = query.order_by(ChannelMaster.name).all()

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Channels fetched successfully",
                data=[
                    ChannelMasterResponse.model_validate(channel)
                    for channel in channels
                ],
            )
        except Exception as e:
            print(str(e))
            logging.error(f"Error fetching channels: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch channels",
                data={},
            )

    @staticmethod
    def get_channel_by_id(channel_id: int) -> GenericResponseModel:
        """Get channel by ID"""
        try:
            db = get_db_session()
            channel = (
                db.query(ChannelMaster).filter(ChannelMaster.id == channel_id).first()
            )
            if not channel:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Channel not found",
                    data={},
                )
            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Channel fetched successfully",
                data=ChannelMasterResponse.model_validate(channel),
            )
        except Exception as e:
            logging.error(f"Error fetching channel {channel_id}: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch channel",
                data={},
            )

    @staticmethod
    def get_channel_by_slug(slug: str) -> GenericResponseModel:
        """Get channel by slug"""
        try:
            db = get_db_session()
            channel = db.query(ChannelMaster).filter(ChannelMaster.slug == slug).first()
            if not channel:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Channel not found",
                    data={},
                )
            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Channel fetched successfully",
                data=ChannelMasterResponse.model_validate(channel),
            )
        except Exception as e:
            logging.error(f"Error fetching channel by slug {slug}: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch channel",
                data={},
            )

    @staticmethod
    def create_channel(channel_data: ChannelMasterCreate) -> GenericResponseModel:
        """Create a new channel"""
        try:
            db = get_db_session()
            # Check if slug already exists
            existing = (
                db.query(ChannelMaster)
                .filter(ChannelMaster.slug == channel_data.slug)
                .first()
            )
            if existing:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Channel with this slug already exists",
                    data={},
                )

            channel = ChannelMaster(**channel_data.dict())
            db.add(channel)
            db.commit()
            db.refresh(channel)

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.CREATED,
                message="Channel created successfully",
                data=ChannelMasterResponse.model_validate(channel),
            )
        except Exception as e:
            logging.error(f"Error creating channel: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to create channel",
                data={},
            )

    @staticmethod
    def update_channel(
        channel_id: int, channel_data: ChannelMasterUpdate
    ) -> GenericResponseModel:
        """Update an existing channel"""
        try:
            db = get_db_session()
            channel = (
                db.query(ChannelMaster).filter(ChannelMaster.id == channel_id).first()
            )
            if not channel:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Channel not found",
                    data={},
                )

            # Update fields
            for field, value in channel_data.dict(exclude_unset=True).items():
                setattr(channel, field, value)

            db.commit()
            db.refresh(channel)

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Channel updated successfully",
                data=ChannelMasterResponse.model_validate(channel),
            )
        except Exception as e:
            logging.error(f"Error updating channel {channel_id}: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to update channel",
                data={},
            )


class ClientChannelIntegrationService:

    @staticmethod
    def get_client_integrations(client_id: int) -> GenericResponseModel:
        """Get all integrations for a client"""
        try:
            db = get_db_session()
            integrations = (
                db.query(ClientChannelIntegration)
                .join(ChannelMaster)
                .filter(ClientChannelIntegration.client_id == client_id)
                .all()
            )

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Client integrations fetched successfully",
                data=[
                    ClientChannelIntegrationResponse.model_validate(integration)
                    for integration in integrations
                ],
            )
        except Exception as e:
            logging.error(
                f"Error fetching integrations for client {client_id}: {str(e)}"
            )
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch integrations",
                data={},
            )

    @staticmethod
    def get_integration_by_id(integration_id: int) -> GenericResponseModel:
        """Get integration by ID"""
        try:
            db = get_db_session()
            integration = (
                db.query(ClientChannelIntegration)
                .filter(ClientChannelIntegration.id == integration_id)
                .first()
            )
            if not integration:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Integration not found",
                    data={},
                )

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Integration fetched successfully",
                data=ClientChannelIntegrationResponse.model_validate(integration),
            )
        except Exception as e:
            logging.error(f"Error fetching integration {integration_id}: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch integration",
                data={},
            )

    @staticmethod
    def create_integration(
        integration_data: ClientChannelIntegrationCreate,
    ) -> GenericResponseModel:
        """Create a new integration"""
        try:
            db = get_db_session()

            # Check if channel exists
            channel = (
                db.query(ChannelMaster)
                .filter(ChannelMaster.id == integration_data.channel_id)
                .first()
            )
            if not channel:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Channel not found",
                    data={},
                )

            # Check if integration already exists for this client and channel
            existing = (
                db.query(ClientChannelIntegration)
                .filter(
                    ClientChannelIntegration.client_id == integration_data.client_id,
                    ClientChannelIntegration.channel_id == integration_data.channel_id,
                )
                .first()
            )
            if existing:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Integration already exists for this client and channel",
                    data={},
                )

            integration = ClientChannelIntegration(**integration_data.dict())
            db.add(integration)
            db.commit()
            db.refresh(integration)

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.CREATED,
                message="Integration created successfully",
                data=ClientChannelIntegrationResponse.model_validate(integration),
            )
        except Exception as e:
            logging.error(f"Error creating integration: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to create integration",
                data={},
            )

    @staticmethod
    def update_integration(
        integration_id: int, integration_data: ClientChannelIntegrationUpdate
    ) -> GenericResponseModel:
        """Update an existing integration"""
        try:
            db = get_db_session()
            integration = (
                db.query(ClientChannelIntegration)
                .filter(ClientChannelIntegration.id == integration_id)
                .first()
            )
            if not integration:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Integration not found",
                    data={},
                )

            # Update fields
            for field, value in integration_data.dict(exclude_unset=True).items():
                setattr(integration, field, value)

            db.commit()
            db.refresh(integration)

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Integration updated successfully",
                data=ClientChannelIntegrationResponse.model_validate(integration),
            )
        except Exception as e:
            logging.error(f"Error updating integration {integration_id}: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to update integration",
                data={},
            )

    @staticmethod
    def test_connection(integration_id: int) -> GenericResponseModel:
        """Test connection for an integration"""
        try:
            db = get_db_session()
            integration = (
                db.query(ClientChannelIntegration)
                .join(ChannelMaster)
                .filter(ClientChannelIntegration.id == integration_id)
                .first()
            )
            if not integration:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Integration not found",
                    data={},
                )

            channel = integration.channel
            if not channel:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Channel not found",
                    data={},
                )

            # Import the connector factory inside the method to avoid circular imports
            try:
                from modules.channels.channel_connectors import ChannelConnectorFactory

                connector = ChannelConnectorFactory.create_connector(
                    channel.slug, integration.credentials, integration.config
                )
                result = connector.test_connection()

                return GenericResponseModel(
                    status=result.get("success", False),
                    status_code=(
                        http.HTTPStatus.OK
                        if result.get("success")
                        else http.HTTPStatus.BAD_REQUEST
                    ),
                    message=result.get("message", "Connection test completed"),
                    data=result,
                )
            except ValueError as ve:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=str(ve),
                    data={},
                )
        except Exception as e:
            logging.error(
                f"Error testing connection for integration {integration_id}: {str(e)}"
            )
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to test connection",
                data={},
            )

    @staticmethod
    def delete_integration(integration_id: int) -> GenericResponseModel:
        """Delete (deactivate) an integration"""
        try:
            db = get_db_session()
            integration = (
                db.query(ClientChannelIntegration)
                .filter(ClientChannelIntegration.id == integration_id)
                .first()
            )
            if not integration:
                return GenericResponseModel(
                    status=False,
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Integration not found",
                    data={},
                )

            integration.is_active = False
            db.commit()

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Integration deleted successfully",
                data={},
            )
        except Exception as e:
            logging.error(f"Error deleting integration {integration_id}: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to delete integration",
                data={},
            )


class IntegrationSyncService:

    @staticmethod
    def get_integration_logs(
        integration_id: int, limit: int = 50
    ) -> GenericResponseModel:
        """Get sync logs for an integration"""
        try:
            db = get_db_session()
            logs = (
                db.query(IntegrationSyncLog)
                .filter(IntegrationSyncLog.integration_id == integration_id)
                .order_by(IntegrationSyncLog.created_at.desc())
                .limit(limit)
                .all()
            )

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="Sync logs fetched successfully",
                data=[IntegrationSyncLogResponse.model_validate(log) for log in logs],
            )
        except Exception as e:
            logging.error(
                f"Error fetching sync logs for integration {integration_id}: {str(e)}"
            )
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch sync logs",
                data={},
            )

    @staticmethod
    def create_sync_log(log_data: IntegrationSyncLogCreate) -> GenericResponseModel:
        """Create a new sync log"""
        try:
            db = get_db_session()
            log = IntegrationSyncLog(**log_data.dict())
            db.add(log)
            db.commit()
            db.refresh(log)

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.CREATED,
                message="Sync log created successfully",
                data=IntegrationSyncLogResponse.model_validate(log),
            )
        except Exception as e:
            logging.error(f"Error creating sync log: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to create sync log",
                data={},
            )
