import http
from fastapi import APIRouter, Query
from typing import List

from modules.channels.channel_service import (
    ChannelService,
    ClientChannelIntegrationService,
    IntegrationSyncService,
)

from .channel_schema import (
    ChannelMasterCreate,
    ChannelMasterUpdate,
    ClientChannelIntegrationCreate,
    ClientChannelIntegrationUpdate,
    IntegrationSyncLogCreate,
)
from context_manager.context import context_user_data
from schema.base import GenericResponseModel
from utils.response_handler import build_api_response

router = APIRouter(prefix="/channels", tags=["Channel Management"])


# Channel Master endpoints
@router.get("/all", response_model=GenericResponseModel)
async def get_all_channels(
    include_inactive: bool = Query(True, description="Include inactive channels")
):
    """Get all available channels for integration"""
    response: GenericResponseModel = ChannelService.get_all_channels(include_inactive=include_inactive)
    return build_api_response(response)


@router.get("/{channel_id}", response_model=GenericResponseModel)
async def get_channel(channel_id: int):
    """Get a specific channel by ID"""
    response: GenericResponseModel = ChannelService.get_channel_by_id(channel_id)
    return build_api_response(response)


@router.get("/slug/{slug}", response_model=GenericResponseModel)
async def get_channel_by_slug(slug: str):
    """Get a specific channel by slug"""
    response: GenericResponseModel = ChannelService.get_channel_by_slug(slug)
    return build_api_response(response)


@router.post("/", response_model=GenericResponseModel, status_code=http.HTTPStatus.CREATED)
async def create_channel(
    channel_data: ChannelMasterCreate,
):
    """Create a new channel (Admin only)"""
    # TODO: Add admin permission check
    response: GenericResponseModel = ChannelService.create_channel(channel_data)
    return build_api_response(response)


@router.put("/{channel_id}", response_model=GenericResponseModel)
async def update_channel(
    channel_id: int,
    channel_data: ChannelMasterUpdate,
):
    """Update a channel (Admin only)"""
    # TODO: Add admin permission check
    response: GenericResponseModel = ChannelService.update_channel(channel_id, channel_data)
    return build_api_response(response)


# Client Channel Integration endpoints
@router.get("/integrations/client/{client_id}", response_model=GenericResponseModel)
async def get_client_integrations(
    client_id: int,
):
    """Get all channel integrations for a specific client"""
    # TODO: Add permission check - user should only see their own client's integrations
    response: GenericResponseModel = ClientChannelIntegrationService.get_client_integrations(client_id)
    return build_api_response(response)


@router.get("/integrations/my", response_model=GenericResponseModel)
async def get_my_integrations():
    """Get channel integrations for the current user's client"""
    user_data = context_user_data.get()
    client_id = user_data.client_id
    if not client_id:
        response = GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.BAD_REQUEST,
            message="User not associated with a client",
            data={}
        )
        return build_api_response(response)

    response: GenericResponseModel = ClientChannelIntegrationService.get_client_integrations(client_id)
    return build_api_response(response)


@router.get("/integrations/{integration_id}", response_model=GenericResponseModel)
async def get_integration(
    integration_id: int,
):
    """Get a specific integration by ID"""
    response: GenericResponseModel = ClientChannelIntegrationService.get_integration_by_id(integration_id)
    # TODO: Add permission check - user should only see their own client's integrations
    return build_api_response(response)


@router.post("/integrations", response_model=GenericResponseModel, status_code=http.HTTPStatus.CREATED)
async def create_integration(
    integration_data: ClientChannelIntegrationCreate,
):
    """Create a new channel integration"""
    # Ensure user can only create integrations for their own client
    user_data = context_user_data.get()
    if integration_data.client_id != user_data.client_id:
        response = GenericResponseModel(
            status=False,
            status_code=http.HTTPStatus.FORBIDDEN,
            message="Cannot create integration for other clients",
            data={}
        )
        return build_api_response(response)

    response: GenericResponseModel = ClientChannelIntegrationService.create_integration(integration_data)
    return build_api_response(response)


@router.put("/integrations/{integration_id}", response_model=GenericResponseModel)
async def update_integration(
    integration_id: int,
    integration_data: ClientChannelIntegrationUpdate,
):
    """Update an existing integration"""
    # TODO: Add permission check - user should only update their own client's integrations

    response: GenericResponseModel = ClientChannelIntegrationService.update_integration(
        integration_id, integration_data
    )
    return build_api_response(response)


@router.post("/integrations/{integration_id}/test", response_model=GenericResponseModel)
async def test_integration_connection(
    integration_id: int,
):
    """Test the connection for an integration"""
    # TODO: Add permission check - user should only test their own client's integrations

    response: GenericResponseModel = ClientChannelIntegrationService.test_connection(integration_id)
    return build_api_response(response)


@router.delete("/integrations/{integration_id}", response_model=GenericResponseModel)
async def delete_integration(
    integration_id: int,
):
    """Delete (deactivate) an integration"""
    # TODO: Add permission check - user should only delete their own client's integrations

    response: GenericResponseModel = ClientChannelIntegrationService.delete_integration(integration_id)
    return build_api_response(response)


# Sync Log endpoints
@router.get("/integrations/{integration_id}/logs", response_model=GenericResponseModel)
async def get_integration_logs(
    integration_id: int,
    limit: int = Query(50, ge=1, le=200, description="Number of logs to retrieve"),
):
    """Get sync logs for an integration"""
    # TODO: Add permission check - user should only see their own client's integration logs

    response: GenericResponseModel = IntegrationSyncService.get_integration_logs(integration_id, limit)
    return build_api_response(response)


# Manual sync trigger endpoint
@router.post("/integrations/{integration_id}/sync", response_model=GenericResponseModel)
async def trigger_manual_sync(
    integration_id: int,
    sync_type: str = Query("order_import", description="Type of sync to trigger"),
):
    """Trigger a manual sync for an integration"""
    # TODO: Add permission check and implement actual sync logic
    # TODO: Validate sync_type against SyncType enum

    # This would typically trigger a background job
    # For now, just return a success message
    response = GenericResponseModel(
        status=True,
        status_code=http.HTTPStatus.OK,
        message=f"Manual {sync_type} sync triggered for integration {integration_id}",
        data={}
    )
    return build_api_response(response)
