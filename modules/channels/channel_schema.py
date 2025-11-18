from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class ChannelType(str, Enum):
    MARKETPLACE = "marketplace"
    WMS = "wms"
    ERP = "erp"
    POS = "pos"


class ConnectionStatus(str, Enum):
    CONNECTED = "connected"
    FAILED = "failed"
    PENDING = "pending"


class SyncStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"


class SyncType(str, Enum):
    ORDER_IMPORT = "order_import"
    STATUS_UPDATE = "status_update"
    INVENTORY_SYNC = "inventory_sync"


class SyncTrigger(str, Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    WEBHOOK = "webhook"


# Channel Master Schemas
class ChannelMasterBase(BaseModel):
    name: str = Field(..., max_length=255)
    slug: str = Field(..., max_length=100)
    channel_type: ChannelType
    logo_url: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = None
    credentials_schema: Dict[str, Any]
    config_schema: Optional[Dict[str, Any]] = None
    supports_order_import: bool = True
    supports_order_status_update: bool = True
    supports_inventory_sync: bool = False
    supports_webhook: bool = False
    is_active: bool = True
    version: str = "1.0"
    documentation_url: Optional[str] = Field(None, max_length=500)


class ChannelMasterCreate(ChannelMasterBase):
    pass


class ChannelMasterUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=255)
    logo_url: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = None
    credentials_schema: Optional[Dict[str, Any]] = None
    config_schema: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    version: Optional[str] = None
    documentation_url: Optional[str] = Field(None, max_length=500)


class ChannelMasterResponse(ChannelMasterBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Client Channel Integration Schemas
class ClientChannelIntegrationBase(BaseModel):
    channel_id: int
    integration_name: str = Field(..., max_length=255)
    credentials: Dict[str, Any]
    config: Optional[Dict[str, Any]] = None
    auto_sync_enabled: bool = True
    sync_interval_minutes: int = 30
    order_statuses_to_fetch: Optional[List[str]] = None
    webhook_enabled: bool = False
    webhook_url: Optional[str] = Field(None, max_length=500)
    webhook_secret: Optional[str] = Field(None, max_length=255)


class ClientChannelIntegrationCreate(ClientChannelIntegrationBase):
    client_id: int


class ClientChannelIntegrationUpdate(BaseModel):
    integration_name: Optional[str] = Field(None, max_length=255)
    credentials: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    auto_sync_enabled: Optional[bool] = None
    sync_interval_minutes: Optional[int] = None
    order_statuses_to_fetch: Optional[List[str]] = None
    webhook_enabled: Optional[bool] = None
    webhook_url: Optional[str] = Field(None, max_length=500)
    webhook_secret: Optional[str] = Field(None, max_length=255)
    is_active: Optional[bool] = None


class ClientChannelIntegrationResponse(BaseModel):
    id: int
    client_id: int
    channel_id: int
    integration_name: str
    config: Optional[Dict[str, Any]]
    auto_sync_enabled: bool
    sync_interval_minutes: int
    order_statuses_to_fetch: Optional[List[str]]
    last_order_sync_at: Optional[datetime]
    last_successful_sync_at: Optional[datetime]
    webhook_enabled: bool
    webhook_url: Optional[str]
    is_active: bool
    connection_status: ConnectionStatus
    last_connection_test_at: Optional[datetime]
    connection_error_message: Optional[str]
    total_orders_synced: int
    total_sync_errors: int
    last_sync_error_message: Optional[str]
    created_at: datetime
    updated_at: datetime

    # Related data
    channel: Optional[ChannelMasterResponse] = None

    class Config:
        from_attributes = True


# Integration Sync Log Schemas
class IntegrationSyncLogCreate(BaseModel):
    integration_id: int
    sync_type: SyncType
    sync_trigger: SyncTrigger
    sync_data: Optional[Dict[str, Any]] = None


class IntegrationSyncLogResponse(BaseModel):
    id: int
    integration_id: int
    sync_type: SyncType
    sync_trigger: SyncTrigger
    status: SyncStatus
    records_processed: int
    records_successful: int
    records_failed: int
    sync_data: Optional[Dict[str, Any]]
    error_details: Optional[Dict[str, Any]]
    started_at: datetime
    completed_at: Optional[datetime]
    duration_seconds: Optional[int]
    error_message: Optional[str]
    error_code: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


# Comprehensive response schemas
class ClientChannelIntegrationWithLogs(ClientChannelIntegrationResponse):
    recent_logs: List[IntegrationSyncLogResponse] = []


class ChannelListResponse(BaseModel):
    channels: List[ChannelMasterResponse]
    total: int


class ClientIntegrationsResponse(BaseModel):
    integrations: List[ClientChannelIntegrationResponse]
    total: int


# Test connection schemas
class TestConnectionRequest(BaseModel):
    channel_id: int
    credentials: Dict[str, Any]


class TestConnectionResponse(BaseModel):
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None
