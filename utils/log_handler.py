"""
Centralized Log Handlers
- ActivityLogHandler: Entity changes (pickup locations, products, customers, etc.)
- OrderLogHandler: Order-specific high-volume logs
- IntegrationLogHandler: Integration lifecycle events

For security/auth events, use AuditLogger from utils/audit_logger.py
"""

from datetime import datetime
from logger import logger


class ActivityLogHandler:
    """
    Centralized handler for entity activity logs.
    """

    # Action types
    ACTION_CREATE = "CREATE"
    ACTION_UPDATE = "UPDATE"
    ACTION_DELETE = "DELETE"
    ACTION_SET_DEFAULT = "SET_DEFAULT"
    ACTION_REMOVE_DEFAULT = "REMOVE_DEFAULT"
    ACTION_TOGGLE_STATUS = "TOGGLE_STATUS"
    ACTION_RESTORE = "RESTORE"
    ACTION_ARCHIVE = "ARCHIVE"

    @classmethod
    def log(
        cls,
        db,
        entity_type: str,
        entity_id: str,
        action: str,
        client_id: int,
        user_id: int = None,
        user_email: str = None,
        old_value: dict = None,
        new_value: dict = None,
        description: str = None,
        extra_data: dict = None,
    ):
        """Create an activity log entry for any entity type."""
        try:
            from models import ActivityLog
            from utils.request_helper import RequestHelper

            request_info = RequestHelper.get_request_info()

            activity_log = ActivityLog(
                entity_type=entity_type,
                entity_id=str(entity_id),
                action=action,
                client_id=client_id,
                user_id=user_id,
                user_email=user_email,
                old_value=old_value,
                new_value=new_value,
                description=description,
                ip_address=request_info.get("ip_address"),
                user_agent=request_info.get("user_agent"),
                endpoint=request_info.get("endpoint"),
                extra_data=extra_data,
            )
            db.add(activity_log)
            return activity_log

        except Exception as e:
            logger.error(f"[ActivityLogHandler] Failed to log activity: {str(e)}")
            return None


class OrderLogHandler:
    """
    Centralized handler for order audit logs.
    """

    # Action types
    ACTION_CREATED = "created"
    ACTION_UPDATED = "updated"
    ACTION_AWB_ASSIGNED = "awb_assigned"
    ACTION_STATUS_CHANGED = "status_changed"
    ACTION_PICKUP_SCHEDULED = "pickup_scheduled"
    ACTION_PICKED_UP = "picked_up"
    ACTION_IN_TRANSIT = "in_transit"
    ACTION_OUT_FOR_DELIVERY = "out_for_delivery"
    ACTION_DELIVERED = "delivered"
    ACTION_RTO_INITIATED = "rto_initiated"
    ACTION_RTO_DELIVERED = "rto_delivered"
    ACTION_CANCELLED = "cancelled"
    ACTION_NDR_RAISED = "ndr_raised"
    ACTION_NDR_ACTION = "ndr_action"
    ACTION_LABEL_GENERATED = "label_generated"
    ACTION_DIMENSIONS_UPDATED = "dimensions_updated"
    ACTION_PICKUP_LOCATION_CHANGED = "pickup_location_changed"
    ACTION_CLONED = "cloned"

    @staticmethod
    def log(
        db,
        order_id: int,
        action: str,
        message: str,
        user_id: int = None,
        user_name: str = None,
        extra_data: dict = None,
    ):
        """Create an order audit log entry."""
        try:
            from models import OrderAuditLog

            log_entry = OrderAuditLog.create_log(
                order_id=order_id,
                action=action,
                message=message,
                user_id=user_id,
                user_name=user_name,
                extra_data=extra_data,
            )
            db.add(log_entry)
            return log_entry

        except Exception as e:
            logger.error(f"[OrderLogHandler] Failed to log order action: {str(e)}")
            return None


class IntegrationLogHandler:
    """
    Centralized handler for integration audit logs.

    """

    # Event types
    EVENT_CONNECTED = "integration_connected"
    EVENT_DISCONNECTED = "integration_disconnected"
    EVENT_RECONNECTED = "integration_reconnected"
    EVENT_PAUSED = "integration_paused"
    EVENT_RESUMED = "integration_resumed"
    EVENT_DELETED = "integration_deleted"
    EVENT_STORE_UNINSTALLED = "store_uninstalled"
    EVENT_TEST_CONNECTION = "test_connection"
    EVENT_WEBHOOK_REGISTERED = "webhook_registered"
    EVENT_CONFIG_UPDATED = "config_updated"
    EVENT_SYNC_STARTED = "sync_started"
    EVENT_SYNC_COMPLETED = "sync_completed"
    EVENT_SYNC_FAILED = "sync_failed"

    # Trigger types
    TRIGGER_USER = "user_action"
    TRIGGER_STORE = "store_action"
    TRIGGER_WEBHOOK = "webhook"
    TRIGGER_SYSTEM = "system"
    TRIGGER_API = "api"

    # Status types
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"

    @staticmethod
    def log(
        db,
        integration_id: int,
        event_type: str,
        trigger: str,
        status: str,
        client_id: int = None,
        event_data: dict = None,
        error_message: str = None,
        error_code: str = None,
        error_details: dict = None,
        occurred_at: datetime = None,
    ):
        """Create an integration audit log entry."""
        try:
            from models import IntegrationAuditLog
            from utils.request_helper import RequestHelper

            request_info = RequestHelper.get_request_info()

            return IntegrationAuditLog.create_audit_log(
                db_session=db,
                integration_id=integration_id,
                event_type=event_type,
                trigger=trigger,
                status=status,
                client_id=client_id,
                event_data=event_data,
                error_message=error_message,
                error_code=error_code,
                error_details=error_details,
                ip_address=request_info.get("ip_address"),
                user_agent=request_info.get("user_agent"),
                occurred_at=occurred_at,
            )
        except Exception as e:
            logger.error(f"[IntegrationLogHandler] Failed to log: {str(e)}")
            return None
