import http
from psycopg2 import DatabaseError
from typing import List, Any
from sqlalchemy import or_, desc, cast, func
from sqlalchemy.types import DateTime, String
from sqlalchemy.orm import joinedload
from datetime import datetime, timezone
import pandas as pd
import base64
from fastapi.encoders import jsonable_encoder
from io import BytesIO

# import requests
import json  # Import the json module


from context_manager.context import context_user_data, get_db_session

from logger import logger

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model, Order_Response_Model
from modules.ndr.ndr_schema import (
    Ndr_filters,
    Ndr_reattempt_escalate,
    Ndr_status_update,
    Bulk_Ndr_reattempt_escalate,
)

# services
from modules.ndr_history.ndr_history_service import NdrHistoryService


# models
from models import Ndr
from models import Order


class NdrService:

    # ✅ NEW: Enhanced status mapping with groups
    # Each key maps to a list of equivalent statuses in the database
    status_groups = {
        "new": ["new", "take_action"],  # New NDR requiring action
        "take_action": ["new", "take_action"],  # Same as new
        "reattempt": ["reattempt", "REATTEMPT"],  # Reattempt statuses
        "REATTEMPT": ["reattempt", "REATTEMPT"],  # Same as reattempt
        "rto": ["rto", "RTO"],  # RTO statuses
        "RTO": ["rto", "RTO"],  # Same as rto
        "delivered": ["delivered", "DELIVERED"],  # Delivered statuses
        "DELIVERED": ["delivered", "DELIVERED"],  # Same as delivered
    }

    # Legacy mapping for backward compatibility
    status_mapping = {
        "take_action": "NDR",
        "NDR": "take_action",
        "reattempt_requested": "REATTEMPT",
        "delivered": "DELIVERED",
        "rto_requested": "RTO",
    }

    @staticmethod
    def get_mapped_statuses(requested_status: str) -> List[str]:
        """
        Get all database statuses that should be included for a requested status

        Args:
            requested_status: The status requested by the user

        Returns:
            List of database statuses to include in the query
        """
        # Check if status exists in groups
        if requested_status in NdrService.status_groups:
            mapped_statuses = NdrService.status_groups[requested_status]
            print(f"🔍 Status mapping: '{requested_status}' → {mapped_statuses}")
            return mapped_statuses

        # Fallback to legacy mapping
        if requested_status in NdrService.status_mapping:
            legacy_status = NdrService.status_mapping[requested_status]
            print(f"🔍 Legacy mapping: '{requested_status}' → ['{legacy_status}']")
            return [legacy_status]

        # If no mapping found, return the original status
        print(f"🔍 No mapping found for '{requested_status}', using as-is")
        return [requested_status]

    @staticmethod
    def _group_status_counts(raw_status_counts: dict) -> dict:
        """
        Group raw status counts according to status mapping groups

        Args:
            raw_status_counts: Dictionary of {status: count} from database

        Returns:
            Dictionary with clean stats structure for frontend display
        """

        # Initialize core status counts
        status_counts = {
            "new": 0,  # new + take_action
            "reattempt": 0,  # reattempt + REATTEMPT
            "rto": 0,  # rto + RTO
            "delivered": 0,  # delivered + DELIVERED
        }

        # Map database statuses to display groups
        status_to_group = {
            "new": "new",
            "take_action": "new",
            "reattempt": "reattempt",
            "REATTEMPT": "reattempt",
            "rto": "rto",
            "RTO": "rto",
            "delivered": "delivered",
            "DELIVERED": "delivered",
        }

        # Group the counts
        for db_status, count in raw_status_counts.items():
            group = status_to_group.get(db_status)
            if group:
                status_counts[group] += count
            else:
                # Handle unknown statuses by adding them directly
                status_counts[db_status] = count
                print(f"⚠️  Unknown status found: {db_status} with count: {count}")

        # Calculate total
        total = sum(
            status_counts[key] for key in ["new", "reattempt", "rto", "delivered"]
        )

        # Return clean stats structure
        stats_result = {
            "stats": {
                "new": status_counts["new"],  # Maps to "Take Action"
                "reattempt": status_counts[
                    "reattempt"
                ],  # Maps to "Re-Attempt Requested"
                "rto": status_counts["rto"],  # Maps to "RTO"
                "delivered": status_counts["delivered"],  # Maps to "Delivered"
                "total": total,  # Maps to "Total" stat card
            }
        }

        print(f"📊 Status count grouping: {raw_status_counts} → {stats_result}")
        return stats_result

    # status_mapping = {
    #     "NDR": "take_action",
    #     "reattempt_requested": "REATTEMPT",
    #     "delivered": "DELIVERED",
    #     "RTO": "rto",
    # }

    def get_time_difference(new_time, old_time, status):
        #  less then  172800 second is 24 hr
        time_difference = datetime.strptime(
            new_time, "%d-%m-%Y %H:%M:%S"
        ) - datetime.strptime(old_time, "%d-%m-%Y %H:%M:%S")
        if int(time_difference.total_seconds()) < 172800:
            print("difference create")
            return status
        else:
            return "RTO"

    @staticmethod
    def get_all_ndr(ndr_filters: Ndr_filters):
        try:
            # filters
            page_number = ndr_filters.page_number
            batch_size = ndr_filters.batch_size
            ndr_status = ndr_filters.ndr_status
            search_term = ndr_filters.search_term
            start_date = ndr_filters.start_date
            end_date = ndr_filters.end_date

            db = get_db_session()
            client_id = context_user_data.get().client_id

            # ✅ NEW: Use enhanced status mapping to support status groups
            mapped_statuses = NdrService.get_mapped_statuses(ndr_status)

            # fetch all the NDRs of the selected type(s)
            query = db.query(Ndr).filter(
                Ndr.client_id == client_id,
                Ndr.status.in_(mapped_statuses),  # Use .in_() for multiple statuses
            )

            # Join Order first before using its fields
            query = query.join(Order)

            # Now you can safely filter using Order fields
            # query = query.filter(
            #     cast(Ndr.datetime, DateTime) >= start_date,
            #     cast(Order.order_date, DateTime) <= end_date,
            # )

            if search_term:
                search_terms = [term.strip() for term in search_term.split(",")]
                query = query.filter(
                    or_(
                        *[
                            or_(
                                Order.order_id == term,
                                Ndr.awb == term,
                                Order.consignee_phone == term,
                            )
                            for term in search_terms
                        ]
                    )
                )

            # Pagination - Get total count before applying pagination
            total_count = query.count()

            # Apply pagination
            ndrs_with_orders = (
                query.offset((page_number - 1) * batch_size).limit(batch_size).all()
            )

            ndrs = [
                {
                    "awb": ndr.awb,
                    "uuid": ndr.uuid,
                    "status": ndr.status,
                    "datetime": ndr.datetime,
                    "attempt": ndr.attempt,
                    "reason": ndr.reason,
                    "order_id": ndr.order.order_id,
                    "order_date": ndr.order.order_date,
                    "payment_mode": ndr.order.payment_mode,
                    "total_value": ndr.order.order_value,
                    "consignee_address": ndr.order.consignee_address,
                    "consignee_phone": ndr.order.consignee_phone,
                    "order": Order_Response_Model(**ndr.order.to_model().model_dump()),
                }
                for ndr in ndrs_with_orders
            ]

            print(6)
            # ✅ FIXED: GET ALL RECORD AND COUNT OF EACH STATUS WITH PROPER GROUPING
            # Get raw counts from database
            result = (
                db.query(Ndr.status, func.count(Ndr.id))
                .filter(Ndr.client_id == client_id)
                .group_by(Ndr.status)
                .all()
            )
            raw_status_counts = {status: count for status, count in result}

            # ✅ NEW: Group status counts according to our status mapping
            grouped_status_counts = NdrService._group_status_counts(raw_status_counts)

            print(8)
            # ✅ FIXED: Add pagination metadata
            pagination_info = {
                "current_page": page_number,
                "batch_size": batch_size,
                "total_records": total_count,
                "total_pages": (total_count + batch_size - 1)
                // batch_size,  # Ceiling division
                "has_next": page_number * batch_size < total_count,
                "has_previous": page_number > 1,
            }

            # ✅ FIXED: Structure response with clean stats and pagination
            response_data = {
                **grouped_status_counts,  # Contains "stats" key with totals
                "ndrs": ndrs,  # NDR records
                "pagination": pagination_info,  # Pagination metadata
            }

            print(response_data, "new status with pagination")
            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="Orders fetched Successfully",
                data=response_data,
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
                data=None,
                status=False,
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
                data=None,
                status=False,
            )

    @staticmethod
    def export_all_ndr(ndr_filters: Ndr_filters):
        try:
            # if search term is present, give it the highest priority and no other filter will be applied
            ndr_status = ndr_filters.ndr_status
            # ndr_type = ndr_filters.ndr_type
            # keyword = ndr_filters.keyword
            start_date = ndr_filters.start_date
            end_date = ndr_filters.end_date
            db = get_db_session()
            client_id = context_user_data.get().client_id
            query = db.query(Ndr)

            # ✅ NEW: Use enhanced status mapping to support status groups
            mapped_statuses = NdrService.get_mapped_statuses(ndr_status)
            logger.info(
                f"NDR Export: Mapped statuses for '{ndr_status}': {mapped_statuses}"
            )

            query = query.filter(
                Ndr.client_id == client_id,
                Ndr.status.in_(mapped_statuses),  # Use .in_() for multiple statuses
            )

            # Join with Order table first, then apply Order filters
            query = query.join(Order, Ndr.order_id == Order.id)

            query = query.filter(
                cast(Order.order_date, DateTime) >= start_date,
                cast(Order.order_date, DateTime) <= end_date,
            )

            # if keyword != "":
            #     if ndr_type == "Order_Id":
            #         query = query.filter(Order.order_id == keyword)
            #     else:
            #         query = query.filter(Order.awb_number == keyword)

            ndrs_with_orders = query.all()
            logger.info(f"NDR Export: Found {len(ndrs_with_orders)} NDR records")

            ndr_data = []

            for ndr in ndrs_with_orders:
                try:
                    body = {
                        "awb": ndr.awb,
                        "status": ndr.status,
                        "datetime": ndr.datetime,
                        "attempt": ndr.attempt,
                        "reason": ndr.reason,
                        "order_id": ndr.order.order_id if ndr.order else "",
                        "order_date": (
                            ndr.order.order_date.strftime("%Y-%m-%d %H:%M:%S")
                            if ndr.order and ndr.order.order_date
                            else ""
                        ),
                        "payment_mode": ndr.order.payment_mode if ndr.order else "",
                        "total_value": ndr.order.order_value if ndr.order else 0,
                        "consignee_address": (
                            ndr.order.consignee_address if ndr.order else ""
                        ),
                        "consignee_phone": (
                            ndr.order.consignee_phone if ndr.order else ""
                        ),
                    }
                    ndr_data.append(body)
                except Exception as row_error:
                    logger.error(
                        f"NDR Export: Error processing NDR row {ndr.id}: {str(row_error)}"
                    )
                    continue

            logger.info(f"NDR Export: Processed {len(ndr_data)} NDR records for export")

            if len(ndr_data) == 0:
                logger.warning("NDR Export: No data found for export")
                # Create empty DataFrame with headers
                df = pd.DataFrame(
                    columns=[
                        "awb",
                        "status",
                        "datetime",
                        "attempt",
                        "reason",
                        "order_id",
                        "order_date",
                        "payment_mode",
                        "total_value",
                        "consignee_address",
                        "consignee_phone",
                    ]
                )
            else:
                df = pd.DataFrame(ndr_data)

            # Create an in-memory bytes buffer
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="NDR")

            # Return the file as a downloadable response
            output.seek(0)
            headers = {
                "Content-Disposition": 'attachment; filename="ndr.xlsx"',
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            }
            return base64.b64encode(output.getvalue()).decode("utf-8")

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
        finally:
            if "db" in locals():
                db.close()

    @staticmethod
    def ndr_reattempt_escalate(ndr_reattempt_escalate: Ndr_reattempt_escalate):
        try:
            db = get_db_session()
            get_result = (
                db.query(Ndr).filter(Ndr.uuid == ndr_reattempt_escalate.uuid).first()
            )
            print(get_result.awb)
            if get_result != None:
                get_result.alternate_phone_number = (
                    ndr_reattempt_escalate.alternatePhoneNumber
                )
                get_result.address = ndr_reattempt_escalate.address
                get_result.status = "REATTEMPT"
                get_result.updated_at = datetime.now(timezone.utc)
                db.add(get_result)
                db.commit()

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    message="Ndr Updated Successfully",
                    status=True,
                )
        except DatabaseError as e:
            # Log database error without context_user_data to avoid serialization issues
            logger.error(
                msg="Database error in NDR reattempt escalate: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while updating the NDR.",
                status=False,
            )

        except Exception as e:
            # Log other unhandled exceptions without context_user_data to avoid serialization issues
            logger.error(
                msg="Unhandled error in NDR reattempt escalate: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
                status=False,
            )

    @staticmethod
    def bulk_ndr_status_change(bulkstatus: Bulk_Ndr_reattempt_escalate):
        try:
            db = get_db_session()
            client_id = context_user_data.get().client_id
            updated_count = 0
            for uuid in bulkstatus.order_ids:
                record = (
                    db.query(Ndr)
                    .filter(Ndr.uuid == uuid, Ndr.client_id == client_id)
                    .first()
                )
                if record:
                    record.status = "REATTEMPT"
                    record.updated_at = datetime.now(timezone.utc)
                    db.add(record)
                    updated_count += 1

            db.commit()  # Commit once after all updates for better performance

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message=f"Successfully updated {updated_count} NDR records",
                status=True,
            )
        except DatabaseError as e:
            # Log database error without context_user_data to avoid serialization issues
            logger.error(
                msg="Database error in bulk NDR status change: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while updating NDR records.",
                status=False,
            )

        except Exception as e:
            # Log other unhandled exceptions without context_user_data to avoid serialization issues
            logger.error(
                msg="Unhandled error in bulk NDR status change: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
                status=False,
            )

    @staticmethod
    @staticmethod
    def health_check_ndr_system():
        """
        ✅ NEW: Health check method to validate NDR system integrity
        """
        try:
            issues = []

            db = get_db_session()
            client_id = context_user_data.get().client_id

            # Check for NDR records without corresponding orders
            orphaned_ndrs = (
                db.query(Ndr)
                .outerjoin(Order, Ndr.order_id == Order.id)
                .filter(Ndr.client_id == client_id, Order.id.is_(None))
                .count()
            )

            if orphaned_ndrs > 0:
                issues.append(f"Found {orphaned_ndrs} orphaned NDR records")

            # Check for orders with NDR status but no NDR records
            from sqlalchemy import text

            orders_with_ndr_status = db.execute(
                text(
                    """
                SELECT COUNT(*) FROM orders o 
                WHERE o.client_id = :client_id 
                AND o.status = 'NDR' 
                AND NOT EXISTS (
                    SELECT 1 FROM ndr n WHERE n.order_id = o.id
                )
            """
                ),
                {"client_id": client_id},
            ).scalar()

            if orders_with_ndr_status > 0:
                issues.append(
                    f"Found {orders_with_ndr_status} orders with NDR status but no NDR records"
                )

            # Check for NDR records with invalid status values using grouped statuses
            all_valid_statuses = set()
            for status_list in NdrService.status_groups.values():
                all_valid_statuses.update(status_list)

            invalid_status_ndrs = (
                db.query(Ndr)
                .filter(
                    Ndr.client_id == client_id,
                    ~Ndr.status.in_(list(all_valid_statuses)),
                )
                .count()
            )

            if invalid_status_ndrs > 0:
                issues.append(
                    f"Found {invalid_status_ndrs} NDR records with invalid status values"
                )

            # Summary with grouped status counts
            total_ndrs = db.query(Ndr).filter(Ndr.client_id == client_id).count()

            # Get status distribution with grouping
            result = (
                db.query(Ndr.status, func.count(Ndr.id))
                .filter(Ndr.client_id == client_id)
                .group_by(Ndr.status)
                .all()
            )
            raw_status_counts = {status: count for status, count in result}
            grouped_status_counts = NdrService._group_status_counts(raw_status_counts)

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=len(issues) == 0,
                data={
                    "total_ndr_records": total_ndrs,
                    "issues_found": len(issues),
                    "issues": issues,
                    "status_mapping": NdrService.status_groups,  # Use status_groups instead of status_mapping
                    "status_distribution": {
                        "raw_counts": raw_status_counts,
                        "grouped_counts": grouped_status_counts,
                    },
                    "health_status": (
                        "healthy" if len(issues) == 0 else "issues_detected"
                    ),
                },
                message="NDR system health check completed",
            )

        except Exception as e:
            logger.error(f"Error in NDR health check: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message=f"Health check failed: {str(e)}",
            )

    def ndr_status_update(ndr_status_update: Ndr_status_update):
        """
        ✅ IMPROVED: Update NDR status with proper validation and error handling
        """
        try:
            if not ndr_status_update or not ndr_status_update.uuid:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid NDR update request - UUID is required",
                    status=False,
                )

            db = get_db_session()
            ndr_record = (
                db.query(Ndr).filter(Ndr.uuid == ndr_status_update.uuid).first()
            )

            if ndr_record is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="NDR record not found",
                    status=False,
                )

            # ✅ IMPROVEMENT: Validate status transitions
            current_status = ndr_record.status
            new_status = getattr(
                ndr_status_update, "status", "RTO"
            )  # Default to RTO if not specified

            # Define valid status transitions
            valid_transitions = {
                "take_action": ["REATTEMPT", "RTO"],
                "REATTEMPT": ["DELIVERED", "RTO"],
                "DELIVERED": [],  # Final state
                "RTO": [],  # Final state
            }

            if current_status in valid_transitions:
                if (
                    valid_transitions[current_status]
                    and new_status not in valid_transitions[current_status]
                ):
                    logger.warning(
                        f"Invalid status transition from {current_status} to {new_status}"
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=f"Invalid status transition from {current_status} to {new_status}",
                        status=False,
                    )
                elif not valid_transitions[current_status]:  # Final state
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message=f"Cannot update NDR in final state: {current_status}",
                        status=False,
                    )

            # ✅ IMPROVEMENT: Update with audit trail
            old_status = ndr_record.status
            ndr_record.status = new_status
            ndr_record.updated_at = datetime.now(timezone.utc)

            # ✅ IMPROVEMENT: Update attempt count for specific transitions
            if new_status == "REATTEMPT":
                ndr_record.attempt += 1

            db.add(ndr_record)
            db.commit()

            logger.info(
                f"NDR status updated from {old_status} to {new_status} for UUID {ndr_status_update.uuid}"
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message=f"NDR status updated successfully from {old_status} to {new_status}",
                status=True,
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error updating NDR status: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database error occurred while updating NDR status.",
                status=False,
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error updating NDR status: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred while updating NDR status.",
                status=False,
            )

    @staticmethod
    def _parse_ndr_datetime(dt_str: str) -> str:
        """
        Parse NDR datetime string in either '%d-%m-%Y %H:%M:%S' or '%Y-%m-%d %H:%M:%S' format.
        Returns the string in '%d-%m-%Y %H:%M:%S' format for consistency.
        """
        from datetime import datetime

        for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(dt_str, fmt)
                return dt.strftime("%d-%m-%Y %H:%M:%S")
            except Exception:
                continue
        # If all parsing fails, return as-is (or raise error if you want strictness)
        return dt_str

    @staticmethod
    def create_ndr(
        ndr_list: Any,
        order: Order_Model,
    ):
        """
        ✅ OPTIMIZED: Create NDR records with proper validation, deduplication, and error handling
        """
        try:
            if not ndr_list or not order:
                logger.warning("Invalid NDR data provided - ndr_list or order is empty")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Invalid NDR data provided",
                    status=False,
                )

            db = get_db_session()
            client_id = context_user_data.get().client_id

            # ✅ FIX: Validate NDR list data
            valid_ndr_events = []
            for ndr_event in ndr_list:
                if (
                    not isinstance(ndr_event, dict)
                    or "datetime" not in ndr_event
                    or "status" not in ndr_event
                ):
                    logger.warning(f"Invalid NDR event structure: {ndr_event}")
                    continue
                # ✅ Robust datetime parsing
                ndr_event["datetime"] = NdrService._parse_ndr_datetime(
                    ndr_event["datetime"]
                )
                valid_ndr_events.append(ndr_event)

            if not valid_ndr_events:
                logger.warning(f"No valid NDR events found for order {order.order_id}")
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="No valid NDR events found",
                    status=False,
                )

            # Get existing NDR record
            ndr_record = (
                db.query(Ndr)
                .filter(Ndr.order_id == order.id, Ndr.client_id == client_id)
                .first()
            )

            print(f"Found existing NDR record: {ndr_record is not None}")

            # Use the latest NDR event (last in chronological order)
            ndr_index = len(valid_ndr_events) - 1
            latest_ndr_event = valid_ndr_events[ndr_index]

            # ✅ IMPROVEMENT: Validate datetime format
            try:
                event_datetime = datetime.strptime(
                    latest_ndr_event["datetime"], "%d-%m-%Y %H:%M:%S"
                )
            except ValueError as e:
                logger.error(
                    f"Invalid datetime format in NDR event: {latest_ndr_event['datetime']}, error: {e}"
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Invalid datetime format: {latest_ndr_event['datetime']}",
                    status=False,
                )

            # ✅ IMPROVEMENT: Validate status mapping
            ndr_status = latest_ndr_event.get("status")
            if ndr_status not in NdrService.status_mapping:
                logger.warning(f"Unknown NDR status: {ndr_status}, using default 'NDR'")
                mapped_status = NdrService.status_mapping.get("NDR", "take_action")
            else:
                mapped_status = NdrService.status_mapping[ndr_status]

            if ndr_record is None:
                # ✅ FIX: Create new NDR record with proper validation
                try:
                    create_new_ndr = {
                        "order_id": int(order.id),
                        "client_id": int(order.client_id),
                        "awb": order.awb_number,
                        "status": mapped_status,
                        "datetime": latest_ndr_event["datetime"],
                        "attempt": 1,
                        "reason": latest_ndr_event.get(
                            "description", "NDR - Non Delivery Report"
                        ),
                    }

                    ndr_model_instance = Ndr.create_db_entity(create_new_ndr)
                    created_ndr = Ndr.create_new_ndr(ndr_model_instance)

                    logger.info(f"Created new NDR record for order {order.order_id}")

                    # ✅ FIX: Create history with proper error handling
                    try:
                        NdrHistoryService.create_ndr_history(
                            valid_ndr_events, order.id, created_ndr.id
                        )
                    except Exception as history_error:
                        logger.error(f"Failed to create NDR history: {history_error}")
                        # Don't fail the entire operation for history creation failure

                except Exception as create_error:
                    logger.error(f"Failed to create new NDR record: {create_error}")
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Failed to create NDR record",
                        status=False,
                    )

            else:
                # ✅ FIX: Update existing NDR record with proper logic
                try:
                    # Calculate time difference and determine status
                    status = NdrService.get_time_difference(
                        latest_ndr_event["datetime"],
                        ndr_record.datetime,
                        latest_ndr_event["status"],
                    )

                    print(
                        f"NDR status comparison: existing='{ndr_record.status}', new='{status}'"
                    )

                    # ✅ FIX: Only update if not in final states
                    if ndr_record.status not in ["DELIVERED", "RTO"]:
                        print("Updating existing NDR record")

                        # ✅ FIX: Check for duplicate datetime entries
                        duplicate_ndr = (
                            db.query(Ndr)
                            .filter(
                                Ndr.order_id == order.id,
                                Ndr.datetime == latest_ndr_event["datetime"],
                                Ndr.client_id == client_id,
                            )
                            .first()
                        )

                        if duplicate_ndr is None:
                            # Update existing record
                            ndr_record.status = NdrService.status_mapping.get(
                                status, mapped_status
                            )
                            ndr_record.datetime = latest_ndr_event["datetime"]
                            ndr_record.attempt = int(ndr_record.attempt + 1)
                            ndr_record.reason = latest_ndr_event.get(
                                "description", ndr_record.reason
                            )
                            ndr_record.updated_at = datetime.now(timezone.utc)

                            db.add(ndr_record)
                            db.commit()

                            logger.info(
                                f"Updated NDR record for order {order.order_id}, attempt: {ndr_record.attempt}"
                            )
                        else:
                            print("Duplicate NDR entry found, updating status only")
                            # Handle delivered status updates
                            if status in ["delivered", "DELIVERED"]:
                                ndr_record.status = NdrService.status_mapping.get(
                                    status, mapped_status
                                )
                                ndr_record.datetime = latest_ndr_event["datetime"]
                                ndr_record.updated_at = datetime.now(timezone.utc)
                                db.add(ndr_record)
                                db.commit()

                        # ✅ FIX: Create history with proper error handling
                        try:
                            # Use ndr_record.id for existing records
                            NdrHistoryService.create_ndr_history(
                                valid_ndr_events, order.id, ndr_record.id
                            )
                        except Exception as history_error:
                            logger.error(
                                f"Failed to create NDR history for existing record: {history_error}"
                            )

                    else:
                        print(
                            f"NDR record not updated - already in final state: {ndr_record.status}"
                        )
                        # Still create history for audit trail
                        try:
                            NdrHistoryService.create_ndr_history(
                                valid_ndr_events, order.id, ndr_record.id
                            )
                        except Exception as history_error:
                            logger.error(
                                f"Failed to create NDR history for final state record: {history_error}"
                            )

                except Exception as update_error:
                    logger.error(
                        f"Failed to update existing NDR record: {update_error}"
                    )
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                        message="Failed to update NDR record",
                        status=False,
                    )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                message="NDR created/updated successfully",
                status=True,
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Database error in NDR creation: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Database error occurred while processing NDR.",
                status=False,
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error in NDR creation: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred while processing NDR.",
                status=False,
            )
