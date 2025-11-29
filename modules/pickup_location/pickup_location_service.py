import http
from psycopg2 import DatabaseError
from sqlalchemy import asc, func
from typing import Optional

from context_manager.context import context_user_data, get_db_session

from logger import logger

# models
from models import Pickup_Location, Order, ActivityLog

# schema
from schema.base import GenericResponseModel
from .pickup_location_schema import (
    PickupLocationInsertModel,
    PickupLocationUpdateModel,
    PickupLocationResponseModel,
    PaginatedPickupLocationResponse,
)


class PickupLocationService:
    """Service class for managing pickup locations"""

    # Entity type for activity logging
    ENTITY_TYPE = "pickup_location"

    @staticmethod
    def _log_activity(
        db,
        action: str,
        entity_id: str,
        client_id: int,
        company_id: int,
        user_id: int = None,
        user_email: str = None,
        old_value: dict = None,
        new_value: dict = None,
        description: str = None,
    ):
        """Helper method to log activity"""
        try:
            activity_log = ActivityLog(
                entity_type=PickupLocationService.ENTITY_TYPE,
                entity_id=entity_id,
                action=action,
                client_id=client_id,
                company_id=company_id,
                user_id=user_id,
                user_email=user_email,
                old_value=old_value,
                new_value=new_value,
                description=description,
            )
            db.add(activity_log)
        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Failed to log activity: {str(e)}",
            )

    @staticmethod
    def _location_to_dict(location: Pickup_Location) -> dict:
        """Convert location model to dictionary for logging"""
        return {
            "location_code": location.location_code,
            "location_name": location.location_name,
            "active": location.active,
            "is_default": location.is_default,
            "address": location.address,
            "pincode": location.pincode,
            "city": location.city,
            "state": location.state,
        }

    @staticmethod
    def create_pickup_location(
        pickup_location_data: PickupLocationInsertModel,
    ) -> GenericResponseModel:
        """Create a new pickup location. First location is automatically set as default."""
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id
            user_id = getattr(user_data, "user_id", None)
            user_email = getattr(user_data, "email", None)

            # Extract is_default from request
            set_as_default = pickup_location_data.is_default

            db = get_db_session()

            # Generate location code using DB sequence (thread-safe)
            location_code = Pickup_Location.generate_location_code(db)

            location_data = {
                **pickup_location_data.model_dump(exclude={"is_default"}),
                "client_id": client_id,
                "company_id": company_id,
                "location_code": location_code,
                "courier_location_codes": {},
                "is_default": False,
                "active": True,
            }

            # Check existing location name
            existing_location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_name == pickup_location_data.location_name,
                    Pickup_Location.is_deleted == False,
                )
                .first()
            )

            if existing_location:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    message="A location with this name already exists",
                    data={"location_code": existing_location.location_code},
                )

            # Check if this is the first location (auto-set as default)
            existing_count = (
                db.query(func.count(Pickup_Location.id))
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.is_deleted == False,
                )
                .scalar()
            )

            is_first_location = existing_count == 0

            # Set as default if first location OR user requested it
            if is_first_location or set_as_default:
                # Remove default from existing location if any
                current_default = (
                    db.query(Pickup_Location)
                    .filter(
                        Pickup_Location.client_id == client_id,
                        Pickup_Location.is_default == True,
                        Pickup_Location.is_deleted == False,
                    )
                    .with_for_update()
                    .first()
                )

                if current_default:
                    old_default_value = PickupLocationService._location_to_dict(
                        current_default
                    )
                    current_default.is_default = False
                    db.add(current_default)

                    # Log the default removal
                    PickupLocationService._log_activity(
                        db=db,
                        action="REMOVE_DEFAULT",
                        entity_id=current_default.location_code,
                        client_id=client_id,
                        company_id=company_id,
                        user_id=user_id,
                        user_email=user_email,
                        old_value=old_default_value,
                        new_value={**old_default_value, "is_default": False},
                        description=f"Default removed from '{current_default.location_name}' due to new location creation",
                    )

                location_data["is_default"] = True

            # Create the location
            location_model_instance = Pickup_Location.create_db_entity(location_data)
            db.add(location_model_instance)
            db.flush()

            # Log the creation
            PickupLocationService._log_activity(
                db=db,
                action="CREATE",
                entity_id=location_code,
                client_id=client_id,
                company_id=company_id,
                user_id=user_id,
                user_email=user_email,
                old_value=None,
                new_value=location_data,
                description=f"Created pickup location '{location_data['location_name']}'",
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Location created successfully",
                data={
                    "location_code": location_code,
                    "is_default": location_data["is_default"],
                },
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error creating Pickup Location: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Location.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def set_default_location(
        pickup_location_id: str,
    ) -> GenericResponseModel:
        """Set a pickup location as the default for the client. Uses row locking for concurrency safety."""
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id
            user_id = getattr(user_data, "user_id", None)
            user_email = getattr(user_data, "email", None)

            db = get_db_session()

            # Lock the row for update to prevent concurrent modifications
            location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                    Pickup_Location.is_deleted == False,
                )
                .with_for_update()
                .first()
            )

            if location is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Location not found",
                )

            if not location.active:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Cannot set an inactive location as default. Please enable the location first.",
                )

            if location.is_default:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    message="Location is already the default",
                )

            old_location_value = PickupLocationService._location_to_dict(location)

            # Find and update current default (with lock)
            current_default = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.is_default == True,
                    Pickup_Location.is_deleted == False,
                )
                .with_for_update()
                .first()
            )

            if current_default:
                old_default_value = PickupLocationService._location_to_dict(
                    current_default
                )
                current_default.is_default = False
                db.add(current_default)

                # Log the default removal
                PickupLocationService._log_activity(
                    db=db,
                    action="REMOVE_DEFAULT",
                    entity_id=current_default.location_code,
                    client_id=client_id,
                    company_id=company_id,
                    user_id=user_id,
                    user_email=user_email,
                    old_value=old_default_value,
                    new_value={**old_default_value, "is_default": False},
                    description=f"Default removed from '{current_default.location_name}'",
                )

            # Set new default
            location.is_default = True
            db.add(location)
            db.flush()

            # Log the new default
            PickupLocationService._log_activity(
                db=db,
                action="SET_DEFAULT",
                entity_id=pickup_location_id,
                client_id=client_id,
                company_id=company_id,
                user_id=user_id,
                user_email=user_email,
                old_value=old_location_value,
                new_value={**old_location_value, "is_default": True},
                description=f"Set '{location.location_name}' as default location",
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Default location updated successfully",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Could not update default location: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not update default location",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def toggle_location_active_status(
        pickup_location_id: str,
    ) -> GenericResponseModel:
        """Toggle the active status of a pickup location (enable/disable)"""
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id
            user_id = getattr(user_data, "user_id", None)
            user_email = getattr(user_data, "email", None)

            db = get_db_session()

            # Lock the row for update
            location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                    Pickup_Location.is_deleted == False,
                )
                .with_for_update()
                .first()
            )

            if location is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Location not found",
                )

            # Prevent disabling the default location
            if location.is_default and location.active:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Cannot disable the default location. Please set another location as default first.",
                )

            old_value = PickupLocationService._location_to_dict(location)

            # Toggle the active status
            new_status = not location.active
            location.active = new_status
            db.add(location)
            db.flush()

            status_text = "enabled" if new_status else "disabled"

            # Log the status change
            PickupLocationService._log_activity(
                db=db,
                action="TOGGLE_STATUS",
                entity_id=pickup_location_id,
                client_id=client_id,
                company_id=company_id,
                user_id=user_id,
                user_email=user_email,
                old_value=old_value,
                new_value={**old_value, "active": new_status},
                description=f"Location '{location.location_name}' {status_text}",
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message=f"Location {status_text} successfully",
                data={"active": new_status},
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Could not update location status: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not update location status",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def update_pickup_location(
        pickup_location_id: str,
        update_data: PickupLocationUpdateModel,
    ) -> GenericResponseModel:
        """Update a pickup location. Location code cannot be changed."""
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id
            user_id = getattr(user_data, "user_id", None)
            user_email = getattr(user_data, "email", None)

            db = get_db_session()

            # Lock the row for update
            location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                    Pickup_Location.is_deleted == False,
                )
                .with_for_update()
                .first()
            )

            if location is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Location not found",
                )

            # Get update data, excluding None values
            update_dict = update_data.model_dump(exclude_none=True)

            if not update_dict:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="No fields to update",
                )

            # Check if new location name already exists (if name is being updated)
            if "location_name" in update_dict:
                existing_location = (
                    db.query(Pickup_Location)
                    .filter(
                        Pickup_Location.client_id == client_id,
                        Pickup_Location.location_name == update_dict["location_name"],
                        Pickup_Location.location_code != pickup_location_id,
                        Pickup_Location.is_deleted == False,
                    )
                    .first()
                )

                if existing_location:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        message="A location with this name already exists",
                    )

            old_value = PickupLocationService._location_to_dict(location)

            # Update the location fields
            for field, value in update_dict.items():
                setattr(location, field, value)

            db.add(location)
            db.flush()

            new_value = PickupLocationService._location_to_dict(location)

            # Log the update
            PickupLocationService._log_activity(
                db=db,
                action="UPDATE",
                entity_id=pickup_location_id,
                client_id=client_id,
                company_id=company_id,
                user_id=user_id,
                user_email=user_email,
                old_value=old_value,
                new_value=new_value,
                description=f"Updated pickup location '{location.location_name}'",
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Location updated successfully",
                data=new_value,
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Could not update location: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not update location",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def delete_location(
        pickup_location_id: str,
    ) -> GenericResponseModel:
        """Soft delete a pickup location. Prevents deletion if orders exist."""
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id
            user_id = getattr(user_data, "user_id", None)
            user_email = getattr(user_data, "email", None)

            db = get_db_session()

            # Lock the row for update
            location = (
                db.query(Pickup_Location)
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.location_code == pickup_location_id,
                    Pickup_Location.is_deleted == False,
                )
                .with_for_update()
                .first()
            )

            if location is None:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Location not found",
                )

            if location.is_default:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="Cannot delete the default location. Please set another location as default first.",
                )

            # Check for associated orders
            orders_count = (
                db.query(func.count(Order.id))
                .filter(
                    Order.pickup_location_code == pickup_location_id,
                    Order.client_id == client_id,
                )
                .scalar()
            )

            if orders_count > 0:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message=f"Cannot delete location with {orders_count} associated orders",
                    data={"orders_count": orders_count, "can_delete": False},
                )

            old_value = PickupLocationService._location_to_dict(location)

            # Soft delete
            location.is_deleted = True
            location.active = False
            db.add(location)
            db.flush()

            # Log the deletion
            PickupLocationService._log_activity(
                db=db,
                action="DELETE",
                entity_id=pickup_location_id,
                client_id=client_id,
                company_id=company_id,
                user_id=user_id,
                user_email=user_email,
                old_value=old_value,
                new_value=None,
                description=f"Deleted pickup location '{location.location_name}'",
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Location deleted successfully",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Could not delete location: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not delete location",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def get_pickup_locations(
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
    ) -> GenericResponseModel:
        """Fetch pickup locations with orders count, pagination, and optional search"""
        try:
            user_data = context_user_data.get()
            client_id = user_data.client_id
            company_id = user_data.company_id

            db = get_db_session()

            # Subquery for orders count
            orders_count_subquery = (
                db.query(
                    Order.pickup_location_code,
                    func.count(Order.id).label("orders_count"),
                )
                .filter(Order.client_id == client_id)
                .group_by(Order.pickup_location_code)
                .subquery()
            )

            # Base query
            base_query = (
                db.query(
                    Pickup_Location,
                    func.coalesce(orders_count_subquery.c.orders_count, 0).label(
                        "orders_count"
                    ),
                )
                .outerjoin(
                    orders_count_subquery,
                    Pickup_Location.location_code
                    == orders_count_subquery.c.pickup_location_code,
                )
                .filter(
                    Pickup_Location.client_id == client_id,
                    Pickup_Location.company_id == company_id,
                    Pickup_Location.is_deleted == False,
                )
            )

            # Apply search filter if provided
            if search:
                search_term = f"%{search}%"
                base_query = base_query.filter(
                    (Pickup_Location.location_name.ilike(search_term))
                    | (Pickup_Location.location_code.ilike(search_term))
                    | (Pickup_Location.contact_person_name.ilike(search_term))
                    | (Pickup_Location.address.ilike(search_term))
                    | (Pickup_Location.city.ilike(search_term))
                    | (Pickup_Location.pincode.ilike(search_term))
                )

            # Get total count
            total_count = base_query.count()

            # Calculate pagination
            total_pages = (total_count + page_size - 1) // page_size
            offset = (page - 1) * page_size

            # Get paginated results
            locations_with_counts = (
                base_query.order_by(
                    Pickup_Location.is_default.desc(),
                    asc(Pickup_Location.created_at),
                )
                .offset(offset)
                .limit(page_size)
                .all()
            )

            # Build response
            response_data = []
            for location, orders_count in locations_with_counts:
                location_dict = {
                    "location_code": location.location_code,
                    "location_name": location.location_name,
                    "contact_person_name": location.contact_person_name,
                    "contact_person_phone": location.contact_person_phone,
                    "contact_person_email": location.contact_person_email,
                    "alternate_phone": location.alternate_phone or "",
                    "address": location.address,
                    "landmark": location.landmark or "",
                    "pincode": location.pincode,
                    "city": location.city,
                    "state": location.state,
                    "country": location.country,
                    "location_type": location.location_type,
                    "active": location.active,
                    "is_default": location.is_default,
                    "orders_count": orders_count,
                }
                response_data.append(PickupLocationResponseModel(**location_dict))

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={
                    "locations": response_data,
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_count": total_count,
                        "total_pages": total_pages,
                        "has_next": page < total_pages,
                        "has_prev": page > 1,
                    },
                },
                message="Pickup locations fetched successfully",
            )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error fetching Pickup Locations: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching locations.",
            )

        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Unhandled error: {str(e)}",
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )
