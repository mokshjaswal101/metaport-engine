"""
Pickup Locations Report Generator

Generates a CSV report of all pickup locations for a client.
"""

from typing import List, Any, Tuple
from sqlalchemy import func, asc

from .base_generator import BaseReportGenerator
from models import Pickup_Location, Order
from logger import logger


class PickupLocationsGenerator(BaseReportGenerator):
    """
    Generator for Pickup Locations Summary report.

    Exports all pickup locations with:
    - Location details (code, name, type)
    - Contact information
    - Address details
    - Status (active/disabled, default)
    - Associated orders count
    """

    report_type = "pickup_locations"
    report_name = "Pickup Locations Summary"

    def get_headers(self) -> List[str]:
        """Return CSV column headers for pickup locations report."""
        return [
            "Location Code",
            "Location Name",
            "Contact Person",
            "Phone",
            "Email",
            "Alternate Phone",
            "Address",
            "Landmark",
            "Pincode",
            "City",
            "State",
            "Country",
            "Location Type",
            "Status",
            "Is Default",
            "Orders Count",
        ]

    def get_data(self) -> Tuple[List[List[Any]], int]:
        """
        Fetch pickup locations data for the client.

        Returns:
            Tuple of (rows, total_count)
        """
        try:
            # Subquery for orders count
            orders_count_subquery = (
                self.db_session.query(
                    Order.pickup_location_code,
                    func.count(Order.id).label("orders_count"),
                )
                .filter(Order.client_id == self.client_id)
                .group_by(Order.pickup_location_code)
                .subquery()
            )

            # Main query - get all locations with orders count
            query = (
                self.db_session.query(
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
                    Pickup_Location.client_id == self.client_id,
                    Pickup_Location.company_id == self.company_id,
                    Pickup_Location.is_deleted == False,
                )
            )

            # Apply optional status filter
            status_filter = self.filters.get("status")
            if status_filter == "active":
                query = query.filter(Pickup_Location.active == True)
            elif status_filter == "disabled":
                query = query.filter(Pickup_Location.active == False)

            # Order by default first, then by creation date
            query = query.order_by(
                Pickup_Location.is_default.desc(),
                asc(Pickup_Location.created_at),
            )

            # Execute query
            locations = query.all()

            # Build rows
            rows = []
            for location, orders_count in locations:
                row = [
                    location.location_code,
                    location.location_name,
                    location.contact_person_name,
                    location.contact_person_phone,
                    location.contact_person_email,
                    location.alternate_phone or "",
                    location.address,
                    location.landmark or "",
                    location.pincode,
                    location.city,
                    location.state,
                    location.country,
                    location.location_type,
                    "Active" if location.active else "Disabled",
                    "Yes" if location.is_default else "No",
                    orders_count,
                ]
                rows.append(row)

            return rows, len(rows)

        except Exception as e:
            logger.error(msg=f"Error fetching pickup locations data: {str(e)}")
            raise
