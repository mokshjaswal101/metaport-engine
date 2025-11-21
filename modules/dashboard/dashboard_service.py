import http
from psycopg2 import DatabaseError
from sqlalchemy import func, case, cast
from sqlalchemy.types import DateTime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, case, cast, DateTime, select

from logger import logger
from context_manager.context import context_user_data, get_db_session

# models
from models import Order


# schema
from schema.base import GenericResponseModel
from .dashboard_schema import dashboard_filters


class DashboardService:

    @staticmethod
    async def get_performance_data(filters: dashboard_filters) -> GenericResponseModel:
        db: AsyncSession | None = None
        try:
            db = get_db_session()  # make sure this returns AsyncSession
            start_date = filters.start_date.replace(tzinfo=None)  # naive datetime
            end_date = filters.end_date.replace(tzinfo=None)
            client_id = context_user_data.get().client_id

            # === Overall stats ===
            overall_stmt = select(
                func.count().label("total_orders"),
                func.sum(
                    case((Order.status.in_(["booked", "pickup"]), 1), else_=0)
                ).label("booked"),
                func.sum(
                    case(
                        (
                            Order.status.in_(["in transit", "NDR", "out for delivery"]),
                            1,
                        ),
                        else_=0,
                    )
                ).label("in_transit"),
                func.sum(case((Order.status == "delivered", 1), else_=0)).label(
                    "delivered"
                ),
                func.sum(case((Order.status == "RTO", 1), else_=0)).label("rto"),
            ).filter(
                Order.client_id == client_id,
                Order.status.notin_(["cancelled", "new"]),
                Order.is_deleted == False,
                cast(Order.booking_date, DateTime) >= start_date,
                cast(Order.booking_date, DateTime) <= end_date,
            )

            result = await db.execute(overall_stmt)
            overall_stats = result.one()

            total_completed = (overall_stats.delivered or 0) + (overall_stats.rto or 0)
            delivery_percentage = round(
                (
                    ((overall_stats.delivered / total_completed) * 100)
                    if total_completed
                    else 0
                ),
                2,
            )
            rto_percentage = round(
                ((overall_stats.rto / total_completed) * 100) if total_completed else 0,
                2,
            )

            overall_stats_dict = {
                "total_orders": overall_stats.total_orders or 0,
                "booked": overall_stats.booked or 0,
                "in_transit": overall_stats.in_transit or 0,
                "delivered": overall_stats.delivered or 0,
                "rto": overall_stats.rto or 0,
                "delivery_percentage": delivery_percentage,
                "rto_percentage": rto_percentage,
            }

            # === Courier-wise stats ===
            courier_stmt = (
                select(
                    Order.courier_partner,
                    func.count().label("total_orders"),
                    func.sum(
                        case((Order.status.in_(["booked", "pickup"]), 1), else_=0)
                    ).label("booked"),
                    func.sum(
                        case(
                            (
                                Order.status.in_(
                                    ["in transit", "NDR", "out for delivery"]
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("in_transit"),
                    func.sum(case((Order.status == "delivered", 1), else_=0)).label(
                        "delivered"
                    ),
                    func.sum(case((Order.status == "RTO", 1), else_=0)).label("rto"),
                )
                .filter(
                    Order.client_id == client_id,
                    Order.status.notin_(["cancelled", "new"]),
                    Order.is_deleted == False,
                    cast(Order.booking_date, DateTime) >= start_date,
                    cast(Order.booking_date, DateTime) <= end_date,
                )
                .group_by(Order.courier_partner)
                .order_by(func.count().desc())
                .limit(10)
            )

            result = await db.execute(courier_stmt)
            courier_stats = result.all()

            courier_stats_list = [
                {
                    "courier_partner": row.courier_partner,
                    "total_orders": row.total_orders or 0,
                    "booked": row.booked or 0,
                    "in_transit": row.in_transit or 0,
                    "delivered": row.delivered or 0,
                    "rto": row.rto or 0,
                    "delivery_percentage": round(
                        (
                            (
                                (
                                    row.delivered
                                    / ((row.delivered or 0) + (row.rto or 0))
                                )
                                * 100
                            )
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                    "rto_percentage": round(
                        (
                            ((row.rto / ((row.delivered or 0) + (row.rto or 0))) * 100)
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                }
                for row in courier_stats
            ]

            # === Zone-wise stats ===
            zone_stmt = (
                select(
                    Order.zone,
                    func.count().label("total_orders"),
                    func.sum(
                        case((Order.status.in_(["booked", "pickup"]), 1), else_=0)
                    ).label("booked"),
                    func.sum(
                        case(
                            (
                                Order.status.in_(
                                    ["in transit", "NDR", "out for delivery"]
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("in_transit"),
                    func.sum(case((Order.status == "delivered", 1), else_=0)).label(
                        "delivered"
                    ),
                    func.sum(case((Order.status == "RTO", 1), else_=0)).label("rto"),
                )
                .filter(
                    Order.client_id == client_id,
                    Order.status.notin_(["cancelled", "new"]),
                    Order.is_deleted == False,
                    cast(Order.booking_date, DateTime) >= start_date,
                    cast(Order.booking_date, DateTime) <= end_date,
                )
                .group_by(Order.zone)
                .order_by(func.count().desc())
                .limit(10)
            )

            result = await db.execute(zone_stmt)
            zone_stats = result.all()

            zone_stats_list = [
                {
                    "zone": row.zone,
                    "total_orders": row.total_orders or 0,
                    "booked": row.booked or 0,
                    "in_transit": row.in_transit or 0,
                    "delivered": row.delivered or 0,
                    "rto": row.rto or 0,
                    "delivery_percentage": round(
                        (
                            (
                                (
                                    row.delivered
                                    / ((row.delivered or 0) + (row.rto or 0))
                                )
                                * 100
                            )
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                    "rto_percentage": round(
                        (
                            ((row.rto / ((row.delivered or 0) + (row.rto or 0))) * 100)
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                }
                for row in zone_stats
            ]

            # === City-wise stats ===
            city_stmt = (
                select(
                    Order.consignee_city,
                    func.count().label("total_orders"),
                    func.sum(
                        case((Order.status.in_(["booked", "pickup"]), 1), else_=0)
                    ).label("booked"),
                    func.sum(
                        case(
                            (
                                Order.status.in_(
                                    ["in transit", "NDR", "out for delivery"]
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("in_transit"),
                    func.sum(case((Order.status == "delivered", 1), else_=0)).label(
                        "delivered"
                    ),
                    func.sum(case((Order.status == "RTO", 1), else_=0)).label("rto"),
                )
                .filter(
                    Order.client_id == client_id,
                    Order.status.notin_(["cancelled", "new"]),
                    Order.is_deleted == False,
                    cast(Order.booking_date, DateTime) >= start_date,
                    cast(Order.booking_date, DateTime) <= end_date,
                )
                .group_by(Order.consignee_city)
                .order_by(func.count().desc())
                .limit(10)
            )

            result = await db.execute(city_stmt)
            city_stats = result.all()

            city_stats_list = [
                {
                    "city": row.consignee_city,
                    "total_orders": row.total_orders or 0,
                    "booked": row.booked or 0,
                    "in_transit": row.in_transit or 0,
                    "delivered": row.delivered or 0,
                    "rto": row.rto or 0,
                    "delivery_percentage": round(
                        (
                            (
                                (
                                    row.delivered
                                    / ((row.delivered or 0) + (row.rto or 0))
                                )
                                * 100
                            )
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                    "rto_percentage": round(
                        (
                            ((row.rto / ((row.delivered or 0) + (row.rto or 0))) * 100)
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                }
                for row in city_stats
            ]

            # === State-wise stats ===
            state_stmt = (
                select(
                    Order.consignee_state,
                    func.count().label("total_orders"),
                    func.sum(
                        case((Order.status.in_(["booked", "pickup"]), 1), else_=0)
                    ).label("booked"),
                    func.sum(
                        case(
                            (
                                Order.status.in_(
                                    ["in transit", "NDR", "out for delivery"]
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("in_transit"),
                    func.sum(case((Order.status == "delivered", 1), else_=0)).label(
                        "delivered"
                    ),
                    func.sum(case((Order.status == "RTO", 1), else_=0)).label("rto"),
                )
                .filter(
                    Order.client_id == client_id,
                    Order.status.notin_(["cancelled", "new"]),
                    Order.is_deleted == False,
                    cast(Order.booking_date, DateTime) >= start_date,
                    cast(Order.booking_date, DateTime) <= end_date,
                )
                .group_by(Order.consignee_state)
                .order_by(func.count().desc())
                .limit(10)
            )

            result = await db.execute(state_stmt)
            state_stats = result.all()

            state_stats_list = [
                {
                    "state": row.consignee_state,
                    "total_orders": row.total_orders or 0,
                    "booked": row.booked or 0,
                    "in_transit": row.in_transit or 0,
                    "delivered": row.delivered or 0,
                    "rto": row.rto or 0,
                    "delivery_percentage": round(
                        (
                            (
                                (
                                    row.delivered
                                    / ((row.delivered or 0) + (row.rto or 0))
                                )
                                * 100
                            )
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                    "rto_percentage": round(
                        (
                            ((row.rto / ((row.delivered or 0) + (row.rto or 0))) * 100)
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                }
                for row in state_stats
            ]

            # === Pincode-wise stats ===
            pincode_stmt = (
                select(
                    Order.consignee_pincode,
                    func.count().label("total_orders"),
                    func.sum(
                        case((Order.status.in_(["booked", "pickup"]), 1), else_=0)
                    ).label("booked"),
                    func.sum(
                        case(
                            (
                                Order.status.in_(
                                    ["in transit", "NDR", "out for delivery"]
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("in_transit"),
                    func.sum(case((Order.status == "delivered", 1), else_=0)).label(
                        "delivered"
                    ),
                    func.sum(case((Order.status == "RTO", 1), else_=0)).label("rto"),
                )
                .filter(
                    Order.client_id == client_id,
                    Order.status.notin_(["cancelled", "new"]),
                    Order.is_deleted == False,
                    cast(Order.booking_date, DateTime) >= start_date,
                    cast(Order.booking_date, DateTime) <= end_date,
                )
                .group_by(Order.consignee_pincode)
                .order_by(func.count().desc())
                .limit(10)
            )

            result = await db.execute(pincode_stmt)
            pincode_stats = result.all()

            pincode_stats_list = [
                {
                    "pincode": row.consignee_pincode,
                    "total_orders": row.total_orders or 0,
                    "booked": row.booked or 0,
                    "in_transit": row.in_transit or 0,
                    "delivered": row.delivered or 0,
                    "rto": row.rto or 0,
                    "delivery_percentage": round(
                        (
                            (
                                (
                                    row.delivered
                                    / ((row.delivered or 0) + (row.rto or 0))
                                )
                                * 100
                            )
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                    "rto_percentage": round(
                        (
                            ((row.rto / ((row.delivered or 0) + (row.rto or 0))) * 100)
                            if (row.delivered or 0) + (row.rto or 0)
                            else 0
                        ),
                        2,
                    ),
                }
                for row in pincode_stats
            ]

            # Build final response
            data = {
                "overall_stats": overall_stats_dict,
                "courier_wise_stats": courier_stats_list,
                "zone_wise_stats": zone_stats_list,
                "city_wise_stats": city_stats_list,
                "state_wise_stats": state_stats_list,
                "pincode_wise_stats": pincode_stats_list,
            }

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=data,
                message="Successful",
            )

        except Exception as e:
            logger.error(extra=context_user_data.get(), msg=f"Unhandled error: {e}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )
        finally:
            if db:
                await db.close()
