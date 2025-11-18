from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    Numeric,
)
from sqlalchemy.orm import relationship

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class CourierBilling(DBBase, DBBaseClass):
    """
    Basic model to track final freight and tax received from courier partners
    Linked to orders for billing accuracy
    """

    __tablename__ = "courier_billing"

    # Foreign key to order
    order_id = Column(Integer, ForeignKey("order.id"), nullable=False)

    # AWB number for reference
    awb_number = Column(String(255), nullable=True)

    # Final charges received from courier
    final_freight = Column(Numeric(12, 3), nullable=True)
    final_tax = Column(Numeric(12, 3), nullable=True)

    # Calculated charges for internal processing
    calculated_freight = Column(Numeric(12, 3), nullable=True)
    calculated_tax = Column(Numeric(12, 3), nullable=True)

    # Relationships
    order = relationship("Order", back_populates="courier_billing", lazy="noload")

    @classmethod
    def create_billing_record(cls, billing_data):
        """Create a new courier billing record"""
        try:
            db = get_db_session()
            billing_record = cls(**billing_data)
            db.add(billing_record)
            db.flush()
            db.commit()

            logger.info(f"Courier billing record created with ID: {billing_record.id}")
            return billing_record

        except Exception as e:
            logger.error(f"Error creating courier billing record: {str(e)}")
            db.rollback()
            raise

    @classmethod
    def get_by_order_id(cls, order_id):
        """Get courier billing records by order ID"""
        try:
            db = get_db_session()
            return (
                db.query(cls)
                .filter(cls.order_id == order_id, cls.is_deleted.is_(False))
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching courier billing by order ID: {str(e)}")
            return []

    @classmethod
    def bulk_create_billing_records(cls, billing_records_list):
        """Bulk create courier billing records for optimized performance"""
        try:
            db = get_db_session()

            # Use bulk_insert_mappings for maximum performance
            db.bulk_insert_mappings(cls, billing_records_list)
            db.commit()

            logger.info(
                f"Bulk created {len(billing_records_list)} courier billing records"
            )
            return True

        except Exception as e:
            logger.error(f"Error bulk creating courier billing records: {str(e)}")
            db.rollback()
            raise

    @classmethod
    def get_by_awb_list(cls, awb_list):
        """Get existing billing records by AWB list"""
        try:
            db = get_db_session()
            return (
                db.query(cls)
                .filter(cls.awb_number.in_(awb_list), cls.is_deleted.is_(False))
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching courier billing by AWB list: {str(e)}")
            return []
