from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Text, TIMESTAMP, Numeric, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB

from database import DBBaseClass, DBBase

import uuid as uuid


class Billing_Invoice(DBBase, DBBaseClass):
    __tablename__ = "billing_invoice"

    invoice_number = Column(String(255), nullable=False)

    invoice_date = Column(TIMESTAMP(timezone=True), nullable=False)
    due_date = Column(TIMESTAMP(timezone=True), nullable=True)
    paid_date = Column(TIMESTAMP(timezone=True), nullable=True)

    total_amount = Column(Numeric(10, 3), nullable=False)
    tax_amount = Column(Numeric(10, 3), nullable=False)
    status = Column(String(255), nullable=False)

    name = Column(String(255), nullable=False)
    url = Column(Text, nullable=True)

    remarks = Column(String(255), nullable=True)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    def to_model(self):
        from modules.documents.billing_invoice.billing_invoice_schema import (
            BillingInvoiceModel,
        )

        return BillingInvoiceModel.model_validate(self)
