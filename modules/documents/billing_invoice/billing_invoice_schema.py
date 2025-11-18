from pydantic import BaseModel, validator
from typing import Optional, Any
from datetime import datetime
from enum import Enum

# schema
from schema.base import DBBaseModel


class InvoiceStatus(str, Enum):
    draft = "draft"
    issued = "issued"
    paid = "paid"
    overdue = "overdue"
    cancelled = "cancelled"


class BillingInvoiceModel(DBBaseModel):
    invoice_number: str
    client_id: int
    name: str
    url: Optional[str] = None
    invoice_date: datetime
    due_date: Optional[datetime] = None
    paid_date: Optional[datetime] = None

    total_amount: float = 0
    tax_amount: float = 0

    status: InvoiceStatus = InvoiceStatus.issued
    remarks: Optional[str] = None
