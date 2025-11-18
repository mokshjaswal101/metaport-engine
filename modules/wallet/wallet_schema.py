from pydantic import BaseModel, validator, EmailStr
from enum import Enum
from typing import Optional
from datetime import datetime

# schema
from schema.base import DBBaseModel
from modules.client.client_schema import ClientModel


class WalletBaseModel(BaseModel):
    amount: float
    cod_amount: float
    provisional_cod_amount: float
    credit_limit: Optional[float]
    shipping_notifications: float


class WalletModel(WalletBaseModel, DBBaseModel):
    client_id: int


class WalletResponseModel(WalletBaseModel):
    pass


class WalletLogsBaseModel(BaseModel):
    amount: float
    message: str
    transaction_type: str


class WalletLogsModel(WalletLogsBaseModel, DBBaseModel):
    client_id: int
    client: Optional[ClientModel]

    pass


class walletOptionsSchema(BaseModel):
    amount: float
    wallet_type: Optional[str] = "wallet"


class wallet_log(DBBaseModel):
    datetime: datetime
    transaction_type: str
    credit: float
    debit: float
    wallet_balance_amount: float
    cod_balance_amount: float
    reference: str
    client_id: int
    wallet_id: int
    description: Optional[str] = ""


class log_filters(BaseModel):
    start_date: datetime
    end_date: datetime
    batch_size: int
    page_number: int
    log_type: str


class rechargeRecordFilters(BaseModel):
    batch_size: int
    page_number: int
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
