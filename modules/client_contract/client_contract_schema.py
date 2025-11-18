from pydantic import BaseModel, Json
from typing import Optional, List, Any

# schema
from schema.base import DBBaseModel

from modules.company_contract.company_contract_schema import CompanyContractModel
from modules.shipping_partner.shipping_partner_schema import (
    AggregatorCourierModel,
    AggregatorResponseModel,
)


class CompanyToClientContractInsertModel(BaseModel):
    company_contract_id: int
    client_id: int
    aggregator_courier_id: Optional[int]

    isActive: bool = True
    rate_type: Optional[str] = "forward"


class RateInsertModel(BaseModel):
    contract_id: int
    zone: str
    base_rate: float
    additional_rate: float
    rto_base_rate: float
    rto_additional_rate: float


class RateModel(RateInsertModel, DBBaseModel):
    pass


class CodRateInsertModel(BaseModel):
    contract_id: int
    percentage_rate: float
    absolute_rate: float


class CodRateModel(CodRateInsertModel, DBBaseModel):
    pass


class CompanyToClientContractModel(CompanyToClientContractInsertModel, DBBaseModel):

    company_contract: Optional[CompanyContractModel]
    aggregator_courier: Optional[AggregatorCourierModel]

    rates: Optional[List[RateModel]]
    cod_rates: Optional[List[CodRateModel]]


class RateCardResponseModel(BaseModel):
    # slug: str
    courier: AggregatorResponseModel
    # rates: Any
    # cod_rates: Any
