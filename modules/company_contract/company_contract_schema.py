from pydantic import BaseModel
from typing import List, Dict, Optional

# schema
from schema.base import DBBaseModel
from modules.company.company_schema import CompanyModel
from modules.shipping_partner.shipping_partner_schema import ShippingPartnerModel


class CompanyContractInsertModel(BaseModel):
    company_id: int
    shipping_partner_id: int

    credentials: Dict[str, str] = {}
    isActive: bool

    tracking_series: int


class CompanyContractModel(CompanyContractInsertModel, DBBaseModel):

    company: Optional[CompanyModel]

    shipping_partner: Optional[ShippingPartnerModel]
