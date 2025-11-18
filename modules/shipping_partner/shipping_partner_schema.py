from pydantic import BaseModel, Json
from typing import Optional

# schema
from schema.base import DBBaseModel


class AggregatorCourierInsertModel(BaseModel):
    name: str
    slug: str
    aggregator_slug: str

    min_chargeable_weight: float
    additional_weight_bracket: float

    mode: str
    logo: Optional[str]


class AggregatorCourierModel(AggregatorCourierInsertModel, DBBaseModel):
    pass


class AggregatorResponseModel(AggregatorCourierInsertModel):
    pass


class ShippingPartnerInsertModel(BaseModel):
    name: str
    is_aggregator: bool
    slug: str

    mode: Optional[str]
    credentials: Json = {}
    logo: Optional[str]


class ShippingPartnerModel(ShippingPartnerInsertModel, DBBaseModel):
    pass
