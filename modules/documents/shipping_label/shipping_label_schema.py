from pydantic import BaseModel
from typing import Optional, List

from schema.base import DBBaseModel


class LabelSettingUpdateModel(BaseModel):
    logo_url: Optional[str]

    label_format: str
    order_id_barcode_enabled: bool
    barcode_format: str

    logo_shown: bool
    consignee_phone: bool
    package_dimensions: bool
    weight: bool
    order_date: bool
    payment_type: bool
    company_name: bool
    pickup_address: bool
    product_name: bool
    SKU: bool
    prepaid_amount: bool
    COD_amount: bool


class LabelSettingResponseModel(LabelSettingUpdateModel):
    branding: bool
    message: bool

    pass


class LabelSettingModel(LabelSettingUpdateModel, DBBaseModel):
    branding: bool
    message: bool
    product_name: bool
    pass


class generateLabelRequest(BaseModel):
    order_ids: List[str]


class UploadImageRequest(BaseModel):
    file_name: str


class UploadImageResponse(BaseModel):
    url: str
