from enum import Enum
from uuid import UUID
from typing import Optional, Any, List, Dict
from datetime import datetime

from pydantic import BaseModel, Field, root_validator

# schema
from schema.base import DBBaseModel


class addClientMetaOptions(BaseModel):
    company_id: int
    client_id: int
    type: str
    meta_slug: str
    meta_value: str


class reording(BaseModel):
    meta_slug: str
    meta_value: str


# class AggregatorCourierModel(BaseModel):


class Assigned_Courier_Response_Model(BaseModel):
    slug: str
    name: str
    logo: str


# CONFIG SETTINGS
class Courier_Config_Settings_Model(BaseModel):
    courier_method: str
    status: bool


# CONFIG SETTINGS
class Courier_Deactivate_Model(BaseModel):
    courier_method: str
    status: bool


class meta_options_model(BaseModel):
    meta_slug: str
    meta_value: str
    ordering_key: Optional[int]


class Courier_Response_Model(BaseModel):
    priority_type: str
    uuid: UUID
    meta_options: Optional[List[meta_options_model]] = None
    # courier_type_id: str


class KeyValue(BaseModel):
    ordering_key: int
    key: str
    value: str


class addClientMetaOptionsRequest(BaseModel):
    type: str
    reording: Optional[List[KeyValue]] = Field(default=None)

    @root_validator(pre=True)
    def check_reording_mandatory(cls, values):
        print(f"DEBUG: Type of values: {type(values)}")  # 🔍 Debugging
        print(f"DEBUG: Values received: {values}")  # 🔍 Print received input

        if not isinstance(values, dict):
            raise ValueError("Invalid input format. Expected a dictionary.")

        type_value = values.get("type")
        reording_value = values.get("reording")

        print(f"DEBUG: Type of `type_value`: {type(type_value)}, Value: {type_value}")
        print(
            f"DEBUG: Type of `reording_value`: {type(reording_value)}, Value: {reording_value}"
        )

        if type_value == "custom" and (
            reording_value is None or not isinstance(reording_value, list)
        ):
            raise ValueError(f"'reording' must be a list when type is '{type_value}'")

        return values


class Filter_Rule(BaseModel):
    field_name: str
    operator: Optional[str]
    value_a: List[str]
    value_b: Optional[str]
    type: str


class Courier_Priority_keys(BaseModel):
    slug: str
    name: str
    logo: Optional[str]


class addRulesAndCourierPriority(BaseModel):
    name: str
    rules: List[Filter_Rule]
    courier_priority: List[Courier_Priority_keys]


class Courier_Rules_status(BaseModel):
    uuid: UUID
    status: bool


class Courier_Rules_Response_Schema(BaseModel):
    uuid: UUID
    rule_name: str
    updated_at: datetime
    courier_priority: List[Courier_Priority_keys]
    rules: List[Filter_Rule]
    status: bool


class ordering(BaseModel):
    uuid: UUID
    rule_order: int


class Update_Rule_Model(BaseModel):
    ordering: List[ordering]
