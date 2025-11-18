from pydantic import BaseModel, validator
from typing import Optional

# schema
from schema.base import DBBaseModel


class CompanyModel(DBBaseModel):
    company_name: str
    company_code: str


class CompanyInsertModel(BaseModel):
    company_name: str
