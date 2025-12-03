from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class QCItem(BaseModel):
    parametersName: str
    parametersValue: str
    isMandatory: bool


class QCItemSchema(BaseModel):
    category: str
    reasonName: str
    items: List[QCItem]
