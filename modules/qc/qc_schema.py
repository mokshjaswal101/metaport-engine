from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class QCItemSchema(BaseModel):
    category: str
    reasonName: str
    brandName: str
    itemName: str
    itemDescription: str
    isMandatory: bool
