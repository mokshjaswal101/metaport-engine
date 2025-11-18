from pydantic import BaseModel

# schema


class Ndr_History_Model(BaseModel):
    order_id: int
    ndr_id: int
    status: str
    datetime: str
    reason: str
