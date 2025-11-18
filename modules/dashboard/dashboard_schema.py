from datetime import datetime

from pydantic import BaseModel


class dashboard_filters(BaseModel):
    start_date: datetime
    end_date: datetime
