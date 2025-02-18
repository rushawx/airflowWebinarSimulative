import datetime
import uuid

from pydantic import BaseModel
from typing import Optional


class PersonResponse(BaseModel):
    id: uuid.UUID
    name: str
    age: int
    address: str
    email: str
    phone_number: str
    registration_date: datetime.datetime
    created_at: datetime.datetime
    updated_at: datetime.datetime
    deleted_at: Optional[datetime.datetime] = None
