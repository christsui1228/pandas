from sqlmodel import SQLModel
from typing import Optional
from datetime import datetime

# --- Pydantic Models for Sample Orders ---
class SampleOrderResponse(SQLModel):
    id: int | None = None
    order_id: str
    customer_name: str | None = None
    phone: str | None = None
    shop: str | None = None
    amount: float | None = None
    order_status: str | None = None
    sample_status: str | None = None
    order_created_date: datetime | None = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
        arbitrary_types_allowed = True

class SampleOrderCreate(SQLModel):
    order_id: str
    customer_name: str | None = None
    phone: str | None = None
    shop: str | None = None
    amount: float | None = None
    order_status: str | None = None
    sample_status: str | None = None
    order_created_date: datetime | None = None
    
    class Config:
        from_attributes = True
        arbitrary_types_allowed = True

class SampleOrderUpdate(SQLModel):
    order_id: str | None = None
    customer_name: str | None = None
    phone: str | None = None
    shop: str | None = None
    amount: float | None = None
    order_status: str | None = None
    sample_status: str | None = None
    order_created_date: datetime | None = None
    
    class Config:
        from_attributes = True
        arbitrary_types_allowed = True
