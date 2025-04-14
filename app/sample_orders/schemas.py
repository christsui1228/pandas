from sqlmodel import SQLModel
from typing import Optional
from datetime import datetime

# --- Pydantic Models for Sample Orders ---
class SampleOrderResponse(SQLModel):
    id: Optional[int] = None
    order_id: str
    customer_name: Optional[str] = None
    phone: Optional[str] = None
    shop: Optional[str] = None
    amount: Optional[float] = None
    order_status: Optional[str] = None
    sample_status: Optional[str] = None
    order_created_date: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True
        arbitrary_types_allowed = True

class SampleOrderCreate(SQLModel):
    order_id: str
    customer_name: Optional[str] = None
    phone: Optional[str] = None
    shop: Optional[str] = None
    amount: Optional[float] = None
    order_status: Optional[str] = None
    sample_status: Optional[str] = None
    order_created_date: Optional[datetime] = None
    
    class Config:
        arbitrary_types_allowed = True

class SampleOrderUpdate(SQLModel):
    order_id: Optional[str] = None
    customer_name: Optional[str] = None
    phone: Optional[str] = None
    shop: Optional[str] = None
    amount: Optional[float] = None
    order_status: Optional[str] = None
    sample_status: Optional[str] = None
    order_created_date: Optional[datetime] = None
    
    class Config:
        arbitrary_types_allowed = True
