from sqlmodel import SQLModel
from datetime import datetime
from typing import List, Optional

# --- Sample Customer Schemas ---

class SampleCustomerBase(SQLModel):
    customer_name: str
    shop: str | None = None
    region: str | None = None
    handler: str | None = None
    notes: str | None = None
    tags: list[str] | None = None
    wechat: str | None = None
    next_contact_date: datetime | None = None
    next_contact_week: datetime | None = None
    next_contact_month: datetime | None = None
    lost_reason: str | None = None
    # Fields usually calculated/set by backend or relations:
    # first_sample_date: datetime | None = None
    # last_sample_date: datetime | None = None
    # total_sample_amount: float = 0.0
    # sample_orders_count: int = 0
    # converted_to_bulk: bool = False
    # conversion_date: datetime | None = None
    # bulk_customer_id: int | None = None

class SampleCustomerCreate(SampleCustomerBase):
    pass # Inherits all fields from Base, customer_name is required

class SampleCustomerUpdate(SampleCustomerBase):
    # Make all fields optional for updates
    customer_name: str | None = None
    shop: str | None = None
    region: str | None = None
    handler: str | None = None
    notes: str | None = None
    tags: list[str] | None = None
    wechat: str | None = None
    next_contact_date: datetime | None = None
    next_contact_week: datetime | None = None
    next_contact_month: datetime | None = None
    lost_reason: str | None = None
    # Allow updating conversion status?
    converted_to_bulk: bool | None = None
    conversion_date: datetime | None = None
    bulk_customer_id: int | None = None

class SampleCustomerRead(SampleCustomerBase):
    id: int
    # Include calculated/derived fields from the model
    first_sample_date: datetime | None = None
    last_sample_date: datetime | None = None
    total_sample_amount: float
    sample_orders_count: int
    converted_to_bulk: bool
    conversion_date: datetime | None = None
    bulk_customer_id: int | None = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# --- Bulk Customer Schemas ---

class BulkCustomerBase(SQLModel):
    customer_name: str
    phone: str | None = None
    email: str | None = None
    shop: str | None = None
    customer_type: str | None = None
    region: str | None = None
    handler: str | None = None
    notes: str | None = None
    tags: list[str] | None = None
    # Fields usually calculated/set by backend or relations:
    # first_bulk_date: datetime | None = None
    # last_bulk_date: datetime | None = None
    # total_bulk_amount: float = 0.0
    # bulk_orders_count: int = 0
    # converted_from_sample: bool = False
    # sample_customer_id: int | None = None

class BulkCustomerCreate(BulkCustomerBase):
    pass # Inherits all fields from Base, customer_name is required

class BulkCustomerUpdate(BulkCustomerBase):
    # Make all fields optional for updates
    customer_name: str | None = None
    phone: str | None = None
    email: str | None = None
    shop: str | None = None
    customer_type: str | None = None
    region: str | None = None
    handler: str | None = None
    notes: str | None = None
    tags: list[str] | None = None
    # Allow updating conversion status?
    converted_from_sample: bool | None = None
    sample_customer_id: int | None = None

class BulkCustomerRead(BulkCustomerBase):
    id: int
    # Include calculated/derived fields from the model
    first_bulk_date: datetime | None = None
    last_bulk_date: datetime | None = None
    total_bulk_amount: float
    bulk_orders_count: int
    converted_from_sample: bool
    sample_customer_id: int | None = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
