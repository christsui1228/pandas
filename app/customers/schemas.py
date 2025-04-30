from sqlmodel import SQLModel, Field
from datetime import datetime, date
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

# Remove or rename the old SampleCustomerRead
# class SampleCustomerRead(SampleCustomerBase):
#    id: int
#    ...
#    class Config:
#        from_attributes = True

# --- Customer FollowUp Schemas ---

class CustomerFollowUpBase(SQLModel):
    contact_time: datetime | None = None
    follow_up_type: str | None = None
    follow_description: str
    next_contact_time: datetime | None = None
    created_by: str | None = None
    sample_customer_id: int

class CustomerFollowUpCreate(CustomerFollowUpBase):
    pass

class CustomerFollowUpUpdate(SQLModel):
    contact_time: datetime | None = None
    follow_up_type: str | None = None
    follow_description: str | None = None
    next_contact_time: datetime | None = None
    created_by: str | None = None

class CustomerFollowUpRead(CustomerFollowUpBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# --- Updated Sample Customer Read Schema ---
# Replace the original SampleCustomerRead with this one that includes follow-ups
class SampleCustomerReadWithFollowUps(SampleCustomerBase):
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
    follow_ups: List[CustomerFollowUpRead] = [] # Add the list of follow-ups

    class Config:
        from_attributes = True

# --- Add Response Model for Paginated List ---
class SampleCustomerListResponse(SQLModel):
    items: List[SampleCustomerReadWithFollowUps] # Use the detailed schema for now
    total: int

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
