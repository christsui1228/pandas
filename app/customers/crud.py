from sqlmodel import Session, select, func
from datetime import datetime
from typing import List, Tuple

from .models import SampleCustomer, BulkCustomer, CustomerFollowUp
from .schemas import (SampleCustomerCreate, SampleCustomerUpdate,
                    BulkCustomerCreate, BulkCustomerUpdate,
                    CustomerFollowUpCreate, CustomerFollowUpUpdate)

# --- Sample Customer CRUD Operations ---

def get_sample_customer(db: Session, customer_id: int) -> SampleCustomer | None:
    """Get a sample customer by ID."""
    return db.get(SampleCustomer, customer_id)

def get_sample_customers(db: Session, skip: int = 0, limit: int = 100) -> Tuple[List[SampleCustomer], int]:
    """Get a list of sample customers with total count."""
    # Query for the current page items
    statement = select(SampleCustomer).offset(skip).limit(limit)
    items = db.exec(statement).all()

    # Query for the total count (without offset and limit)
    count_statement = select(func.count()).select_from(SampleCustomer)
    # Use db.scalar(statement) to get a single scalar result
    total_count = db.scalar(count_statement) or 0

    return items, total_count

def create_sample_customer(db: Session, *, customer_in: SampleCustomerCreate) -> SampleCustomer:
    """Create a new sample customer."""
    # Here you might want to check for duplicates based on customer_name or other unique fields
    # before creating, depending on business logic.
    db_customer = SampleCustomer.model_validate(customer_in)
    db.add(db_customer)
    db.commit()
    db.refresh(db_customer)
    return db_customer

def update_sample_customer(
    db: Session, *, db_customer: SampleCustomer, customer_in: SampleCustomerUpdate
) -> SampleCustomer:
    """Update an existing sample customer."""
    customer_data = customer_in.model_dump(exclude_unset=True)
    for key, value in customer_data.items():
        setattr(db_customer, key, value)
    
    # Ensure updated_at is set
    db_customer.updated_at = datetime.now()
    
    db.add(db_customer)
    db.commit()
    db.refresh(db_customer)
    return db_customer

def delete_sample_customer(db: Session, *, db_customer: SampleCustomer) -> None:
    """Delete a sample customer."""
    db.delete(db_customer)
    db.commit()
    return None

# --- Bulk Customer CRUD Operations ---

def get_bulk_customer(db: Session, customer_id: int) -> BulkCustomer | None:
    """Get a bulk customer by ID."""
    return db.get(BulkCustomer, customer_id)

def get_bulk_customers(db: Session, skip: int = 0, limit: int = 100) -> list[BulkCustomer]:
    """Get a list of bulk customers."""
    statement = select(BulkCustomer).offset(skip).limit(limit)
    return db.exec(statement).all()

def create_bulk_customer(db: Session, *, customer_in: BulkCustomerCreate) -> BulkCustomer:
    """Create a new bulk customer."""
    # Similar check for duplicates might be needed here.
    db_customer = BulkCustomer.model_validate(customer_in)
    db.add(db_customer)
    db.commit()
    db.refresh(db_customer)
    return db_customer

def update_bulk_customer(
    db: Session, *, db_customer: BulkCustomer, customer_in: BulkCustomerUpdate
) -> BulkCustomer:
    """Update an existing bulk customer."""
    customer_data = customer_in.model_dump(exclude_unset=True)
    for key, value in customer_data.items():
        setattr(db_customer, key, value)
        
    # Ensure updated_at is set
    db_customer.updated_at = datetime.now()
        
    db.add(db_customer)
    db.commit()
    db.refresh(db_customer)
    return db_customer

def delete_bulk_customer(db: Session, *, db_customer: BulkCustomer) -> None:
    """Delete a bulk customer."""
    db.delete(db_customer)
    db.commit()
    return None

# --- Customer FollowUp CRUD Operations ---

def get_customer_follow_up(db: Session, follow_up_id: int) -> CustomerFollowUp | None:
    """Get a customer follow-up by ID."""
    # Use relationship loading options if needed when fetching related data elsewhere
    return db.get(CustomerFollowUp, follow_up_id)

def get_customer_follow_ups_by_customer_id(
    db: Session, customer_id: int, skip: int = 0, limit: int = 100
) -> List[CustomerFollowUp]:
    """Get all follow-ups for a specific customer."""
    statement = (
        select(CustomerFollowUp)
        .where(CustomerFollowUp.sample_customer_id == customer_id)
        .offset(skip)
        .limit(limit)
        # Optional: Order by date, e.g., .order_by(CustomerFollowUp.contact_time.desc())
    )
    results = db.exec(statement).all()
    return results

def create_customer_follow_up(
    db: Session, *, follow_up_in: CustomerFollowUpCreate, customer_id: int
) -> CustomerFollowUp:
    """Create a new customer follow-up for a given customer."""
    
    follow_up_data = follow_up_in.model_dump()
    
    # Ensure the customer_id from the path parameter is used, overriding any potential value from the body
    follow_up_data['customer_id'] = customer_id 
        
    # Create the instance using only the dictionary after ensuring correct customer_id
    db_follow_up = CustomerFollowUp(**follow_up_data)

    db.add(db_follow_up)
    db.commit()
    db.refresh(db_follow_up)
    return db_follow_up

def update_customer_follow_up(
    db: Session, *, db_follow_up: CustomerFollowUp, follow_up_in: CustomerFollowUpUpdate
) -> CustomerFollowUp:
    """Update an existing customer follow-up."""
    # follow_up_in comes from CustomerFollowUpUpdate schema, which has all fields optional.
    update_data = follow_up_in.model_dump(exclude_unset=True) # exclude_unset ensures only provided fields are updated

    # Prevent updating the customer_id
    update_data.pop('customer_id', None) 

    for key, value in update_data.items():
        setattr(db_follow_up, key, value)

    # SQLModel models typically don't automatically update 'updated_at' unless configured with a default_factory or Pydantic logic.
    # If your CustomerFollowUp model has `updated_at: datetime = Field(default_factory=datetime.utcnow, sa_column_kwargs={"onupdate": datetime.utcnow})`
    # then the database might handle it. Otherwise, update manually:
    db_follow_up.updated_at = datetime.now() # Consider timezone if applicable, e.g., datetime.now(timezone.utc)

    db.add(db_follow_up) # Add the existing object to the session to mark it as dirty
    db.commit()
    db.refresh(db_follow_up)
    return db_follow_up

def delete_customer_follow_up(db: Session, *, follow_up_id: int) -> CustomerFollowUp | None:
    """Delete a customer follow-up by ID."""
    db_follow_up = db.get(CustomerFollowUp, follow_up_id)
    if db_follow_up:
        db.delete(db_follow_up)
        db.commit()
        return db_follow_up # Optionally return the deleted object or just None
    return None # Follow-up not found
