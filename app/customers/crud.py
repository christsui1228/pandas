from sqlmodel import Session, select
from datetime import datetime

from .models import SampleCustomer, BulkCustomer
from .schemas import (SampleCustomerCreate, SampleCustomerUpdate,
                    BulkCustomerCreate, BulkCustomerUpdate)

# --- Sample Customer CRUD Operations ---

def get_sample_customer(db: Session, customer_id: int) -> SampleCustomer | None:
    """Get a sample customer by ID."""
    return db.get(SampleCustomer, customer_id)

def get_sample_customers(db: Session, skip: int = 0, limit: int = 100) -> list[SampleCustomer]:
    """Get a list of sample customers."""
    statement = select(SampleCustomer).offset(skip).limit(limit)
    return db.exec(statement).all()

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
