from fastapi import APIRouter, Depends
from sqlmodel import Session, select
from typing import List

from app.models.customers import SampleCustomer, BulkCustomer
from app.core.database import get_session

router = APIRouter()

@router.get("/samples", response_model=List[SampleCustomer]) # Changed response_model for clarity
async def get_sample_customers(skip: int = 0, limit: int = 100, db: Session = Depends(get_session)):
    """获取样品客户列表"""
    customers = db.exec(select(SampleCustomer).offset(skip).limit(limit)).all()
    return customers

@router.get("/bulk", response_model=List[BulkCustomer]) # Changed response_model for clarity
async def get_bulk_customers(skip: int = 0, limit: int = 100, db: Session = Depends(get_session)):
    """获取批量客户列表"""
    customers = db.exec(select(BulkCustomer).offset(skip).limit(limit)).all()
    return customers
