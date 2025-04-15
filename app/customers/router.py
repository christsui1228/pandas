from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session, select

from .models import SampleCustomer, BulkCustomer
from . import crud 
from .schemas import (
    SampleCustomerCreate, SampleCustomerRead, SampleCustomerUpdate,
    BulkCustomerCreate, BulkCustomerRead, BulkCustomerUpdate
)
from app.core.database import get_session

router = APIRouter()

# --- Sample Customer Endpoints ---

@router.post("/samples", response_model=SampleCustomerRead, status_code=status.HTTP_201_CREATED)
async def create_sample_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_in: SampleCustomerCreate
):
    """创建新的样品客户"""
    return crud.create_sample_customer(db=db, customer_in=customer_in)

@router.get("/samples", response_model=list[SampleCustomerRead])
async def get_sample_customers_endpoint(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_session)
):
    """获取样品客户列表"""
    customers = crud.get_sample_customers(db, skip=skip, limit=limit)
    return customers

@router.get("/samples/{customer_id}", response_model=SampleCustomerRead)
async def get_sample_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_id: int
):
    """根据ID获取样品客户"""
    db_customer = crud.get_sample_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="样品客户不存在")
    return db_customer

@router.put("/samples/{customer_id}", response_model=SampleCustomerRead)
async def update_sample_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_id: int,
    customer_in: SampleCustomerUpdate
):
    """更新样品客户"""
    db_customer = crud.get_sample_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="样品客户不存在")
    updated_customer = crud.update_sample_customer(
        db=db, db_customer=db_customer, customer_in=customer_in
    )
    return updated_customer

@router.delete("/samples/{customer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_sample_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_id: int
):
    """删除样品客户"""
    db_customer = crud.get_sample_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="样品客户不存在")
    crud.delete_sample_customer(db=db, db_customer=db_customer)
    return None # No content return for 204


# --- Bulk Customer Endpoints ---

@router.post("/bulk", response_model=BulkCustomerRead, status_code=status.HTTP_201_CREATED)
async def create_bulk_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_in: BulkCustomerCreate
):
    """创建新的批量客户"""
    return crud.create_bulk_customer(db=db, customer_in=customer_in)

@router.get("/bulk", response_model=list[BulkCustomerRead])
async def get_bulk_customers_endpoint(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_session)
):
    """获取批量客户列表"""
    customers = crud.get_bulk_customers(db, skip=skip, limit=limit)
    return customers

@router.get("/bulk/{customer_id}", response_model=BulkCustomerRead)
async def get_bulk_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_id: int
):
    """根据ID获取批量客户"""
    db_customer = crud.get_bulk_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="批量客户不存在")
    return db_customer

@router.put("/bulk/{customer_id}", response_model=BulkCustomerRead)
async def update_bulk_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_id: int,
    customer_in: BulkCustomerUpdate
):
    """更新批量客户"""
    db_customer = crud.get_bulk_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="批量客户不存在")
    updated_customer = crud.update_bulk_customer(
        db=db, db_customer=db_customer, customer_in=customer_in
    )
    return updated_customer

@router.delete("/bulk/{customer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_bulk_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_id: int
):
    """删除批量客户"""
    db_customer = crud.get_bulk_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="批量客户不存在")
    crud.delete_bulk_customer(db=db, db_customer=db_customer)
    return None # No content return for 204
