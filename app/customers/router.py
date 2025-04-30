from fastapi import APIRouter, Depends, HTTPException, status, Response
from sqlmodel import Session, select
from typing import List

from .models import SampleCustomer, BulkCustomer
from . import crud 
from .schemas import (
    SampleCustomerCreate, SampleCustomerReadWithFollowUps, SampleCustomerUpdate,
    BulkCustomerCreate, BulkCustomerRead, BulkCustomerUpdate,
    CustomerFollowUpCreate, CustomerFollowUpRead, CustomerFollowUpUpdate,
    SampleCustomerListResponse
)
from app.core.database import get_session

router = APIRouter()

# --- Sample Customer Endpoints ---

@router.post("/samples", response_model=SampleCustomerReadWithFollowUps, status_code=status.HTTP_201_CREATED)
async def create_sample_customer_endpoint(
    *, 
    db: Session = Depends(get_session),
    customer_in: SampleCustomerCreate
):
    """创建新的样品客户"""
    return crud.create_sample_customer(db=db, customer_in=customer_in)

@router.get("/samples", response_model=SampleCustomerListResponse)
async def get_sample_customers_endpoint(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_session)
):
    """获取样品客户列表 (带总数)"""
    customers, total_count = crud.get_sample_customers(db, skip=skip, limit=limit)
    return SampleCustomerListResponse(items=customers, total=total_count)

@router.get("/samples/{customer_id}", response_model=SampleCustomerReadWithFollowUps)
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

@router.put("/samples/{customer_id}", response_model=SampleCustomerReadWithFollowUps)
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
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- Customer FollowUp Endpoints ---

@router.post("/{customer_id}/followups/", response_model=CustomerFollowUpRead, status_code=status.HTTP_201_CREATED)
async def create_customer_follow_up_endpoint(
    *,
    db: Session = Depends(get_session),
    customer_id: int,
    follow_up_in: CustomerFollowUpCreate
):
    """为指定客户创建新的跟进记录"""
    # Optional: Verify customer exists first (SampleCustomer in this case)
    db_customer = crud.get_sample_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="样品客户不存在")

    # Pass customer_id explicitly to the crud function
    return crud.create_customer_follow_up(db=db, follow_up_in=follow_up_in, customer_id=customer_id)

@router.get("/{customer_id}/followups/", response_model=List[CustomerFollowUpRead])
async def get_customer_follow_ups_for_customer_endpoint(
    *,
    db: Session = Depends(get_session),
    customer_id: int,
    skip: int = 0,
    limit: int = 100
):
    """获取指定客户的所有跟进记录"""
    # Optional: Verify customer exists first
    db_customer = crud.get_sample_customer(db=db, customer_id=customer_id)
    if not db_customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="样品客户不存在")

    follow_ups = crud.get_customer_follow_ups_by_customer_id(
        db=db, customer_id=customer_id, skip=skip, limit=limit
    )
    return follow_ups

@router.get("/followups/{followup_id}", response_model=CustomerFollowUpRead)
async def get_single_customer_follow_up_endpoint(
    *,
    db: Session = Depends(get_session),
    followup_id: int
):
    """获取单个跟进记录 (路径: /api/v1/customers/followups/{followup_id})"""
    db_follow_up = crud.get_customer_follow_up(db=db, follow_up_id=followup_id)
    if not db_follow_up:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="跟进记录不存在")
    # Consider adding check if the follow-up belongs to an accessible customer if needed
    return db_follow_up

@router.put("/followups/{followup_id}", response_model=CustomerFollowUpRead)
async def update_customer_follow_up_endpoint(
    *,
    db: Session = Depends(get_session),
    followup_id: int,
    follow_up_in: CustomerFollowUpUpdate
):
    """更新单个跟进记录 (路径: /api/v1/customers/followups/{followup_id})"""
    db_follow_up = crud.get_customer_follow_up(db=db, follow_up_id=followup_id)
    if not db_follow_up:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="跟进记录不存在")

    updated_follow_up = crud.update_customer_follow_up(
        db=db, db_follow_up=db_follow_up, follow_up_in=follow_up_in
    )
    return updated_follow_up

@router.delete("/followups/{followup_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_customer_follow_up_endpoint(
    *,
    db: Session = Depends(get_session),
    followup_id: int
):
    """删除单个跟进记录 (路径: /api/v1/customers/followups/{followup_id})"""
    deleted_follow_up = crud.delete_customer_follow_up(db=db, follow_up_id=followup_id)
    # The CRUD function now returns the deleted object or None if not found
    if deleted_follow_up is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="跟进记录不存在")

    return Response(status_code=status.HTTP_204_NO_CONTENT) # Explicitly return 204 response
