from fastapi import APIRouter, HTTPException, Depends
from sqlmodel import Session, select
from typing import List, Optional
from datetime import datetime

from .models import SampleOrder
from .schemas import SampleOrderResponse, SampleOrderCreate, SampleOrderUpdate
from app.core.database import get_session

router = APIRouter()

# --- Sample Order API Routes (moved from main.py) ---
@router.get("/", response_model=List[SampleOrderResponse])
async def get_sample_orders(
    skip: int = 0, 
    limit: int = 100,
    db: Session = Depends(get_session)
):
    """获取样品订单列表"""
    orders = db.exec(select(SampleOrder).offset(skip).limit(limit)).all()
    return orders

@router.get("/{order_id}", response_model=SampleOrderResponse)
async def get_sample_order(order_id: str, db: Session = Depends(get_session)):
    """根据订单ID获取样品订单"""
    order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order_id)).first()
    if not order:
        raise HTTPException(status_code=404, detail="订单不存在")
    return order

@router.post("/", response_model=SampleOrderResponse)
async def create_sample_order(order: SampleOrderCreate, db: Session = Depends(get_session)):
    """创建新的样品订单"""
    existing_order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order.order_id)).first()
    if existing_order:
        raise HTTPException(status_code=400, detail="该订单ID已存在")
        
    db_order = SampleOrder.model_validate(order.model_dump())
    
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    return db_order

@router.put("/{order_id}", response_model=SampleOrderResponse)
async def update_sample_order(order_id: str, order: SampleOrderUpdate, db: Session = Depends(get_session)):
    """更新现有样品订单"""
    db_order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order_id)).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    order_data = order.model_dump(exclude_unset=True)
    for key, value in order_data.items():
        if value is not None and hasattr(db_order, key):
            setattr(db_order, key, value)
    
    db_order.updated_at = datetime.now()
    
    db.commit()
    db.refresh(db_order)
    
    return db_order

@router.delete("/{order_id}")
async def delete_sample_order(order_id: str, db: Session = Depends(get_session)):
    """删除样品订单"""
    db_order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order_id)).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    db.delete(db_order)
    db.commit()
    
    return {"message": "订单已成功删除"}
