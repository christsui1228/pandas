from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select, SQLModel
from typing import List, Optional
import uvicorn
from datetime import datetime, date
# 导入数据库模型和配置
from app.models.sample_orders import SampleOrder
from app.models.customers import SampleCustomer, BulkCustomer
from app.core.database import get_session, get_engine

# 获取数据库引擎
engine = get_engine()

# 创建FastAPI应用
app = FastAPI(title="Pandas API", description="样品和批量订单管理系统API")

# 配置CORS以允许前端访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源，在生产环境中应该限制为特定域名
    allow_credentials=False,  # 设置为False当使用allow_origins=["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)

# API请求和响应模型
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
    # 其他必要字段...
    
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
    # 其他可选字段...
    
    class Config:
        arbitrary_types_allowed = True

# 首页路由
@app.get("/")
async def root():
    return {"message": "Pandas API 服务运行中"}

# 样品订单API路由
@app.get("/api/sample-orders", response_model=List[SampleOrderResponse])
async def get_sample_orders(
    skip: int = 0, 
    limit: int = 100,
    db: Session = Depends(get_session)
):
    """获取样品订单列表"""
    orders = db.exec(select(SampleOrder).offset(skip).limit(limit)).all()
    return orders

@app.get("/api/sample-orders/{order_id}", response_model=SampleOrderResponse)
async def get_sample_order(order_id: str, db: Session = Depends(get_session)):
    """根据订单ID获取样品订单"""
    order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order_id)).first()
    if not order:
        raise HTTPException(status_code=404, detail="订单不存在")
    return order

@app.post("/api/sample-orders", response_model=SampleOrderResponse)
async def create_sample_order(order: SampleOrderCreate, db: Session = Depends(get_session)):
    """创建新的样品订单"""
    # 检查订单ID是否已存在
    existing_order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order.order_id)).first()
    if existing_order:
        raise HTTPException(status_code=400, detail="该订单ID已存在")
        
    # 创建新订单对象
    db_order = SampleOrder.model_validate(order.model_dump())
    
    # 添加到数据库
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    return db_order

@app.put("/api/sample-orders/{order_id}", response_model=SampleOrderResponse)
async def update_sample_order(order_id: str, order: SampleOrderUpdate, db: Session = Depends(get_session)):
    """更新现有样品订单"""
    # 查找订单
    db_order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order_id)).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    # 更新字段 - 只更新非None值
    order_data = order.model_dump(exclude_unset=True)
    for key, value in order_data.items():
        if value is not None and hasattr(db_order, key):
            setattr(db_order, key, value)
    
    # 更新时间戳
    db_order.updated_at = datetime.now()
    
    # 提交到数据库
    db.commit()
    db.refresh(db_order)
    
    return db_order

@app.delete("/api/sample-orders/{order_id}")
async def delete_sample_order(order_id: str, db: Session = Depends(get_session)):
    """删除样品订单"""
    # 查找订单
    db_order = db.exec(select(SampleOrder).where(SampleOrder.order_id == order_id)).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="订单不存在")
    
    # 从数据库中删除
    db.delete(db_order)
    db.commit()
    
    return {"message": "订单已成功删除"}

# 样品客户API路由
@app.get("/api/sample-customers")
async def get_sample_customers(skip: int = 0, limit: int = 100, db: Session = Depends(get_session)):
    """获取样品客户列表"""
    customers = db.exec(select(SampleCustomer).offset(skip).limit(limit)).all()
    return customers

# 批量客户API路由
@app.get("/api/bulk-customers")
async def get_bulk_customers(skip: int = 0, limit: int = 100, db: Session = Depends(get_session)):
    """获取批量客户列表"""
    customers = db.exec(select(BulkCustomer).offset(skip).limit(limit)).all()
    return customers

# 应用程序入口点
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
