from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel

class BulkOrder(SQLModel, table=True):
    """批量订单表模型"""
    __tablename__ = "bulk_orders"
    
    # 主键和外键
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str = Field(index=True, unique=True)
    
    # 订单基本信息
    role: Optional[str] = None
    handler: Optional[str] = None
    process: Optional[str] = None
    amount: Optional[float] = None
    
    # 高清图相关信息
    picture_amount: Optional[int] = None
    picture_price: Optional[float] = None
    picture_cost: Optional[float] = None
    color_cost: Optional[float] = None
    work_cost: Optional[float] = None  # 注意: 修正了拼写，原为 wokr_cost
    
    # 衣服相关信息
    cloth_price: Optional[float] = None
    quantity: Optional[int] = None
    cloth_cost: Optional[float] = None
    cloth_pack_cost: Optional[float] = None
    cloth_code: Optional[str] = None
    color_amount: Optional[int] = None
    
    # 客户信息
    customer_name: Optional[str] = None
    phone: Optional[str] = None
    shop: Optional[str] = None
    express: Optional[str] = None
    order_status: Optional[str] = None
    
    # 订单时间信息
    order_created_date: Optional[datetime] = None
    order_processed_date: Optional[datetime] = None
    completion_date: Optional[datetime] = None
    order_type: Optional[str] = None
    
    # 备注信息
    notes: Optional[str] = None
    
    # 系统字段
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    # 批量订单特有字段
    production_status: Optional[str] = None  # 生产状态
    delivery_date: Optional[datetime] = None  # 预计交付日期
    payment_status: Optional[str] = None     # 付款状态
    shipping_details: Optional[str] = None   # 物流详情
