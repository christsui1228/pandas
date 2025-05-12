from datetime import datetime, timezone
from sqlmodel import Field, SQLModel

class BulkOrder(SQLModel, table=True):
    """批量订单表模型"""
    __tablename__ = "bulk_orders"
    
    # 主键和外键
    id: int | None = Field(default=None, primary_key=True)
    order_id: str = Field(index=True, unique=True)
    
    # 订单基本信息
    role: str | None = None
    handler: str | None = None
    process: str | None = None
    amount: float | None = None
    
    # 高清图相关信息
    picture_amount: int | None = None
    picture_price: float | None = None
    picture_cost: float | None = None
    color_cost: float | None = None
    work_cost: float | None = None
    
    # 衣服相关信息
    cloth_price: float | None = None
    quantity: int | None = None
    cloth_cost: float | None = None
    cloth_pack_cost: float | None = None
    cloth_code: str | None = None
    color_amount: int | None = None
    
    # 客户信息
    customer_name: str | None = None
    phone: str | None = None
    shop: str | None = None
    express: str | None = None
    order_status: str | None = None
    
    # 订单时间信息
    order_created_date: datetime | None = None
    order_processed_date: datetime | None = None
    completion_date: datetime | None = None
    order_type: str | None = None
    
    # 备注信息
    notes: str | None = None
    
    # 系统字段
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # 批量订单特有字段
    production_status: str | None = None
    delivery_date: datetime | None = None
    payment_status: str | None = None
    shipping_details: str | None = None
