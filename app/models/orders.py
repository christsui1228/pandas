# app/models/orders.py
from datetime import datetime, timezone
from sqlmodel import SQLModel, Field

class Order(SQLModel, table=True):
    """订单数据模型"""
    __tablename__ = "original_orders"
    
    id: int | None = Field(default=None, primary_key=True)
    order_id: str = Field(index=True, unique=True)  # 订单ID
    role: str | None = Field(default=None)  # 角色
    handler: str | None = Field(default=None)  # 处理人
    process: str | None = Field(default=None)  # 工艺
    amount: float | None = Field(default=None)  # 金额
    
    # 高清图相关信息
    picture_amount: int | None = Field(default=None)  # 高清图数
    picture_price: float | None = Field(default=None)  # 印制报价
    picture_cost: float | None = Field(default=None)  # 高清图尺寸成本
    color_cost: float | None = Field(default=None)  # 高清图颜色成本
    work_cost: float | None = Field(default=None)  # 高清图工费成本
    
    # 衣服相关信息
    cloth_price: float | None = Field(default=None)  # 衣服售价总额
    quantity: int | None = Field(default=None)  # 衣服总数
    cloth_cost: float | None = Field(default=None)  # 衣服成本
    cloth_pack_cost: float | None = Field(default=None)  # 叠衣服成本
    cloth_code: str | None = Field(default=None)  # 衣服款式
    color_amount: int | None = Field(default=None)  # 颜色总数
    
    # 客户信息
    customer_name: str | None = Field(default=None, index=True)  # 客户
    phone: str | None = Field(default=None)  # 电话
    shop: str | None = Field(default=None)  # 渠道
    express: str | None = Field(default=None)  # 快递
    order_status: str | None = Field(default=None)  # 订单状态
    
    # 订单处理时间信息
    order_created_date: datetime | None = Field(default=None)  # 下单时间
    order_processed_date: datetime | None = Field(default=None)  # 处理时间
    completion_date: datetime | None = Field(default=None)  # 完成时间
    order_type: str | None = Field(default=None)  # 订单分类
    
    # 备注信息
    notes: str | None = Field(default=None)  # 备注
    
    # 元数据
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))