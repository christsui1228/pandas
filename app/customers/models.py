from datetime import datetime
from typing import Optional, Dict, List
from sqlmodel import Field, SQLModel, Column, JSON


class SampleCustomer(SQLModel, table=True):
    """样品订单客户表模型"""
    __tablename__ = "sample_customers"
    
    # 主键和基本标识
    id: Optional[int] = Field(default=None, primary_key=True) 
    customer_name: str = Field(index=True)  # 客户名称
    
    # 业务信息
    shop: Optional[str] = Field(default=None)  # 渠道/店铺
    region: Optional[str] = Field(default=None)  # 区域
    handler: Optional[str] = Field(default=None)  # 处理人
    
    # 样品订单统计信息
    first_sample_date: Optional[datetime] = None  # 首次样品订单日期
    last_sample_date: Optional[datetime] = None  # 最近样品订单日期
    total_sample_amount: float = Field(default=0.0)  # 样品订单总金额
    sample_orders_count: int = Field(default=0)  # 样品订单数量
    
    # 转化状态
    converted_to_bulk: bool = Field(default=False)  # 是否已转化为批量客户
    conversion_date: Optional[datetime] = None  # 转化日期
    
    # 关联到批量客户
    bulk_customer_id: Optional[int] = None  # 关联的批量客户ID
    
    # 额外信息
    notes: Optional[str] = Field(default=None)  # 备注
    tags: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))  # 标签
    wechat: Optional[str] = Field(default=None)  # 微信

    # 跟进时间  
    next_contact_date: Optional[datetime] = None  # 下次跟进日期
    next_contact_week: Optional[datetime] = None  # 下次跟进星期
    next_contact_month: Optional[datetime] = None  # 下次跟进月份
    
    # 丢失原因
    lost_reason: Optional[str] = Field(default=None)  # 失联原因

    # 元数据
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    def __repr__(self):
        return f"SampleCustomer(id={self.id}, name='{self.customer_name}', phone='{self.phone}')"


class BulkCustomer(SQLModel, table=True):
    """批量订单客户表模型"""
    __tablename__ = "bulk_customers"
    
    # 主键和基本标识
    id: Optional[int] = Field(default=None, primary_key=True)
    customer_name: str = Field(index=True)  # 客户名称
    phone: Optional[str] = Field(default=None, index=True)  # 电话
    email: Optional[str] = Field(default=None)  # 邮箱
    
    # 业务信息
    shop: Optional[str] = Field(default=None)  # 渠道/店铺
    customer_type: Optional[str] = Field(default=None)  # 客户类型
    region: Optional[str] = Field(default=None)  # 区域
    handler: Optional[str] = Field(default=None)  # 处理人
    
    # 批量订单统计信息
    first_bulk_date: Optional[datetime] = None  # 首次批量订单日期
    last_bulk_date: Optional[datetime] = None  # 最近批量订单日期
    total_bulk_amount: float = Field(default=0.0)  # 批量订单总金额
    bulk_orders_count: int = Field(default=0)  # 批量订单数量
    
    # 转化信息
    converted_from_sample: bool = Field(default=False)  # 是否从样品客户转化而来
    sample_customer_id: Optional[int] = None  # 关联的样品客户ID
    
    # 额外信息
    notes: Optional[str] = Field(default=None)  # 备注
    tags: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))  # 标签
    
    # 元数据
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    def __repr__(self):
        return f"BulkCustomer(id={self.id}, name='{self.customer_name}', phone='{self.phone}')"


class SampleOrderCustomer(SQLModel, table=True):
    """样品订单与客户关联表"""
    __tablename__ = "sample_order_customers"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    sample_customer_id: int = Field(foreign_key="sample_customers.id", index=True)
    order_id: str = Field(foreign_key="sample_orders.order_id", index=True)
    order_date: Optional[datetime] = None  # 订单日期
    amount: Optional[float] = None  # 订单金额
    
    # 元数据
    created_at: datetime = Field(default_factory=datetime.now)


class BulkOrderCustomer(SQLModel, table=True):
    """批量订单与客户关联表"""
    __tablename__ = "bulk_order_customers"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    bulk_customer_id: int = Field(foreign_key="bulk_customers.id", index=True)
    order_id: str = Field(foreign_key="bulk_orders.order_id", index=True)
    order_date: Optional[datetime] = None  # 订单日期
    amount: Optional[float] = None  # 订单金额
    
    # 元数据
    created_at: datetime = Field(default_factory=datetime.now)


class CustomerConversion(SQLModel, table=True):
    """客户转化记录表 - 记录样品客户转化为批量客户的关系"""
    __tablename__ = "customer_conversions"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    sample_customer_id: int = Field(foreign_key="sample_customers.id", index=True)
    bulk_customer_id: int = Field(foreign_key="bulk_customers.id", index=True)
    conversion_date: datetime = Field(default_factory=datetime.now)  # 转化日期
    
    # 可能与转化相关的订单
    sample_order_id: Optional[str] = Field(default=None, foreign_key="sample_orders.order_id")
    bulk_order_id: Optional[str] = Field(default=None, foreign_key="bulk_orders.order_id")
    
    # 转化信息
    conversion_days: Optional[int] = None  # 从样品到批量的转化天数
    conversion_notes: Optional[str] = None  # 转化备注
    
    # 元数据
    created_at: datetime = Field(default_factory=datetime.now)
