import os
from datetime import datetime
from typing import Dict, List, Optional
from sqlmodel import Session, select, text
from app.core.database import get_engine

class OrderSyncService:
    """订单同步服务，负责在各个订单表之间同步数据"""
    
    def __init__(self, session: Optional[Session] = None):
        """初始化同步服务，可选传入现有会话"""
        self.session = session
    
    def _get_session(self) -> Session:
        """获取数据库会话，如果没有现有会话则创建一个新的"""
        if self.session:
            return self.session
        return Session(get_engine())
    
    def sync_sample_orders(self) -> Dict[str, int]:
        """从original_orders同步样品订单数据到sample_orders表"""
        print("开始同步样品订单数据...")
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        session = self._get_session()
        
        try:
            # 使用原生SQL进行高效同步
            # 1. 更新已存在的记录
            update_result = session.execute(
                text("""
                UPDATE sample_orders 
                SET 
                    role = o.role,
                    handler = o.handler,
                    process = o.process,
                    amount = o.amount,
                    picture_amount = o.picture_amount,
                    picture_price = o.picture_price,
                    picture_cost = o.picture_cost,
                    color_cost = o.color_cost,
                    work_cost = o.work_cost,
                    cloth_price = o.cloth_price,
                    quantity = o.quantity,
                    cloth_cost = o.cloth_cost,
                    cloth_pack_cost = o.cloth_pack_cost,
                    cloth_code = o.cloth_code,
                    color_amount = o.color_amount,
                    customer_name = o.customer_name,
                    phone = o.phone,
                    shop = o.shop,
                    express = o.express,
                    order_status = o.order_status,
                    order_created_date = o.order_created_date,
                    order_processed_date = o.order_processed_date,
                    completion_date = o.completion_date,
                    order_type = o.order_type,
                    notes = o.notes,
                    updated_at = NOW()
                FROM original_orders o
                WHERE 
                    sample_orders.order_id = o.order_id
                    AND o.order_type IN ('纯衣看样', '打样单')
                    AND (
                        o.updated_at > sample_orders.updated_at
                        OR sample_orders.updated_at IS NULL
                    )
                """)
            )
            updated_count = update_result.rowcount
            stats["updated"] = updated_count
            
            # 2. 插入新记录
            insert_result = session.execute(
                text("""
                INSERT INTO sample_orders (
                    order_id, role, handler, process, amount,
                    picture_amount, picture_price, picture_cost, color_cost, work_cost,
                    cloth_price, quantity, cloth_cost, cloth_pack_cost, cloth_code, color_amount,
                    customer_name, phone, shop, express, order_status,
                    order_created_date, order_processed_date, completion_date, order_type,
                    notes, created_at, updated_at
                )
                SELECT 
                    o.order_id, o.role, o.handler, o.process, o.amount,
                    o.picture_amount, o.picture_price, o.picture_cost, o.color_cost, o.work_cost,
                    o.cloth_price, o.quantity, o.cloth_cost, o.cloth_pack_cost, o.cloth_code, o.color_amount,
                    o.customer_name, o.phone, o.shop, o.express, o.order_status,
                    o.order_created_date, o.order_processed_date, o.completion_date, o.order_type,
                    o.notes, o.created_at, o.updated_at
                FROM original_orders o
                WHERE 
                    o.order_type IN ('纯衣看样', '打样单')
                    AND NOT EXISTS (
                        SELECT 1 FROM sample_orders s WHERE s.order_id = o.order_id
                    )
                """)
            )
            inserted_count = insert_result.rowcount
            stats["inserted"] = inserted_count
            
            # 提交事务
            session.commit()
            print(f"样品订单同步完成: {stats['inserted']}行新增, {stats['updated']}行更新")
            
        except Exception as e:
            session.rollback()
            print(f"同步样品订单时出错: {str(e)}")
            stats["errors"] = 1
            
        # 如果使用的是新创建的会话，关闭它
        if not self.session:
            session.close()
            
        return stats
    
    def sync_bulk_orders(self) -> Dict[str, int]:
        """从original_orders同步批量订单数据到bulk_orders表"""
        print("开始同步批量订单数据...")
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        session = self._get_session()
        
        try:
            # 使用原生SQL进行高效同步
            # 1. 更新已存在的记录
            update_result = session.execute(
                text("""
                UPDATE bulk_orders 
                SET 
                    role = o.role,
                    handler = o.handler,
                    process = o.process,
                    amount = o.amount,
                    picture_amount = o.picture_amount,
                    picture_price = o.picture_price,
                    picture_cost = o.picture_cost,
                    color_cost = o.color_cost,
                    work_cost = o.work_cost,
                    cloth_price = o.cloth_price,
                    quantity = o.quantity,
                    cloth_cost = o.cloth_cost,
                    cloth_pack_cost = o.cloth_pack_cost,
                    cloth_code = o.cloth_code,
                    color_amount = o.color_amount,
                    customer_name = o.customer_name,
                    phone = o.phone,
                    shop = o.shop,
                    express = o.express,
                    order_status = o.order_status,
                    order_created_date = o.order_created_date,
                    order_processed_date = o.order_processed_date,
                    completion_date = o.completion_date,
                    order_type = o.order_type,
                    notes = o.notes,
                    updated_at = NOW()
                FROM original_orders o
                WHERE 
                    bulk_orders.order_id = o.order_id
                    AND o.order_type IN ('新订单', '续订单', '纯衣单','改版续订')
                    AND (
                        o.updated_at > bulk_orders.updated_at
                        OR bulk_orders.updated_at IS NULL
                    )
                """)
            )
            updated_count = update_result.rowcount
            stats["updated"] = updated_count
            
            # 2. 插入新记录
            insert_result = session.execute(
                text("""
                INSERT INTO bulk_orders (
                    order_id, role, handler, process, amount,
                    picture_amount, picture_price, picture_cost, color_cost, work_cost,
                    cloth_price, quantity, cloth_cost, cloth_pack_cost, cloth_code, color_amount,
                    customer_name, phone, shop, express, order_status,
                    order_created_date, order_processed_date, completion_date, order_type,
                    notes, created_at, updated_at
                )
                SELECT 
                    o.order_id, o.role, o.handler, o.process, o.amount,
                    o.picture_amount, o.picture_price, o.picture_cost, o.color_cost, o.work_cost,
                    o.cloth_price, o.quantity, o.cloth_cost, o.cloth_pack_cost, o.cloth_code, o.color_amount,
                    o.customer_name, o.phone, o.shop, o.express, o.order_status,
                    o.order_created_date, o.order_processed_date, o.completion_date, o.order_type,
                    o.notes, o.created_at, o.updated_at
                FROM original_orders o
                WHERE 
                    o.order_type IN ('新订单', '续订单', '纯衣单','改版续订')
                    AND NOT EXISTS (
                        SELECT 1 FROM bulk_orders b WHERE b.order_id = o.order_id
                    )
                """)
            )
            inserted_count = insert_result.rowcount
            stats["inserted"] = inserted_count
            
            # 提交事务
            session.commit()
            print(f"批量订单同步完成: {stats['inserted']}行新增, {stats['updated']}行更新")
            
        except Exception as e:
            session.rollback()
            print(f"同步批量订单时出错: {str(e)}")
            stats["errors"] = 1
            
        # 如果使用的是新创建的会话，关闭它
        if not self.session:
            session.close()
            
        return stats
    
    def sync_all_orders(self) -> Dict[str, Dict[str, int]]:
        """同步所有类型的订单数据"""
        result = {
            "sample_orders": self.sync_sample_orders(),
            "bulk_orders": self.sync_bulk_orders()
        }
        return result
