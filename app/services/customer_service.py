from datetime import datetime, timedelta
import pandas as pd
from sqlmodel import Session, select, text
from app.core.database import get_engine
from app.models.customers import (
    SampleCustomer, BulkCustomer, 
    SampleOrderCustomer, BulkOrderCustomer, CustomerConversion
)
from app.models.sample_orders import SampleOrder
from app.models.bulk_orders import BulkOrder


class CustomerService:
    """客户数据服务，负责样品客户和批量客户的数据管理"""
    
    def __init__(self, session: Session | None = None):
        """初始化客户服务，可选传入现有会话"""
        self.session = session
    
    def _get_session(self) -> Session:
        """获取数据库会话，如果没有现有会话则创建一个新的"""
        if self.session:
            return self.session
        return Session(get_engine())
    
    def extract_customers_from_orders(self) -> dict[str, int]:
        """从订单数据中提取客户信息，并创建对应的样品和批量客户记录
        
        Returns:
            dict[str, int]: 统计信息，如新增客户数、更新客户数等
        """
        print("开始从订单数据中提取客户信息...")
        stats = {
            "new_sample": 0, 
            "updated_sample": 0, 
            "new_bulk": 0, 
            "updated_bulk": 0, 
            "sample_relations": 0, 
            "bulk_relations": 0, 
            "conversions": 0,
            "errors": 0
        }
        
        # 使用多个小事务处理数据
        try:
            # 1. 从样品订单中提取客户信息 
            sample_session = self._get_session()
            try:
                self._extract_from_sample_orders(sample_session, stats)
                sample_session.commit()
                print(f"  - 完成样品客户提取: 新增 {stats['new_sample']} 位, 更新 {stats['updated_sample']} 位")
            except Exception as e:
                sample_session.rollback()
                print(f"  - 样品客户提取出错: {str(e)}")
                stats["errors"] = 1
            finally:
                if not self.session:
                    sample_session.close()
            
            # 2. 从批量订单中提取客户信息
            bulk_session = self._get_session()
            try:
                self._extract_from_bulk_orders(bulk_session, stats)
                bulk_session.commit()
                print(f"  - 完成批量客户提取: 新增 {stats['new_bulk']} 位, 更新 {stats['updated_bulk']} 位")
            except Exception as e:
                bulk_session.rollback()
                print(f"  - 批量客户提取出错: {str(e)}")
                stats["errors"] = 1
            finally:
                if not self.session:
                    bulk_session.close()
            
            # 3. 识别并记录客户转化关系
            conv_session = self._get_session()
            try:
                self._identify_customer_conversions(conv_session, stats)
                conv_session.commit()
                print(f"  - 完成客户转化关系识别: {stats['conversions']} 个转化记录")
            except Exception as e:
                conv_session.rollback()
                print(f"  - 客户转化识别出错: {str(e)}")
                stats["errors"] = 1
            finally:
                if not self.session:
                    conv_session.close()
            
            # 4. 更新客户统计信息
            stats_session = self._get_session()
            try:
                # 拆分成两个小事务
                self._update_sample_customer_stats(stats_session)
                stats_session.commit()
                print("  - 完成样品客户统计更新")
            except Exception as e:
                stats_session.rollback()
                print(f"  - 样品客户统计更新出错: {str(e)}")
                stats["errors"] = 1
            finally:
                if not self.session:
                    stats_session.close()
                    
            bulk_stats_session = self._get_session()
            try:
                self._update_bulk_customer_stats(bulk_stats_session)
                bulk_stats_session.commit()
                print("  - 完成批量客户统计更新")
            except Exception as e:
                bulk_stats_session.rollback()
                print(f"  - 批量客户统计更新出错: {str(e)}")
                stats["errors"] = 1
            finally:
                if not self.session:
                    bulk_stats_session.close()
            
            print(f"客户数据提取完成:")
            print(f"  - 样品客户: 新增 {stats['new_sample']} 位, 更新 {stats['updated_sample']} 位")
            print(f"  - 批量客户: 新增 {stats['new_bulk']} 位, 更新 {stats['updated_bulk']} 位")
            print(f"  - 关联关系: {stats['sample_relations']} 个样品订单关系, {stats['bulk_relations']} 个批量订单关系")
            print(f"  - 转化记录: {stats['conversions']} 个")
            
        except Exception as e:
            print(f"数据提取过程中出现意外错误: {str(e)}")
            stats["errors"] = 1
        
        # 返回与 setup_customer_tables.py 兼容的键名格式
        return {
            "new": stats["new_sample"] + stats["new_bulk"],
            "updated": stats["updated_sample"] + stats["updated_bulk"],
            "sample_relations": stats["sample_relations"],
            "bulk_relations": stats["bulk_relations"],
            "errors": stats["errors"]
        }
    
    def _extract_from_sample_orders(self, session: Session, stats: dict[str, int]) -> None:
        """使用pandas从样品订单中提取客户信息"""
        # 使用pandas读取样品订单数据
        query = select(SampleOrder)
        sample_orders_df = pd.read_sql(query, session.connection())
        
        # 过滤掉缺少关键信息的订单
        valid_orders = sample_orders_df.dropna(subset=['customer_name', 'order_id'])
        
        if valid_orders.empty:
            return
            
        # 提取唯一客户信息
        unique_customers = valid_orders[['customer_name', 'shop', 'handler']].drop_duplicates()
        
        # 处理每个唯一客户
        for _, customer_row in unique_customers.iterrows():
            # 查找或创建样品客户
            customer_id = self._find_or_create_sample_customer(
                session, 
                customer_row['customer_name'], 
                customer_row.get('shop'), 
                customer_row.get('handler'),
                stats
            )
            
            # 获取该客户的所有订单
            customer_orders = valid_orders[valid_orders['customer_name'] == customer_row['customer_name']]
            
            # 为每个订单创建关联
            for _, order_row in customer_orders.iterrows():
                order = SampleOrder(**order_row.to_dict())
                if self._create_sample_order_relation(session, customer_id, order):
                    stats["sample_relations"] += 1
    
    def _extract_from_bulk_orders(self, session: Session, stats: dict[str, int]) -> None:
        """使用pandas从批量订单中提取客户信息"""
        # 使用pandas读取批量订单数据
        query = select(BulkOrder)
        bulk_orders_df = pd.read_sql(query, session.connection())
        
        # 过滤掉缺少关键信息的订单
        valid_orders = bulk_orders_df.dropna(subset=['customer_name', 'order_id'])
        
        if valid_orders.empty:
            return
            
        # 提取唯一客户信息
        unique_customers = valid_orders[['customer_name', 'shop', 'handler']].drop_duplicates()
        
        # 处理每个唯一客户
        for _, customer_row in unique_customers.iterrows():
            # 查找或创建批量客户
            customer_id = self._find_or_create_bulk_customer(
                session, 
                customer_row['customer_name'], 
                customer_row.get('shop'), 
                customer_row.get('handler'),
                stats
            )
            
            # 获取该客户的所有订单
            customer_orders = valid_orders[valid_orders['customer_name'] == customer_row['customer_name']]
            
            # 为每个订单创建关联
            for _, order_row in customer_orders.iterrows():
                order = BulkOrder(**order_row.to_dict())
                if self._create_bulk_order_relation(session, customer_id, order):
                    stats["bulk_relations"] += 1
    
    def _find_or_create_sample_customer(
            self, 
            session: Session, 
            name: str, 
            shop: str | None,
            handler: str | None,
            stats: dict[str, int]
        ) -> int:
        """查找或创建样品客户记录"""
        # 查找现有客户
        query = select(SampleCustomer).where(SampleCustomer.customer_name == name)
        if shop:
            query = query.where(SampleCustomer.shop == shop)
            
        existing = session.exec(query).first()
        
        if existing:
            # 更新现有客户信息
            if shop and not existing.shop:
                existing.shop = shop
            if handler and not existing.handler:
                existing.handler = handler
            session.add(existing)
            stats["updated_sample"] += 1
            return existing.id
        
        # 创建新客户
        new_customer = SampleCustomer(
            customer_name=name,
            shop=shop,
            handler=handler
        )
        session.add(new_customer)
        session.flush()  # 立即获取ID
        stats["new_sample"] += 1
        return new_customer.id
    
    def _find_or_create_bulk_customer(
            self, 
            session: Session, 
            name: str, 
            shop: str | None,
            handler: str | None,
            stats: dict[str, int]
        ) -> int:
        """查找或创建批量客户记录"""
        # 查找现有客户
        query = select(BulkCustomer).where(BulkCustomer.customer_name == name)
        if shop:
            query = query.where(BulkCustomer.shop == shop)
            
        existing = session.exec(query).first()
        
        if existing:
            # 更新现有客户信息
            if shop and not existing.shop:
                existing.shop = shop
            if handler and not existing.handler:
                existing.handler = handler
            session.add(existing)
            stats["updated_bulk"] += 1
            return existing.id
        
        # 创建新客户
        new_customer = BulkCustomer(
            customer_name=name,
            shop=shop,
            handler=handler
        )
        session.add(new_customer)
        session.flush()  # 立即获取ID
        stats["new_bulk"] += 1
        return new_customer.id
    
    def _create_sample_order_relation(
            self, 
            session: Session, 
            sample_customer_id: int, 
            order: SampleOrder
        ) -> bool:
        """创建样品客户-订单关联"""
        # 检查关联是否已存在
        existing = session.exec(
            select(SampleOrderCustomer).where(
                SampleOrderCustomer.sample_customer_id == sample_customer_id,
                SampleOrderCustomer.order_id == order.order_id
            )
        ).first()
        
        if existing:
            return False  # 关联已存在
        
        # 创建新关联
        relation = SampleOrderCustomer(
            sample_customer_id=sample_customer_id,
            order_id=order.order_id,
            order_date=getattr(order, 'order_date', None) or getattr(order, 'created_at', None),
            amount=getattr(order, 'amount', None) or getattr(order, 'cost', None)
        )
        session.add(relation)
        return True
    
    def _create_bulk_order_relation(
            self, 
            session: Session, 
            bulk_customer_id: int, 
            order: BulkOrder
        ) -> bool:
        """创建批量客户-订单关联"""
        # 检查关联是否已存在
        existing = session.exec(
            select(BulkOrderCustomer).where(
                BulkOrderCustomer.bulk_customer_id == bulk_customer_id,
                BulkOrderCustomer.order_id == order.order_id
            )
        ).first()
        
        if existing:
            return False  # 关联已存在
        
        # 创建新关联
        relation = BulkOrderCustomer(
            bulk_customer_id=bulk_customer_id,
            order_id=order.order_id,
            order_date=getattr(order, 'order_date', None) or getattr(order, 'created_at', None),
            amount=getattr(order, 'amount', None) or getattr(order, 'cost', None)
        )
        session.add(relation)
        return True
    
    def _identify_customer_conversions(self, session: Session, stats: dict[str, int]) -> None:
        """识别样品客户转化为批量客户的情况，并创建转化记录"""
        # 获取所有样品客户
        sample_customers_df = pd.read_sql(select(SampleCustomer), session.connection())
        
        # 获取所有批量客户
        bulk_customers_df = pd.read_sql(select(BulkCustomer), session.connection())
        
        # 如果任一数据集为空，无需继续
        if sample_customers_df.empty or bulk_customers_df.empty:
            return
        
        # 识别名称匹配的客户
        for _, sample_row in sample_customers_df.iterrows():
            matching_bulk = bulk_customers_df[bulk_customers_df['customer_name'] == sample_row['customer_name']]
            
            if not matching_bulk.empty:
                # 对于每个匹配的批量客户，创建一个转化记录
                for _, bulk_row in matching_bulk.iterrows():
                    # 检查是否已存在转化记录
                    existing = session.exec(
                        select(CustomerConversion).where(
                            CustomerConversion.sample_customer_id == sample_row['id'],
                            CustomerConversion.bulk_customer_id == bulk_row['id']
                        )
                    ).first()
                    
                    if not existing:
                        # 创建新转化记录
                        conversion = CustomerConversion(
                            sample_customer_id=sample_row['id'],
                            bulk_customer_id=bulk_row['id'],
                            conversion_date=datetime.now()
                        )
                        session.add(conversion)
                        
                        # 更新样品客户的转化状态
                        sample_customer = session.get(SampleCustomer, sample_row['id'])
                        if sample_customer:
                            sample_customer.converted_to_bulk = True
                            sample_customer.conversion_date = conversion.conversion_date
                            sample_customer.bulk_customer_id = bulk_row['id']
                            session.add(sample_customer)
                        
                        # 更新批量客户的来源信息
                        bulk_customer = session.get(BulkCustomer, bulk_row['id'])
                        if bulk_customer:
                            bulk_customer.converted_from_sample = True
                            bulk_customer.sample_customer_id = sample_row['id']
                            session.add(bulk_customer)
                        
                        stats["conversions"] += 1
    
    def _update_sample_customer_stats(self, session: Session) -> None:
        """使用pandas更新样品客户统计信息"""
        # 获取所有样品客户
        sample_customers_df = pd.read_sql(select(SampleCustomer), session.connection())
        
        if sample_customers_df.empty:
            return
        
        # 获取所有样品订单关联
        sample_relations_df = pd.read_sql(
            select(
                SampleOrderCustomer.sample_customer_id,
                SampleOrderCustomer.order_date,
                SampleOrderCustomer.amount
            ),
            session.connection()
        )
        
        if sample_relations_df.empty:
            return
        
        # 对每个客户执行统计
        for customer_id in sample_customers_df['id'].unique():
            # 转换NumPy int64类型为Python原生int类型
            customer_id_int = int(customer_id)
            
            # 获取该客户的所有订单关系
            customer_orders = sample_relations_df[sample_relations_df['sample_customer_id'] == customer_id]
            
            if customer_orders.empty:
                continue
            
            # 计算统计信息
            orders_count = len(customer_orders)
            total_amount = customer_orders['amount'].sum()
            first_date = customer_orders['order_date'].min()
            last_date = customer_orders['order_date'].max()
            
            # 更新客户记录
            customer = session.get(SampleCustomer, customer_id_int)
            if customer:
                customer.sample_orders_count = orders_count
                customer.total_sample_amount = float(total_amount)
                customer.first_sample_date = first_date
                customer.last_sample_date = last_date
                session.add(customer)
    
    def _update_bulk_customer_stats(self, session: Session) -> None:
        """使用pandas更新批量客户统计信息"""
        # 获取所有批量客户
        bulk_customers_df = pd.read_sql(select(BulkCustomer), session.connection())
        
        if bulk_customers_df.empty:
            return
        
        # 获取所有批量订单关联
        bulk_relations_df = pd.read_sql(
            select(
                BulkOrderCustomer.bulk_customer_id,
                BulkOrderCustomer.order_date,
                BulkOrderCustomer.amount
            ),
            session.connection()
        )
        
        if bulk_relations_df.empty:
            return
        
        # 对每个客户执行统计
        for customer_id in bulk_customers_df['id'].unique():
            # 转换NumPy int64类型为Python原生int类型
            customer_id_int = int(customer_id)
            
            # 获取该客户的所有订单关系
            customer_orders = bulk_relations_df[bulk_relations_df['bulk_customer_id'] == customer_id]
            
            if customer_orders.empty:
                continue
            
            # 计算统计信息
            orders_count = len(customer_orders)
            total_amount = customer_orders['amount'].sum()
            first_date = customer_orders['order_date'].min()
            last_date = customer_orders['order_date'].max()
            
            # 更新客户记录
            customer = session.get(BulkCustomer, customer_id_int)
            if customer:
                customer.bulk_orders_count = orders_count
                customer.total_bulk_amount = float(total_amount)
                customer.first_bulk_date = first_date
                customer.last_bulk_date = last_date
                session.add(customer)
            
        finally:
            if not self.session:
                session.close()

    def get_sample_customers_by_handler(self, handler: str | None = None) -> list[dict]:
        """获取指定处理人的样品客户"""
        session = self._get_session()
        
        try:
            query = select(SampleCustomer)
            if handler:
                query = query.where(SampleCustomer.handler == handler)
            
            customers_df = pd.read_sql(query, session.connection())
            
            if customers_df.empty:
                return []
            
            # 转换为字典列表
            return customers_df.to_dict(orient='records')
        
        finally:
            if not self.session:
                session.close()
    
    def get_bulk_customers_by_handler(self, handler: str | None = None) -> list[dict]:
        """获取指定处理人的批量客户"""
        session = self._get_session()
        
        try:
            query = select(BulkCustomer)
            if handler:
                query = query.where(BulkCustomer.handler == handler)
            
            customers_df = pd.read_sql(query, session.connection())
            
            if customers_df.empty:
                return []
            
            # 转换为字典列表
            return customers_df.to_dict(orient='records')
        
        finally:
            if not self.session:
                session.close()
    
    def update_sample_customer(self, customer_id: int, data: dict) -> bool:
        """更新样品客户信息"""
        session = self._get_session()
        
        try:
            customer = session.get(SampleCustomer, customer_id)
            if not customer:
                return False
            
            # 更新允许的字段
            for field in ['customer_name', 'shop', 'handler', 'region', 'notes', 
                          'wechat', 'next_contact_date', 'lost_reason', 'tags']:
                if field in data:
                    setattr(customer, field, data[field])
            
            session.add(customer)
            session.commit()
            return True
        
        except Exception as e:
            session.rollback()
            print(f"更新样品客户信息时出错: {str(e)}")
            return False
        
        finally:
            if not self.session:
                session.close()
    
    def update_bulk_customer(self, customer_id: int, data: dict) -> bool:
        """更新批量客户信息"""
        session = self._get_session()
        
        try:
            customer = session.get(BulkCustomer, customer_id)
            if not customer:
                return False
            
            # 更新允许的字段
            for field in ['customer_name', 'shop', 'handler', 'region', 'notes', 'tags']:
                if field in data:
                    setattr(customer, field, data[field])
            
            session.add(customer)
            session.commit()
            return True
        
        except Exception as e:
            session.rollback()
            print(f"更新批量客户信息时出错: {str(e)}")
            return False
        
        finally:
            if not self.session:
                session.close()
