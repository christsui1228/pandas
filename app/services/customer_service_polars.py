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
        
        session = self._get_session()
        
        try:
            # 1. 从样品订单中提取客户信息
            self._extract_from_sample_orders(session, stats)
            
            # 2. 从批量订单中提取客户信息
            self._extract_from_bulk_orders(session, stats)
            
            # 3. 识别并记录客户转化关系
            self._identify_customer_conversions(session, stats)
            
            # 4. 更新客户统计信息
            self._update_sample_customer_stats(session)
            self._update_bulk_customer_stats(session)
            
            # 提交事务
            session.commit()
            print(f"客户数据提取完成:")
            print(f"  - 样品客户: 新增 {stats['new_sample']} 位, 更新 {stats['updated_sample']} 位")
            print(f"  - 批量客户: 新增 {stats['new_bulk']} 位, 更新 {stats['updated_bulk']} 位")
            print(f"  - 关联关系: {stats['sample_relations']} 个样品订单关系, {stats['bulk_relations']} 个批量订单关系")
            print(f"  - 转化记录: {stats['conversions']} 个")
            
        except Exception as e:
            session.rollback()
            print(f"提取客户数据时出错: {str(e)}")
            stats["errors"] = 1
        
        # 如果使用的是新创建的会话，关闭它
        if not self.session:
            session.close()
            
        return stats
    
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
            customer_orders = valid_orders[
                (valid_orders['customer_name'] == customer_row['customer_name']) &
                (valid_orders['shop'] == customer_row.get('shop')) &
                (valid_orders['handler'] == customer_row.get('handler'))
            ]
            
            # 为每个订单创建客户-订单关联
            for _, order_row in customer_orders.iterrows():
                order_id = order_row['order_id']
                # 通过order_id获取实际的SampleOrder对象
                order = session.get(SampleOrder, order_id)
                if order:
                    self._create_sample_order_relation(session, customer_id, order)
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
            customer_orders = valid_orders[
                (valid_orders['customer_name'] == customer_row['customer_name']) &
                (valid_orders['shop'] == customer_row.get('shop')) &
                (valid_orders['handler'] == customer_row.get('handler'))
            ]
            
            # 为每个订单创建客户-订单关联
            for _, order_row in customer_orders.iterrows():
                order_id = order_row['order_id']
                order = session.get(BulkOrder, order_id)
                if order:
                    self._create_bulk_order_relation(session, customer_id, order)
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
        query = select(SampleCustomer).where(SampleCustomer.customer_name == name)
        if shop:
            query = query.where(SampleCustomer.shop == shop)
        if handler:
            query = query.where(SampleCustomer.handler == handler)
        
        existing_customer = session.exec(query).first()
        
        if existing_customer:
            # 如果需要更新信息，在这里添加逻辑
            # 例如，更新最后联系时间等
            # existing_customer.last_contact_date = datetime.now()
            # session.add(existing_customer)
            # stats["updated_sample"] += 1
            return existing_customer.id
        else:
            new_customer = SampleCustomer(
                customer_name=name,
                shop=shop,
                handler=handler,
                # 可以设置默认值
                # first_contact_date = datetime.now()
            )
            session.add(new_customer)
            session.flush() # 获取新客户的ID
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
        query = select(BulkCustomer).where(BulkCustomer.customer_name == name)
        if shop:
            query = query.where(BulkCustomer.shop == shop)
        if handler:
            query = query.where(BulkCustomer.handler == handler)
            
        existing_customer = session.exec(query).first()
        
        if existing_customer:
            # 更新信息逻辑
            # existing_customer.last_contact_date = datetime.now()
            # session.add(existing_customer)
            # stats["updated_bulk"] += 1
            return existing_customer.id
        else:
            new_customer = BulkCustomer(
                customer_name=name,
                shop=shop,
                handler=handler
            )
            session.add(new_customer)
            session.flush()
            stats["new_bulk"] += 1
            return new_customer.id

    def _create_sample_order_relation(
            self, 
            session: Session, 
            sample_customer_id: int, 
            order: SampleOrder
        ) -> None:
        """创建样品客户-订单关联"""
        # 检查关联是否已存在
        existing_relation = session.exec(
            select(SampleOrderCustomer)
            .where(SampleOrderCustomer.sample_customer_id == sample_customer_id)
            .where(SampleOrderCustomer.sample_order_id == order.order_id)
        ).first()
        
        if not existing_relation:
            relation = SampleOrderCustomer(
                sample_customer_id=sample_customer_id,
                sample_order_id=order.order_id,
                order_date=order.order_date, # 从订单获取日期
                # 可能需要其他关联信息
            )
            session.add(relation)

    def _create_bulk_order_relation(
            self, 
            session: Session, 
            bulk_customer_id: int, 
            order: BulkOrder
        ) -> None:
        """创建批量客户-订单关联"""
        existing_relation = session.exec(
            select(BulkOrderCustomer)
            .where(BulkOrderCustomer.bulk_customer_id == bulk_customer_id)
            .where(BulkOrderCustomer.bulk_order_id == order.order_id)
        ).first()
        
        if not existing_relation:
            relation = BulkOrderCustomer(
                bulk_customer_id=bulk_customer_id,
                bulk_order_id=order.order_id,
                order_date=order.order_date
            )
            session.add(relation)

    def _identify_customer_conversions(self, session: Session, stats: dict[str, int]) -> None:
        """识别样品客户转化为批量客户的情况，并创建转化记录"""
        # 使用pandas读取必要的客户和订单关联数据
        sample_customers_df = pd.read_sql(select(SampleCustomer.id, SampleCustomer.customer_name, SampleCustomer.shop), session.connection())
        bulk_customers_df = pd.read_sql(select(BulkCustomer.id, BulkCustomer.customer_name, BulkCustomer.shop), session.connection())
        bulk_relations_df = pd.read_sql(select(BulkOrderCustomer.bulk_customer_id, BulkOrderCustomer.order_date), session.connection())
        
        if sample_customers_df.empty or bulk_customers_df.empty or bulk_relations_df.empty:
            return
        
        # 合并批量客户和他们的首次批量订单日期
        first_bulk_order_dates = bulk_relations_df.groupby('bulk_customer_id')['order_date'].min().reset_index()
        bulk_customers_with_first_order = pd.merge(
            bulk_customers_df, 
            first_bulk_order_dates, 
            left_on='id', 
            right_on='bulk_customer_id'
        )
        
        # 合并样品客户和批量客户（基于名称和店铺）
        merged_customers = pd.merge(
            sample_customers_df, 
            bulk_customers_with_first_order, 
            on=['customer_name', 'shop'], 
            suffixes=('_sample', '_bulk')
        )
        
        # 处理每个潜在的转化
        for _, row in merged_customers.iterrows():
            sample_customer_id = row['id_sample']
            bulk_customer_id = row['id_bulk']
            conversion_date = row['order_date'] # 首次批量订单日期作为转化日期
            
            # 检查转化记录是否已存在
            existing_conversion = session.exec(
                select(CustomerConversion)
                .where(CustomerConversion.sample_customer_id == sample_customer_id)
                .where(CustomerConversion.bulk_customer_id == bulk_customer_id)
            ).first()
            
            if not existing_conversion:
                conversion = CustomerConversion(
                    sample_customer_id=sample_customer_id,
                    bulk_customer_id=bulk_customer_id,
                    conversion_date=conversion_date,
                    created_at=datetime.now()
                )
                session.add(conversion)
                stats["conversions"] += 1

    def _update_sample_customer_stats(self, session: Session) -> None:
        """使用pandas更新样品客户统计信息"""
        # 关联样品客户和他们的订单
        query = text("""
            SELECT 
                sc.id AS customer_id,
                MIN(so.order_date) AS first_order_date,
                MAX(so.order_date) AS last_order_date,
                COUNT(so.order_id) AS orders_count,
                SUM(so.amount) AS total_amount
            FROM sample_customer sc
            JOIN sample_order_customer soc ON sc.id = soc.sample_customer_id
            JOIN sample_order so ON soc.sample_order_id = so.order_id
            GROUP BY sc.id
        """)
        
        stats_df = pd.read_sql(query, session.connection())
        
        if stats_df.empty:
            return
        
        # 批量更新客户信息
        for _, row in stats_df.iterrows():
            customer = session.get(SampleCustomer, int(row['customer_id']))
            if customer:
                customer.first_sample_date = row['first_order_date']
                customer.last_sample_date = row['last_order_date']
                customer.sample_orders_count = int(row['orders_count'])
                customer.sample_total_amount = float(row['total_amount']) if pd.notna(row['total_amount']) else 0.0
                session.add(customer)

    def _update_bulk_customer_stats(self, session: Session) -> None:
        """使用pandas更新批量客户统计信息"""
        # 关联批量客户和他们的订单
        query = text("""
            SELECT 
                bc.id AS customer_id,
                MIN(bo.order_date) AS first_order_date,
                MAX(bo.order_date) AS last_order_date,
                COUNT(bo.order_id) AS orders_count,
                SUM(bo.amount) AS total_amount
            FROM bulk_customer bc
            JOIN bulk_order_customer boc ON bc.id = boc.bulk_customer_id
            JOIN bulk_order bo ON boc.bulk_order_id = bo.order_id
            GROUP BY bc.id
        """)
        
        stats_df = pd.read_sql(query, session.connection())
        
        if stats_df.empty:
            return
        
        # 批量更新客户信息
        for _, row in stats_df.iterrows():
            customer = session.get(BulkCustomer, int(row['customer_id']))
            if customer:
                customer.first_bulk_date = row['first_order_date']
                customer.last_bulk_date = row['last_order_date']
                customer.bulk_orders_count = int(row['orders_count'])
                customer.bulk_total_amount = float(row['total_amount']) if pd.notna(row['total_amount']) else 0.0
                session.add(customer)

    def get_customer_summary(self) -> dict:
        """使用pandas获取客户数据摘要"""
        session = self._get_session()
        try:
            # 使用pandas直接从数据库读取数据进行统计
            sample_customers_df = pd.read_sql(select(SampleCustomer.id), session.connection())
            bulk_customers_df = pd.read_sql(select(BulkCustomer.id), session.connection())
            conversions_df = pd.read_sql(select(CustomerConversion.sample_customer_id), session.connection())
            
            sample_count = len(sample_customers_df)
            bulk_count = len(bulk_customers_df)
            # 去重统计已转化的样品客户数
            converted_count = conversions_df['sample_customer_id'].nunique()
            
            total_customers = sample_count + bulk_count # 注意：这里可能包含重叠，如果一个人既是样品又是批量客户
            # 一个更准确的总数可能需要合并客户列表并去重，但这取决于业务逻辑
            # 例如：combined_df = pd.concat([sample_customers_df[['customer_name', 'shop']], bulk_customers_df[['customer_name', 'shop']]]).drop_duplicates()
            # total_customers = len(combined_df)
            
            conversion_rate = (converted_count / sample_count * 100) if sample_count > 0 else 0
            
            return {
                "total_customers": total_customers, # 或使用去重后的总数
                "sample_customers": sample_count,
                "bulk_customers": bulk_count,
                "converted_customers": converted_count,
                "conversion_rate": round(conversion_rate, 2)
            }
        except Exception as e:
            print(f"获取客户摘要时出错: {str(e)}")
            return {
                "total_customers": 0,
                "sample_customers": 0,
                "bulk_customers": 0,
                "converted_customers": 0,
                "conversion_rate": 0.0
            }
        finally:
            if not self.session:
                session.close()

    def find_unconverted_customers(self, limit: int = 50) -> list[dict]:
        """使用pandas查找未转化的样品客户"""
        session = self._get_session()
        try:
            # 获取所有样品客户及其统计信息
            sample_query = select(
                SampleCustomer.id, 
                SampleCustomer.customer_name, 
                SampleCustomer.shop,
                SampleCustomer.handler,
                SampleCustomer.first_sample_date,
                SampleCustomer.last_sample_date,
                SampleCustomer.sample_orders_count
            ).order_by(SampleCustomer.last_sample_date.desc().nullslast())
            
            sample_customers_df = pd.read_sql(sample_query, session.connection())
            
            # 获取所有转化记录中的样品客户ID
            conversion_query = select(CustomerConversion.sample_customer_id).distinct()
            converted_ids_df = pd.read_sql(conversion_query, session.connection())
            converted_ids = set(converted_ids_df['sample_customer_id'].tolist())
            
            # 过滤掉已转化的客户
            unconverted_df = sample_customers_df[
                ~sample_customers_df['id'].isin(converted_ids)
            ]
            
            # 获取这些未转化客户的样品订单详情 (可选，如果需要展示订单信息)
            if not unconverted_df.empty:
                unconverted_ids_list = unconverted_df['id'].tolist()
                order_details_query = text("""
                    SELECT soc.sample_customer_id, so.order_id, so.order_date, so.amount
                    FROM sample_order_customer soc
                    JOIN sample_order so ON soc.sample_order_id = so.order_id
                    WHERE soc.sample_customer_id IN :customer_ids
                    ORDER BY soc.sample_customer_id, so.order_date DESC
                """)
                order_details_df = pd.read_sql(
                    order_details_query, 
                    session.connection(), 
                    params={'customer_ids': tuple(unconverted_ids_list)}
                )
            else:
                order_details_df = pd.DataFrame(columns=['sample_customer_id', 'order_id', 'order_date', 'amount'])
            
            # 组合结果
            result = []
            for _, customer_row in unconverted_df.head(limit).iterrows():
                customer_id = customer_row['id']
                customer_orders = order_details_df[order_details_df['sample_customer_id'] == customer_id]
                
                result.append({
                    "id": customer_id,
                    "customer_name": customer_row['customer_name'],
                    "shop": customer_row['shop'],
                    "handler": customer_row['handler'],
                    "first_sample_date": customer_row['first_sample_date'],
                    "last_sample_date": customer_row['last_sample_date'],
                    "sample_orders_count": int(customer_row['sample_orders_count']),
                    "sample_orders": [
                        {
                            "order_id": row['order_id'],
                            "date": row['order_date'],
                            "amount": float(row['amount']) if pd.notna(row['amount']) else None
                        }
                        for _, row in customer_orders.iterrows()
                    ]
                })
            
            return result
            
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
