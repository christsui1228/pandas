from datetime import datetime, timedelta
import polars as pl
from sqlmodel import Session, select, text, func
# from app.core.database import get_engine # No longer needed if session is always injected
from app.customers.models import (
    SampleCustomer, BulkCustomer, 
    SampleOrderCustomer, BulkOrderCustomer, CustomerConversion
)
from app.sample_orders.models import SampleOrder
from app.models.bulk_orders import BulkOrder
from app.core.database import get_engine # For Polars connection URI if needed
import logging # Add this import


class CustomerService:
    """客户数据服务，负责样品客户和批量客户的数据管理"""
    
    def __init__(self, db: Session): # Changed from optional session to mandatory db
        """初始化客户服务，必须传入一个数据库会话。"""
        if db is None:
            raise ValueError("A database session (db: Session) must be provided to CustomerService.")
        self.db = db # Store the injected session
        # For Polars, it might be more efficient to get a connection string/engine once
        # However, SQLModel session is primarily for ORM operations. Polars will use its own connection.
        # We can use the engine from the session for Polars if possible, or construct a URI from settings.
        self.engine = self.db.bind # Assuming self.db is a SQLModel session bound to an engine
                                  # Or use get_engine() if self.db.bind is not suitable for polars
        if not self.engine:
            self.engine = get_engine() # Fallback to direct engine
    
    # def _get_session(self) -> Session: # Removed this method
    #     """获取数据库会话，如果没有现有会话则创建一个新的"""
    #     if self.session: # self.session is now self.db
    #         return self.session
    #     return Session(get_engine())
    
    def _get_db_connection_uri(self) -> str:
        # Helper to construct a URI that Polars/ConnectorX can use
        # This depends on your actual DB settings structure
        from app.core.config import settings
        # Example for PostgreSQL, adjust as needed
        return f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

    def extract_customers_from_orders(self) -> dict[str, int]:
        """从订单数据中提取客户信息，并创建对应的样品和批量客户记录
        
        Returns:
            dict[str, int]: 统计信息，如新增客户数、更新客户数等
        """
        logging.info("开始从订单数据中提取客户信息 (使用 Polars)...")
        stats = {
            "new_sample": 0, "updated_sample": 0, "new_bulk": 0, "updated_bulk": 0,
            "sample_relations": 0, "bulk_relations": 0, "conversions": 0, "errors": 0
        }
        
        # 使用注入的 self.db 会话进行所有操作，并在最后统一处理事务
        try:
            # 1. 从样品订单中提取客户信息 
            # sample_session = self._get_session() # Use self.db directly
            self._extract_from_sample_orders_polars(self.db, stats)
            # sample_session.commit() # Commit will be handled at the end of this method
            logging.info(f"  - 完成样品客户提取 (Polars): 新增 {stats['new_sample']} 位, 更新 {stats['updated_sample']} 位")
            
            # 2. 从批量订单中提取客户信息
            # bulk_session = self._get_session() # Use self.db directly
            self._extract_from_bulk_orders_polars(self.db, stats)
            # bulk_session.commit()
            logging.info(f"  - 完成批量客户提取 (Polars): 新增 {stats['new_bulk']} 位, 更新 {stats['updated_bulk']} 位")
            
            # 3. 识别并记录客户转化关系
            # conv_session = self._get_session() # Use self.db directly
            self._identify_customer_conversions_polars(self.db, stats)
            # conv_session.commit()
            logging.info(f"  - 完成客户转化关系识别 (Polars): {stats['conversions']} 个转化记录")
            
            # 4. 更新客户统计信息
            # stats_session = self._get_session() # Use self.db directly
            self._update_sample_customer_stats_polars(self.db)
            # stats_session.commit()
            logging.info("  - 完成样品客户统计更新 (Polars)")

            # bulk_stats_session = self._get_session() # Use self.db directly
            self._update_bulk_customer_stats_polars(self.db)
            # bulk_stats_session.commit()
            logging.info("  - 完成批量客户统计更新 (Polars)")
            
            self.db.commit() # Commit all changes if all steps were successful
            logging.info(f"客户数据提取完成 (Polars):")
            logging.info(f"  - 样品客户: 新增 {stats['new_sample']} 位, 更新 {stats['updated_sample']} 位")
            logging.info(f"  - 批量客户: 新增 {stats['new_bulk']} 位, 更新 {stats['updated_bulk']} 位")
            logging.info(f"  - 关联关系: {stats['sample_relations']} 个样品订单关系, {stats['bulk_relations']} 个批量订单关系")
            logging.info(f"  - 转化记录: {stats['conversions']} 个")
            
        except Exception as e:
            self.db.rollback() # Rollback if any step failed
            logging.error(f"数据提取过程中出现意外错误 (Polars): {str(e)}", exc_info=True)
            stats["errors"] = 1
        # finally block for closing externally managed sessions is not needed here
        # as self.db is managed by the caller.
        
        # 返回与 setup_customer_tables.py 兼容的键名格式
        return {
            "new": stats["new_sample"] + stats["new_bulk"],
            "updated": stats["updated_sample"] + stats["updated_bulk"],
            "sample_relations": stats["sample_relations"],
            "bulk_relations": stats["bulk_relations"],
            "errors": stats["errors"]
        }
    
    def _extract_from_sample_orders_polars(self, session: Session, stats: dict[str, int]) -> None:
        """使用polars从样品订单中提取客户信息"""
        conn_uri = self._get_db_connection_uri()
        sample_orders_query = "SELECT id, order_id, customer_name, shop, handler FROM sample_orders" # Select only needed columns
        try:
            # Use read_database_uri when providing a URI string
            sample_orders_pl_df = pl.read_database_uri(query=sample_orders_query, uri=conn_uri)
        except Exception as e:
            logging.error(f"Polars 读取 sample_orders 失败: {e}", exc_info=True)
            return

        valid_orders_pl_df = sample_orders_pl_df.drop_nulls(subset=['customer_name', 'order_id'])
        if valid_orders_pl_df.is_empty():
            return
            
        unique_customers_pl_df = valid_orders_pl_df.select(['customer_name', 'shop', 'handler']).unique(maintain_order=True)
        
        for customer_row in unique_customers_pl_df.iter_rows(named=True):
            customer_id = self._find_or_create_sample_customer(
                session, 
                customer_row['customer_name'], 
                customer_row.get('shop'), 
                customer_row.get('handler'),
                stats
            )
            
            # Filter orders for the current customer directly from Polars DF
            customer_orders_pl_df_for_customer = valid_orders_pl_df.filter(pl.col('customer_name') == customer_row['customer_name'])
            
            for order_row_data in customer_orders_pl_df_for_customer.iter_rows(named=True):
                retrieved_order = session.get(SampleOrder, order_row_data['id']) # Get by primary key 'id'
                if retrieved_order:
                    if self._create_sample_order_relation(session, customer_id, retrieved_order):
                        stats["sample_relations"] += 1
                else:
                    # Log a warning if an order from DataFrame isn't found by ID (should not happen)
                    logging.warning(f"警告 (Polars): 未能在数据库中通过ID {order_row_data['id']} 找到样品订单 {order_row_data['order_id']}")
    
    def _extract_from_bulk_orders_polars(self, session: Session, stats: dict[str, int]) -> None:
        """使用polars从批量订单中提取客户信息"""
        conn_uri = self._get_db_connection_uri()
        bulk_orders_query = "SELECT id, order_id, customer_name, shop, handler FROM bulk_orders" # Select only needed columns
        try:
            # Use read_database_uri when providing a URI string
            bulk_orders_pl_df = pl.read_database_uri(query=bulk_orders_query, uri=conn_uri)
        except Exception as e:
            logging.error(f"Polars 读取 bulk_orders 失败: {e}", exc_info=True)
            return

        valid_orders_pl_df = bulk_orders_pl_df.drop_nulls(subset=['customer_name', 'order_id'])
        if valid_orders_pl_df.is_empty():
            return
        unique_customers_pl_df = valid_orders_pl_df.select(['customer_name', 'shop', 'handler']).unique(maintain_order=True)
        for customer_row in unique_customers_pl_df.iter_rows(named=True):
            customer_id = self._find_or_create_bulk_customer(
                session, 
                customer_row['customer_name'], 
                customer_row.get('shop'), 
                customer_row.get('handler'),
                stats
            )
            customer_orders_pl_df_for_customer = valid_orders_pl_df.filter(pl.col('customer_name') == customer_row['customer_name'])
            for order_row_data in customer_orders_pl_df_for_customer.iter_rows(named=True):
                retrieved_order = session.get(BulkOrder, order_row_data['id']) # Get by primary key 'id'
                if retrieved_order:
                    if self._create_bulk_order_relation(session, customer_id, retrieved_order):
                        stats["bulk_relations"] += 1
                else:
                    logging.warning(f"警告 (Polars): 未能在数据库中通过ID {order_row_data['id']} 找到批量订单 {order_row_data['order_id']}")
    
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
            order: SampleOrder # Expecting an ORM object
        ) -> bool:
        """创建样品客户-订单关联，并填充日期和金额"""
        existing_relation = session.exec(
            select(SampleOrderCustomer).where(
                SampleOrderCustomer.sample_customer_id == sample_customer_id,
                SampleOrderCustomer.order_id == order.order_id # Linking by business order_id
            )
        ).first()
        
        if existing_relation:
            return False
        
        relation = SampleOrderCustomer(
            sample_customer_id=sample_customer_id,
            order_id=order.order_id, # Business Order ID from SampleOrder
            order_date=getattr(order, 'order_created_date', None) or getattr(order, 'created_at', None),
            amount=getattr(order, 'amount', None)
        )
        session.add(relation)
        return True
    
    def _create_bulk_order_relation(
            self, 
            session: Session, 
            bulk_customer_id: int, 
            order: BulkOrder # Expecting an ORM object
        ) -> bool:
        """创建批量客户-订单关联，并填充日期和金额"""
        existing_relation = session.exec(
            select(BulkOrderCustomer).where(
                BulkOrderCustomer.bulk_customer_id == bulk_customer_id,
                BulkOrderCustomer.order_id == order.order_id # Linking by business order_id
            )
        ).first()
        
        if existing_relation:
            return False
        
        relation = BulkOrderCustomer(
            bulk_customer_id=bulk_customer_id,
            order_id=order.order_id, # Business Order ID from BulkOrder
            order_date=getattr(order, 'order_created_date', None) or getattr(order, 'created_at', None),
            amount=getattr(order, 'amount', None)
        )
        session.add(relation)
        return True
    
    def _identify_customer_conversions_polars(self, session: Session, stats: dict[str, int]) -> None:
        """识别样品客户转化为批量客户的情况，并创建转化记录"""
        conn_uri = self._get_db_connection_uri()
        sample_customers_query = "SELECT id, customer_name FROM sample_customers"
        bulk_customers_query = "SELECT id, customer_name FROM bulk_customers"
        try:
            # Use read_database_uri when providing a URI string
            sample_customers_pl_df = pl.read_database_uri(query=sample_customers_query, uri=conn_uri)
            bulk_customers_pl_df = pl.read_database_uri(query=bulk_customers_query, uri=conn_uri)
        except Exception as e:
            logging.error(f"Polars 读取客户表失败 (conversions): {e}", exc_info=True)
            return

        if sample_customers_pl_df.is_empty() or bulk_customers_pl_df.is_empty():
            return

        # Polars join for finding matches
        # Rename columns for clarity before join if needed, e.g., bulk_customers_pl_df = bulk_customers_pl_df.rename(...)
        joined_df = sample_customers_pl_df.join(
            bulk_customers_pl_df, on="customer_name", how="inner", suffix="_bulk"
        )
        
        for row in joined_df.iter_rows(named=True):
            sample_id = row['id']
            bulk_id = row['id_bulk'] # Joined column
            
            existing = session.exec(
                select(CustomerConversion).where(
                    CustomerConversion.sample_customer_id == sample_id,
                    CustomerConversion.bulk_customer_id == bulk_id
                )
            ).first()
            
            if not existing:
                conversion = CustomerConversion(
                    sample_customer_id=sample_id, bulk_customer_id=bulk_id, conversion_date=datetime.now(timezone.utc) # Use timezone
                )
                session.add(conversion)
                sample_customer = session.get(SampleCustomer, sample_id)
                if sample_customer:
                    sample_customer.converted_to_bulk = True
                    sample_customer.conversion_date = conversion.conversion_date
                    sample_customer.bulk_customer_id = bulk_id
                    session.add(sample_customer)
                bulk_customer = session.get(BulkCustomer, bulk_id)
                if bulk_customer:
                    bulk_customer.converted_from_sample = True
                    bulk_customer.sample_customer_id = sample_id
                    session.add(bulk_customer)
                stats["conversions"] += 1
    
    def _update_sample_customer_stats_polars(self, session: Session) -> None:
        """使用polars更新样品客户统计信息"""
        conn_uri = self._get_db_connection_uri()
        # Select only necessary columns for stats
        relations_query = "SELECT sample_customer_id, order_date, amount FROM sample_order_customers WHERE order_date IS NOT NULL AND amount IS NOT NULL"
        try:
            # Use read_database_uri when providing a URI string
            sample_relations_pl_df = pl.read_database_uri(query=relations_query, uri=conn_uri)
        except Exception as e:
            logging.error(f"Polars 读取 sample_order_customers 失败: {e}", exc_info=True)
            return

        if sample_relations_pl_df.is_empty():
            return
        
        # Ensure order_date is datetime for min/max operations
        sample_relations_pl_df = sample_relations_pl_df.with_columns(
            pl.col("order_date").str.to_datetime(strict=False) # Assuming order_date might be string from DB read
        )

        # Group by customer_id and aggregate using Polars expressions
        customer_stats_pl_df = sample_relations_pl_df.group_by("sample_customer_id").agg([
            pl.count().alias("orders_count"),
            pl.col("amount").sum().alias("total_amount"),
            pl.col("order_date").min().alias("first_date"),
            pl.col("order_date").max().alias("last_date"),
        ])

        for row in customer_stats_pl_df.iter_rows(named=True):
            customer_id_int = int(row['sample_customer_id'])
            customer = session.get(SampleCustomer, customer_id_int)
            if customer:
                customer.sample_orders_count = row['orders_count']
                customer.total_sample_amount = float(row['total_amount'] if row['total_amount'] is not None else 0.0)
                customer.first_sample_date = row['first_date']
                customer.last_sample_date = row['last_date']
                session.add(customer)
    
    def _update_bulk_customer_stats_polars(self, session: Session) -> None:
        """使用polars更新批量客户统计信息"""
        conn_uri = self._get_db_connection_uri()
        relations_query = "SELECT bulk_customer_id, order_date, amount FROM bulk_order_customers WHERE order_date IS NOT NULL AND amount IS NOT NULL"
        try:
            # Use read_database_uri when providing a URI string
            bulk_relations_pl_df = pl.read_database_uri(query=relations_query, uri=conn_uri)
        except Exception as e:
            logging.error(f"Polars 读取 bulk_order_customers 失败: {e}", exc_info=True)
            return
        
        if bulk_relations_pl_df.is_empty():
            return

        bulk_relations_pl_df = bulk_relations_pl_df.with_columns(
            pl.col("order_date").str.to_datetime(strict=False)
        )

        customer_stats_pl_df = bulk_relations_pl_df.group_by("bulk_customer_id").agg([
            pl.count().alias("orders_count"),
            pl.col("amount").sum().alias("total_amount"),
            pl.col("order_date").min().alias("first_date"),
            pl.col("order_date").max().alias("last_date"),
        ])

        for row in customer_stats_pl_df.iter_rows(named=True):
            customer_id_int = int(row['bulk_customer_id'])
            customer = session.get(BulkCustomer, customer_id_int)
            if customer:
                customer.bulk_orders_count = row['orders_count']
                customer.total_bulk_amount = float(row['total_amount'] if row['total_amount'] is not None else 0.0)
                customer.first_bulk_date = row['first_date']
                customer.last_bulk_date = row['last_date']
                session.add(customer)

    def get_customer_summary(self) -> dict:
        """获取客户数据概要统计信息"""
        # This method is read-only, so it can use the injected session without commit/rollback
        # It might create a new session if one isn't provided, or use the provided one.
        # For consistency, let's assume it uses self.db
        
        # total_customers = self.db.exec(select(func.count(SampleCustomer.id))).one() # This counts only sample
        # Need to count distinct customers across both SampleCustomer and BulkCustomer or a unified Customer table if exists.
        # Assuming SampleCustomer and BulkCustomer might have overlapping customer_name or a more robust way to identify unique customers.
        # For now, let's do a simplified count based on what's available and can be easily counted via ORM.
        
        # Count distinct SampleCustomers
        total_sample_customers = self.db.exec(select(func.count(SampleCustomer.id))).one_or_none() or 0
        
        # Count distinct BulkCustomers
        total_bulk_customers = self.db.exec(select(func.count(BulkCustomer.id))).one_or_none() or 0
        
        # Count converted customers (those present in CustomerConversion table)
        # This assumes CustomerConversion links SampleCustomer to BulkCustomer upon conversion
        converted_customer_ids = self.db.exec(select(CustomerConversion.sample_customer_id).distinct()).all()
        converted_count = len(converted_customer_ids)
        
        # Total unique customers (this is an approximation if names can overlap and are not unique identifiers)
        # A better way would be to have a unique identifier across both tables or a master customer table.
        # For this example, let's sum them up, acknowledging this might double-count if a customer is in both as separate entries.
        # A more accurate total_customers count would require a more complex query or a different schema design.
        # Given the current structure, we rely on counts from individual tables.
        # Let's count unique customer_name from SampleCustomer and BulkCustomer and take the union of names.
        
        conn_uri = self._get_db_connection_uri()
        try:
            # Use read_database_uri when providing a URI string
            sample_names_df = pl.read_database_uri(query="SELECT DISTINCT customer_name FROM sample_customers", uri=conn_uri)
            bulk_names_df = pl.read_database_uri(query="SELECT DISTINCT customer_name FROM bulk_customers", uri=conn_uri)
            sample_names = set(sample_names_df["customer_name"].to_list())
            bulk_names = set(bulk_names_df["customer_name"].to_list())
            total_unique_customer_names = len(sample_names.union(bulk_names))
        except Exception as e:
            logging.warning(f"Polars读取客户名称失败 (summary), 将回退到 ORM 方法: {e}", exc_info=True)
            # Fallback to ORM or set to a value indicating an error
            sample_names_orm = set(name for name, in self.db.exec(select(SampleCustomer.customer_name).distinct()).all()) # Ensure correct unpacking for single column results
            bulk_names_orm = set(name for name, in self.db.exec(select(BulkCustomer.customer_name).distinct()).all()) # Ensure correct unpacking
            total_unique_customer_names = len(sample_names_orm.union(bulk_names_orm))

        conversion_rate = (converted_count / total_sample_customers * 100) if total_sample_customers > 0 else 0
        
        return {
            "total_customers": total_unique_customer_names, # Approximation
            "sample_customers": total_sample_customers,
            "bulk_customers": total_bulk_customers,
            "converted_customers": converted_count,
            "conversion_rate": round(conversion_rate, 2)
        }
    
    def get_sample_customers_by_handler(self, handler: str | None = None) -> list[dict]:
        """按处理人获取样品客户列表"""
        query = select(SampleCustomer)
        if handler:
            query = query.where(SampleCustomer.handler == handler)
        customers = self.db.exec(query).all()
        return [c.model_dump() for c in customers]
    
    def get_bulk_customers_by_handler(self, handler: str | None = None) -> list[dict]:
        """按处理人获取批量客户列表"""
        query = select(BulkCustomer)
        if handler:
            query = query.where(BulkCustomer.handler == handler)
        customers = self.db.exec(query).all()
        return [c.model_dump() for c in customers]
    
    def update_sample_customer(self, customer_id: int, data: dict) -> bool:
        """更新样品客户信息"""
        try:
            customer = self.db.get(SampleCustomer, customer_id)
            if not customer:
                return False
            for key, value in data.items():
                setattr(customer, key, value)
            self.db.add(customer)
            self.db.commit()
            self.db.refresh(customer)
            return True
        except Exception as e: # Add 'e' to capture the exception
            self.db.rollback()
            logging.error(f"更新样品客户信息失败 (ID: {customer_id}): {e}", exc_info=True)
            return False
    
    def update_bulk_customer(self, customer_id: int, data: dict) -> bool:
        """更新批量客户信息"""
        try:
            customer = self.db.get(BulkCustomer, customer_id)
            if not customer:
                return False
            for key, value in data.items():
                setattr(customer, key, value)
            self.db.add(customer)
            self.db.commit()
            self.db.refresh(customer)
            return True
        except Exception as e: # Add 'e' to capture the exception
            self.db.rollback()
            logging.error(f"更新批量客户信息失败 (ID: {customer_id}): {e}", exc_info=True)
            return False
    
    def find_unconverted_customers(self, limit: int = 10) -> list[dict]:
        """查找未转化为批量订单的样品客户"""
        # Find sample_customer_ids that are NOT in CustomerConversion table
        converted_ids_query = select(CustomerConversion.sample_customer_id).distinct()
        converted_ids = self.db.exec(converted_ids_query).all()
        
        unconverted_query = select(SampleCustomer).where(SampleCustomer.id.notin_(converted_ids))
        if limit:
            unconverted_query = unconverted_query.limit(limit)
        
        unconverted_customers = self.db.exec(unconverted_query).all()
        
        result = []
        for sc in unconverted_customers:
            # Get related sample orders for this customer
            # Correct the chaining syntax and ensure date field exists
            order_date_field = getattr(SampleOrder, 'order_created_date', getattr(SampleOrder, 'created_at', None))
            if order_date_field is None:
                # Handle cases where neither date field exists if necessary, maybe default order or log warning
                # For now, let's just select without ordering by date if no date field found
                 sample_orders_query = select(SampleOrderCustomer, SampleOrder).join(
                    SampleOrder, SampleOrderCustomer.sample_order_id == SampleOrder.id
                ).where(
                    SampleOrderCustomer.sample_customer_id == sc.id
                ).limit(3)
            else:
                 sample_orders_query = select(SampleOrderCustomer, SampleOrder).join(
                    SampleOrder, SampleOrderCustomer.sample_order_id == SampleOrder.id
                ).where(
                    SampleOrderCustomer.sample_customer_id == sc.id
                ).order_by(order_date_field.desc()).limit(3)
            
            orders_data = []
            related_orders = self.db.exec(sample_orders_query).all()
            for soc, so in related_orders: # Iterate through (SampleOrderCustomer, SampleOrder) tuples
                # Use the same date field as in order_by for consistency
                actual_order_date = getattr(so, 'order_created_date', getattr(so, 'created_at', None))
                orders_data.append({
                    "order_id": so.order_id,
                    "date": actual_order_date.isoformat() if actual_order_date else None,
                    "amount": so.amount
                })

            # Ensure SampleCustomer fields exist before accessing
            first_purchase_date = getattr(sc, 'first_purchase_date', None)
            last_purchase_date = getattr(sc, 'last_purchase_date', None)
            phone_number = getattr(sc, 'phone', None)
            total_orders = getattr(sc, 'total_sample_orders', getattr(sc, 'sample_orders_count', 0)) # Check both possible names

            result.append({
                "id": sc.id,
                "name": sc.customer_name,
                "phone": phone_number, 
                "shop": sc.shop,
                "first_purchase_date": first_purchase_date.isoformat() if first_purchase_date else None,
                "last_purchase_date": last_purchase_date.isoformat() if last_purchase_date else None,
                "sample_orders_count": total_orders,
                "sample_orders": orders_data
            })
        return result

# Helper functions (if any specific to this service that don't belong in the class)
