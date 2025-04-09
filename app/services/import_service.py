import pandas as pd
import os
from datetime import datetime
from sqlmodel import Session, select
from typing import Any, Dict, List, Optional

# 更新导入路径
from app.models.orders import Order
from app.core.database import get_engine

class ImportService:
    """Excel导入服务类"""
    
    def __init__(self, session: Optional[Session] = None):
        """初始化导入服务，可选传入现有会话"""
        self.session = session
    
    @staticmethod
    def list_excel_files(directory: str) -> List[str]:
        """列出指定目录中的所有Excel文件"""
        return [
            os.path.join(directory, file)
            for file in os.listdir(directory)
            if file.endswith(('.xlsx', '.xls')) and not file.startswith('~$')
        ]
    
    @staticmethod
    def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
        """预处理Excel数据，包括列名映射和数据清洗"""
        # 列名映射 (中文 -> 英文)
        column_mapping = {
            "订单ID": "order_id",
            "角色": "role",
            "处理人": "handler",
            "工艺": "process",
            "金额": "amount",
            "高清图数":"picture_amount",
            "印制报价": "picture_price",
            "高清图尺寸成本": "picture_cost",
            "高清图颜色成本": "color_cost",
            "高清图工费成本": "work_cost",
            "衣服售价总额":"cloth_price", 
            "衣服总数": "quantity",
            "衣服成本": "cloth_cost",
            "叠衣服成本": "cloth_pack_cost",
            "衣服款式": "cloth_code",
            "颜色总数": "color_amount",
            "客户": "customer_name",
            "电话": "phone",
            "渠道": "shop",
            "快递": "express",
            "订单状态": "order_status",
            "下单时间": "order_created_date",
            "处理时间": "order_processed_date",
            "完成时间": "completion_date",
            "订单分类": "order_type",
            "备注": "notes"
        }
        
        # 应用列名映射 (仅对存在的列)
        renamed_cols = {}
        for cn_col, en_col in column_mapping.items():
            if cn_col in df.columns:
                renamed_cols[cn_col] = en_col
        
        if renamed_cols:
            df = df.rename(columns=renamed_cols)
        
        # 确保order_id列存在并且值唯一
        if "order_id" not in df.columns:
            raise ValueError("Excel文件缺少必要列：订单ID")
        
        # 数据类型转换和清洗
        # 1. 处理日期字段
        date_columns = [
            "order_created_date", "order_processed_date", "completion_date"
        ]
        
        for col in date_columns:
            if col in df.columns:
                # 将日期转换为datetime类型，并将无效日期处理为NaT
                df[col] = pd.to_datetime(df[col], errors='coerce')
                # 将NaT值替换为None，避免PostgreSQL错误
                df[col] = df[col].where(df[col].notna(), None)
        
        # 2. 处理数字字段
        numeric_columns = [
            "amount", "picture_amount", "picture_price", "picture_cost", "color_cost", 
            "work_cost", "cloth_price", "quantity", "cloth_cost", "cloth_pack_cost", 
            "color_amount"
        ]
        
        # 先处理需要特殊处理的列
        
        # 1. 处理cloth_pack_cost列（如果是字符串类型，直接设置为0）
        if "cloth_pack_cost" in df.columns:
            if df["cloth_pack_cost"].dtype == 'object':
                print(f"- 警告: cloth_pack_cost列中存在非数值数据，设置为0")
                df["cloth_pack_cost"] = 0.0
        
        # 2. 处理color_amount列（转换为整数）
        if "color_amount" in df.columns:
            # 先转换为浮点数，再转换为整数
            df["color_amount"] = pd.to_numeric(df["color_amount"], errors='coerce')
            df["color_amount"] = df["color_amount"].fillna(0).astype(int)
            print("- 将color_amount列转换为整数")
        
        # 3. 处理picture_amount和quantity列（这些也应该是整数）
        for int_col in ["picture_amount", "quantity"]:
            if int_col in df.columns:
                df[int_col] = pd.to_numeric(df[int_col], errors='coerce')
                df[int_col] = df[int_col].fillna(0).astype(int)
        
        # 处理其他数字字段（浮点数）
        float_cols = ["amount", "picture_price", "picture_cost", "color_cost", 
                     "work_cost", "cloth_price", "cloth_cost"]
        
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)
        
        # 3. 处理字符串字段
        string_columns = [
            "order_id", "role", "handler", "process", "cloth_code", "customer_name", 
            "phone", "shop", "express", "order_status", "order_type", "notes"
        ]
        
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)
        
        # 4. 去除重复的订单ID
        duplicate_orders = df["order_id"].duplicated()
        if duplicate_orders.any():
            print(f"警告: 发现{duplicate_orders.sum()}个重复订单ID，将保留第一个")
            df = df[~df["order_id"].duplicated()]
        
        return df
    
    def import_excel_to_db(self, file_path: str, batch_size: int = 20) -> Dict[str, int]:
        """导入Excel文件到数据库，返回处理统计信息"""
        print(f"处理文件: {file_path}")
        
        # 记录导入统计
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        try:
            # 读取Excel文件
            df = pd.read_excel(file_path)
            print(f"- 读取了{len(df)}行数据")
            
            # 数据预处理
            df = self.preprocess_data(df)
            print(f"- 预处理后保留{len(df)}行有效数据")
            
            # 获取数据库连接字符串
            engine = get_engine()
            
            # 获取所有待处理的订单ID
            all_order_ids = df['order_id'].unique().tolist()
            
            # 查询所有已存在的订单并创建字典
            existing_order_dict = {}
            with Session(engine) as session:
                try:
                    # 分批查询已存在订单，防止查询参数过多
                    query_batch_size = 50
                    for i in range(0, len(all_order_ids), query_batch_size):
                        batch_ids = all_order_ids[i:i+query_batch_size]
                        existing_batch = session.exec(
                            select(Order).where(Order.order_id.in_(batch_ids))
                        ).all()
                        for order in existing_batch:
                            existing_order_dict[order.order_id] = order
                    print(f"- 数据库中已存在{len(existing_order_dict)}条订单记录")
                except Exception as e:
                    print(f"- 查询已存在订单时出错: {str(e)}")
                    # 即使查询失败也继续处理，假设所有记录都是新的
            
            # 按批次处理数据
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                record_count = 0
                
                for _, row in batch.iterrows():
                    try:
                        # 转换为字典并处理NaT和NaN值
                        record = row.to_dict()
                        for key, value in list(record.items()):
                            if pd.isna(value) or str(value) == 'NaT':
                                record[key] = None
                        
                        order_id = record.get('order_id')
                        if not order_id:
                            print(f"- 警告: 跳过缺少订单ID的记录")
                            stats["errors"] += 1
                            continue
                        
                        # 处理单个记录，使用新的数据库会话
                        with Session(engine) as record_session:
                            try:
                                # 检查是否已存在
                                if order_id in existing_order_dict:
                                    # 获取现有订单并创建副本进行更新
                                    existing_record = record_session.exec(
                                        select(Order).where(Order.order_id == order_id)
                                    ).first()
                                    
                                    if existing_record:
                                        # 更新字段
                                        for key, value in record.items():
                                            if key != 'order_id':
                                                setattr(existing_record, key, value)
                                        existing_record.updated_at = datetime.now()
                                        stats["updated"] += 1
                                else:
                                    # 创建新的订单记录
                                    record['created_at'] = datetime.now()
                                    record['updated_at'] = datetime.now()
                                    new_order = Order(**record)
                                    record_session.add(new_order)
                                    # 添加到已存在字典中
                                    existing_order_dict[order_id] = True
                                    stats["inserted"] += 1
                                
                                # 立即提交单个记录
                                record_session.commit()
                                record_count += 1
                            except Exception as e:
                                record_session.rollback()
                                print(f"- 错误记录: {order_id} - {str(e)}")
                                stats["errors"] += 1
                    except Exception as e:
                        print(f"- 处理记录时出错: {str(e)}")
                        stats["errors"] += 1
                
                print(f"- 已处理 {i+record_count}/{len(df)} 行")
            
            print(f"导入完成: {stats['inserted']}行新增, {stats['updated']}行更新, {stats['errors']}行失败")
            return stats
        
        except Exception as e:
            print(f"处理文件{file_path}时出错: {str(e)}")
            return stats
    
    def fast_import_excel_to_db(self, file_path: str) -> Dict[str, int]:
        """使用PostgreSQL COPY命令快速导入Excel数据到数据库。
        注意：此方法不检查重复，全部作为新记录插入。如需检查重复，请使用import_excel_to_db方法。
        """
        import tempfile
        import psycopg2
        import os
        
        print(f"快速处理文件: {file_path}")
        
        # 记录导入统计
        stats = {"inserted": 0, "errors": 0}
        
        try:
            # 读取Excel文件
            df = pd.read_excel(file_path)
            print(f"- 读取了{len(df)}行数据")
            
            # 数据预处理
            df = self.preprocess_data(df)
            print(f"- 预处理后保留{len(df)}行有效数据")
            
            if len(df) == 0:
                print("没有有效数据要导入")
                return stats
            
            # 添加创建和更新时间列
            now = datetime.now()
            df['created_at'] = now
            df['updated_at'] = now
            
            # 获取数据库连接信息
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT")
            db_name = os.getenv("DB_NAME")
            
            # 转换为CSV格式存到临时文件
            temp_csv = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
            try:
                # 将NaN和NaT替换为Null
                df = df.replace({pd.NA: None, pd.NaT: None})
                # 导出CSV，确保正确处理日期和空值
                df.to_csv(temp_csv.name, index=False, na_rep='\\N')
                temp_csv.close()
                
                # 使用psycopg2直接与PG数据库交互
                conn = psycopg2.connect(
                    dbname=db_name,
                    user=db_user,
                    password=db_password,
                    host=db_host,
                    port=db_port,
                    # 禁用SSL
                    sslmode='disable'
                )
                conn.autocommit = False
                
                try:
                    with conn.cursor() as cur:
                        # 获取数据库表的所有列
                        cur.execute("SELECT * FROM original_orders LIMIT 0")
                        colnames = [desc[0] for desc in cur.description]
                        
                        # 确保数据库中的列名与DataFrame列名匹配
                        df_cols = set(df.columns)
                        db_cols = set(colnames)
                        missing_cols = db_cols - df_cols - {'id'}  # 忽略主键列
                        
                        # 如果有缺失的列，将其添加到DataFrame
                        if missing_cols:
                            print(f"- 添加缺失的列: {missing_cols}")
                            for col in missing_cols:
                                # 根据列名判断类型并设置合适的默认值
                                if col in ['cloth_pack_cost', 'work_cost', 'picture_cost', 'color_cost', 'cloth_cost', 'picture_price', 'amount', 'cloth_price']:
                                    df[col] = 0.0  # 金额类默认为0
                                elif col in ['picture_amount', 'quantity', 'color_amount']:
                                    df[col] = 0    # 数量类默认为0
                                else:
                                    df[col] = None  # 其他类型默认为None
                            print("- 为缺失列设置了适当的默认值")
                            df.to_csv(temp_csv.name, index=False, na_rep='\\N')
                        
                        # 准备COPY命令的列列表，排除id列
                        cols = [c for c in colnames if c != 'id']
                        cols_str = ', '.join(cols)
                        
                        # 开始批量导入
                        print("- 开始批量导入...")
                        with open(temp_csv.name, 'r') as f:
                            # 跳过CSV头
                            next(f)
                            # 使用COPY命令快速导入
                            cur.copy_expert(f"COPY original_orders({cols_str}) FROM STDIN WITH CSV NULL AS '\\N'", f)
                        
                        # 提交连接
                        conn.commit()
                        stats["inserted"] = cur.rowcount
                        print(f"- 成功导入 {stats['inserted']} 条记录")
                        
                except Exception as e:
                    conn.rollback()
                    print(f"- 数据库操作错误: {str(e)}")
                    stats["errors"] = len(df)
                finally:
                    conn.close()
                    
            finally:
                # 清理临时文件
                if os.path.exists(temp_csv.name):
                    os.unlink(temp_csv.name)
            
            print(f"快速导入完成: {stats['inserted']}行导入, {stats['errors']}行失败")
            return stats
            
        except Exception as e:
            print(f"快速处理文件{file_path}时出错: {str(e)}")
            return stats
    
    def upsert_excel_to_db(self, file_path: str, batch_size: int = 500) -> Dict[str, int]:
        """使用PostgreSQL的UPSERT功能（INSERT ON CONFLICT）高效导入Excel数据到数据库。
        此方法会检查当前批次中的记录是否已存在，已存在的会更新，不存在的会新增。
        为了防止处理大型数据集时连接中断，现在使用分批处理方式。
        
        Args:
            file_path: Excel文件路径
            batch_size: 每批处理的记录数，默认500行
        """
        import tempfile
        import psycopg2
        import os
        from psycopg2.extras import execute_values
        
        print(f"智能处理文件: {file_path}")
        
        # 记录导入统计
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        try:
            # 读取Excel文件
            df = pd.read_excel(file_path)
            print(f"- 读取了{len(df)}行数据")
            
            # 数据预处理
            df = self.preprocess_data(df)
            print(f"- 预处理后保留{len(df)}行有效数据")
            
            if len(df) == 0:
                print("没有有效数据要导入")
                return stats
            
            # 添加创建和更新时间列
            now = datetime.now()
            df['created_at'] = now
            df['updated_at'] = now
            
            # 将NaN和NaT替换为None
            df = df.replace({pd.NA: None, pd.NaT: None})
            
            # 获取数据库连接信息
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT")
            db_name = os.getenv("DB_NAME")
            
            # 准备数据行
            all_data_rows = []
            for _, row in df.iterrows():
                record = row.to_dict()
                # 处理None值
                for key, value in list(record.items()):
                    if pd.isna(value) or str(value) == 'NaT':
                        record[key] = None
                all_data_rows.append(record)
            
            total_rows = len(all_data_rows)
            if total_rows == 0:
                print("没有有效数据要导入")
                return stats
            
            # 分批处理数据
            for i in range(0, total_rows, batch_size):
                batch_rows = all_data_rows[i:i+batch_size]
                batch_count = len(batch_rows)
                print(f"- 处理批次 {i+1} 至 {min(i+batch_count, total_rows)} / {total_rows} 行")
                
                # 创建新的数据库连接，每批使用新连接防止连接中断
                conn = psycopg2.connect(
                    dbname=db_name,
                    user=db_user,
                    password=db_password,
                    host=db_host,
                    port=db_port,
                    # 禁用SSL
                    sslmode='disable'
                )
                conn.autocommit = False
                
                try:
                    with conn.cursor() as cur:
                        # 获取数据库表的列信息
                        cur.execute("SELECT * FROM original_orders LIMIT 0")
                        colnames = [desc[0] for desc in cur.description]
                        
                        # 排除id列
                        colnames = [col for col in colnames if col != 'id']
                        
                        # 确保只使用数据库中存在的列
                        clean_batch_rows = []
                        for row in batch_rows:
                            clean_row = {k: row.get(k) for k in colnames if k in row}
                            clean_batch_rows.append(clean_row)
                        
                        # 创建临时表存放当前批次的数据
                        cur.execute("CREATE TEMP TABLE temp_batch (LIKE original_orders INCLUDING ALL) ON COMMIT DROP")
                        
                        # 准备数据并插入临时表
                        columns = list(clean_batch_rows[0].keys())
                        values = [[row.get(col) for col in columns] for row in clean_batch_rows]
                        
                        # 批量插入数据到临时表
                        execute_values(
                            cur,
                            f"INSERT INTO temp_batch ({', '.join(columns)}) VALUES %s",
                            values
                        )
                        
                        # 查询计数信息
                        batch_order_ids = [row.get('order_id') for row in clean_batch_rows]
                        placeholders = ','.join([f"'{oid}'" for oid in batch_order_ids if oid])
                        
                        # 检查数据库中已存在的订单ID
                        cur.execute(f"""
                        SELECT order_id FROM original_orders 
                        WHERE order_id IN ({placeholders})
                        """)
                        existing_ids = {row[0] for row in cur.fetchall()}
                        
                        # 执行UPSERT操作
                        update_cols = [col for col in columns if col != 'order_id' and col != 'updated_at']
                        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                        
                        # 构建UPSERT语句
                        upsert_sql = f"""
                        INSERT INTO original_orders ({', '.join(columns)})
                        SELECT {', '.join(columns)} FROM temp_batch
                        ON CONFLICT (order_id) DO UPDATE SET 
                        {update_set}"""
                        
                        # 如果update_set不为空，就添加updated_at的设置
                        if update_set:
                            upsert_sql += ",\nupdated_at = EXCLUDED.updated_at"
                        else:
                            upsert_sql += "\nupdated_at = EXCLUDED.updated_at"
                        
                        # 执行UPSERT操作
                        cur.execute(upsert_sql)
                        
                        # 统计当前批次的结果
                        batch_updated = len(existing_ids)
                        batch_inserted = len(clean_batch_rows) - batch_updated
                        stats["updated"] += batch_updated
                        stats["inserted"] += batch_inserted
                        
                        # 提交当前批次的事务
                        conn.commit()
                        print(f"  - 批次结果: 新增 {batch_inserted} 条, 更新 {batch_updated} 条")
                except Exception as e:
                    conn.rollback()
                    print(f"- 批次 {i+1} 至 {min(i+batch_count, total_rows)} 处理错误: {str(e)}")
                    stats["errors"] += batch_count
                finally:
                    # 关闭当前批次的连接
                    conn.close()
            
            print(f"智能导入完成: {stats['inserted']}行新增, {stats['updated']}行更新, {stats['errors']}行失败")
            return stats
            
        except Exception as e:
            print(f"处理文件{file_path}时出错: {str(e)}")
            return stats