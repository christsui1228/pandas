import pandas as pd
import os
from datetime import datetime, timezone
from sqlmodel import Session, select
from typing import Any, Dict, List
import logging

# 更新导入路径
from app.models.orders import Order
from app.core.config import settings

logger = logging.getLogger(__name__)

class ImportService:
    """Excel导入服务类"""
    
    def __init__(self):
        """初始化导入服务。此类中的主要方法使用独立的数据库连接。"""
        pass
    
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
            logger.error("Excel文件缺少必要列：订单ID (order_id after mapping)")
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
                logger.warning(f"cloth_pack_cost列中存在非数值数据，将强制设置为0.0")
                df["cloth_pack_cost"] = 0.0
        
        # 2. 处理color_amount列（转换为整数）
        if "color_amount" in df.columns:
            df["color_amount"] = pd.to_numeric(df["color_amount"], errors='coerce')
            df["color_amount"] = df["color_amount"].fillna(0).astype(int)
            logger.debug("将color_amount列转换为整数")
        
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
        duplicate_order_ids = df[df["order_id"].duplicated()]["order_id"].unique()
        if len(duplicate_order_ids) > 0:
            logger.warning(f"Excel 文件中发现 {len(duplicate_order_ids)} 个重复的订单ID (共 {df['order_id'].duplicated().sum()} 条重复记录). 将只保留每个ID第一次出现的记录. 重复的ID: {list(duplicate_order_ids)}")
            df = df.drop_duplicates(subset=["order_id"], keep='first')
        
        return df
    
    def import_excel_to_db(self, file_path: str, batch_size: int = 20) -> Dict[str, int]:
        """导入Excel文件到数据库（不使用原生UPSERT，应用层判断），返回处理统计信息"""
        logger.info(f"开始处理文件 (旧版 import_excel_to_db 方法): {file_path}")
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        try:
            df = pd.read_excel(file_path)
            logger.info(f"- 读取了 {len(df)} 行数据")
            df = self.preprocess_data(df)
            logger.info(f"- 预处理后保留 {len(df)} 行有效数据")
            
            if df.empty:
                logger.info("没有有效数据可导入。")
                return stats

            all_order_ids = df['order_id'].unique().tolist()
            existing_order_dict = {}
            
            # This method uses self.db (SQLModel session), which is currently not initialized.
            # This method needs to be refactored or removed if ImportService __init__ doesn't take a session.
            # For now, I will assume this method is DEPRECATED and focus on upsert_excel_to_db.
            # If it were to be fixed, it would need its own session management or receive a session.
            logger.error("import_excel_to_db 方法当前未正确初始化DB会话，其功能已由 upsert_excel_to_db 替代。跳过执行。")
            stats["errors"] = len(df) # Mark all as errors if this path is taken
            return stats
            
        except Exception as e:
            logger.error(f"处理文件 {file_path} (import_excel_to_db) 时发生严重错误: {e}", exc_info=True)
            stats["errors"] = len(df) if 'df' in locals() and hasattr(df, '__len__') else stats.get("errors", 0) + 1
            return stats
    
    def fast_import_excel_to_db(self, file_path: str) -> Dict[str, int]:
        """使用PostgreSQL COPY命令快速导入Excel数据到数据库。
        注意：此方法不检查重复，全部作为新记录插入。如需检查重复，请使用import_excel_to_db方法。
        """
        import tempfile
        import psycopg2
        import os
        
        logger.info(f"开始快速处理文件 (fast_import_excel_to_db): {file_path}")
        stats = {"inserted": 0, "errors": 0}
        conn = None # Initialize conn
        temp_csv_name = None # Initialize temp_csv_name
        
        try:
            df = pd.read_excel(file_path)
            logger.info(f"- 读取了 {len(df)} 行数据")
            df = self.preprocess_data(df)
            logger.info(f"- 预处理后保留 {len(df)} 行有效数据")
            
            if df.empty:
                logger.info("没有有效数据可导入。")
                return stats
            
            now = datetime.now(timezone.utc)
            df['created_at'] = now
            df['updated_at'] = now
            
            temp_csv_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
            temp_csv_name = temp_csv_file.name
            try:
                df = df.replace({pd.NA: None, pd.NaT: None})
                df.to_csv(temp_csv_name, index=False, na_rep='\\N')
                temp_csv_file.close()
                
                conn = psycopg2.connect(
                    dbname=settings.DB_NAME, user=settings.DB_USER,
                    password=settings.DB_PASSWORD, host=settings.DB_HOST,
                    port=settings.DB_PORT, sslmode='disable'
                )
                conn.autocommit = False
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM original_orders LIMIT 0")
                        colnames_db = [desc[0] for desc in cur.description]
                        df_cols = set(df.columns)
                        
                        # Ensure all columns required by DB are in DF, add with default if missing for COPY
                        final_df_cols_for_copy = []
                        for db_col_name in colnames_db:
                            if db_col_name == 'id': continue # Skip auto-increment ID for COPY target columns
                            final_df_cols_for_copy.append(db_col_name)
                            if db_col_name not in df_cols:
                                logger.warning(f"COPY操作：数据库列 '{db_col_name}' 在Excel中缺失，将添加默认值 (None/0)。")
                                if db_col_name in ['cloth_pack_cost', 'work_cost', 'picture_cost', 'color_cost', 'cloth_cost', 'picture_price', 'amount', 'cloth_price']:
                                    df[db_col_name] = 0.0
                                elif db_col_name in ['picture_amount', 'quantity', 'color_amount']:
                                    df[db_col_name] = 0
                                else:
                                    df[db_col_name] = None
                        # Re-export CSV if columns were added to df
                        if len(set(final_df_cols_for_copy) - df_cols) > 0: 
                            df.to_csv(temp_csv_name, index=False, columns=final_df_cols_for_copy, na_rep='\\N')
                        else:
                            df.to_csv(temp_csv_name, index=False, columns=final_df_cols_for_copy, na_rep='\\N')

                        cols_str_for_copy = ', '.join(final_df_cols_for_copy)
                        logger.info("- 开始批量导入 (COPY)...")
                        with open(temp_csv_name, 'r', encoding='utf-8') as f_csv:
                            next(f_csv) # Skip CSV header
                            cur.copy_expert(f"COPY original_orders({cols_str_for_copy}) FROM STDIN WITH CSV NULL AS '\\N'", f_csv)
                        conn.commit()
                        stats["inserted"] = cur.rowcount
                        logger.info(f"- 成功导入 (COPY) {stats['inserted']} 条记录")
                except Exception as e_db:
                    if conn: conn.rollback()
                    logger.error(f"- 数据库操作错误 (COPY): {e_db}", exc_info=True)
                    stats["errors"] = len(df)
                finally:
                    if conn: conn.close()
            finally:
                if temp_csv_name and os.path.exists(temp_csv_name):
                    os.unlink(temp_csv_name)
            
            logger.info(f"快速导入完成: {stats['inserted']}行导入, {stats['errors']}行失败")
            return stats
            
        except Exception as e_main:
            logger.error(f"快速处理文件 {file_path} (fast_import_excel_to_db) 时发生严重错误: {e_main}", exc_info=True)
            stats["errors"] = len(df) if 'df' in locals() and hasattr(df, '__len__') else stats.get("errors", 0) + 1
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
        
        logger.info(f"开始智能处理文件 (upsert_excel_to_db): {file_path}")
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        conn = None # Initialize conn
        
        try:
            df = pd.read_excel(file_path)
            logger.info(f"- 读取了 {len(df)} 行数据")
            df = self.preprocess_data(df)
            logger.info(f"- 预处理后保留 {len(df)} 行有效数据")
            
            if df.empty:
                logger.info("没有有效数据可导入。")
                return stats
            
            now = datetime.now(timezone.utc)
            # df['created_at'] = now # Add created_at only if it's not present or ensure it's handled correctly for insert/update
            df['updated_at'] = now # For EXCLUDED.updated_at
            
            df = df.replace({pd.NA: None, pd.NaT: None})
            all_data_rows = []
            for _, row in df.iterrows():
                record = row.to_dict()
                if 'created_at' not in record or pd.isna(record['created_at']):
                    record['created_at'] = now # Set created_at if missing, for new inserts
                all_data_rows.append(record)
            
            total_rows = len(all_data_rows)
            if total_rows == 0:
                logger.info("数据预处理后没有有效行可导入")
                return stats
            
            for i in range(0, total_rows, batch_size):
                batch_rows_dict = all_data_rows[i:i+batch_size]
                batch_count = len(batch_rows_dict)
                logger.info(f"- 处理批次 {i//batch_size + 1} ({i+1} 至 {min(i+batch_count, total_rows)} / {total_rows} 行)")
                
                conn = psycopg2.connect(
                    dbname=settings.DB_NAME, user=settings.DB_USER,
                    password=settings.DB_PASSWORD, host=settings.DB_HOST,
                    port=settings.DB_PORT, sslmode='disable'
                )
                conn.autocommit = False
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM original_orders LIMIT 0")
                        db_colnames = [desc[0] for desc in cur.description]
                        
                        # Columns for INSERT and temp table (all model fields present in df + updated_at, created_at)
                        # df.columns will have all mapped excel columns, plus updated_at, (potentially created_at)
                        # batch_rows_dict[0].keys() are the actual keys after to_dict() and adding created_at if missing.
                        temp_table_columns = list(batch_rows_dict[0].keys())
                        temp_table_columns = [col for col in temp_table_columns if col in db_colnames or col == 'id'] # Filter to what DB knows, id is not in db_colnames from select * limit 0 usually.
                                                                                                                # No, db_colnames from SELECT * LIMIT 0 INCLUDES 'id'.
                        temp_table_columns = [col for col in list(batch_rows_dict[0].keys()) if col in db_colnames] # Use keys from dict, ensure they are in db table

                        # Ensure created_at is handled: it's in temp_table_columns if in dict keys
                        # Values for temp table insertion
                        values_for_temp_table = [[row.get(col) for col in temp_table_columns] for row in batch_rows_dict]
                        
                        cur.execute("CREATE TEMP TABLE temp_batch (LIKE original_orders INCLUDING ALL) ON COMMIT DROP")
                        execute_values(
                            cur,
                            f"INSERT INTO temp_batch ({', '.join(temp_table_columns)}) VALUES %s",
                            values_for_temp_table
                        )
                        
                        batch_order_ids = [row.get('order_id') for row in batch_rows_dict if row.get('order_id')]
                        existing_ids_in_batch = set()
                        if batch_order_ids:
                            # Parameterized query for existing IDs
                            cur.execute("SELECT order_id FROM original_orders WHERE order_id = ANY(%s)", (batch_order_ids,))
                            existing_ids_in_batch = {row[0] for row in cur.fetchall()}
                        
                        # Define columns for UPSERT SET clause (exclude pk, and created_at)
                        update_set_cols = [col for col in temp_table_columns if col not in ('order_id', 'created_at')]
                        # 'updated_at' will be set from EXCLUDED.updated_at which is now() from temp_batch
                        
                        set_clauses = [f"{col} = EXCLUDED.{col}" for col in update_set_cols]
                        # Always ensure updated_at is set from EXCLUDED (which has the 'now' value)
                        if 'updated_at' not in update_set_cols: # it was excluded above, so this is true
                            set_clauses.append(f"updated_at = EXCLUDED.updated_at")
                        final_update_set_string = ", ".join(set_clauses)

                        if not final_update_set_string: # Should not happen if updated_at is always included
                            logger.error("UPSERT SET子句为空，无法执行更新。")
                            raise ValueError("UPSERT SET子句为空")

                        upsert_sql = f"""
                        INSERT INTO original_orders ({', '.join(temp_table_columns)})
                        SELECT {', '.join(temp_table_columns)} FROM temp_batch
                        ON CONFLICT (order_id) DO UPDATE SET
                        {final_update_set_string}
                        RETURNING order_id;"""
                        
                        cur.execute(upsert_sql)
                        processed_order_ids = [row[0] for row in cur.fetchall()]
                        
                        batch_actual_inserted = 0
                        batch_actual_updated = 0
                        
                        if len(processed_order_ids) == batch_count: # All rows from this batch processed
                            for pid in processed_order_ids:
                                if pid in existing_ids_in_batch:
                                    batch_actual_updated += 1
                                else:
                                    batch_actual_inserted += 1
                            stats["inserted"] += batch_actual_inserted
                            stats["updated"] += batch_actual_updated
                            logger.info(f"  - 批次结果: 新增 {batch_actual_inserted} 条, 更新 {batch_actual_updated} 条.")
                        else:
                            logger.warning(f"  - 批次统计: UPSERT影响/返回 {len(processed_order_ids)} 行, 但批次中有 {batch_count} 行数据. 统计可能不完整.")
                            # Increment errors by the difference or just log
                            stats["errors"] += (batch_count - len(processed_order_ids))

                        conn.commit()
                except Exception as e_db:
                    if conn: conn.rollback()
                    logger.error(f"- 批次 {i//batch_size + 1} 处理时数据库错误: {e_db}", exc_info=True)
                    stats["errors"] += batch_count # All rows in batch considered error
                finally:
                    if conn: conn.close()
            
            logger.info(f"智能导入完成: {stats['inserted']}行新增, {stats['updated']}行更新, {stats['errors']}行失败")
            return stats
            
        except Exception as e_main:
            logger.error(f"处理文件 {file_path} (upsert_excel_to_db) 时发生严重错误: {e_main}", exc_info=True)
            # Try to get a count of rows if df is available, otherwise just increment general error
            stats["errors"] += len(df) if 'df' in locals() and hasattr(df, '__len__') else stats.get("errors",0) + total_rows if 'total_rows' in locals() else 1
            return stats