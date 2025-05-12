#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
从 original_orders 表同步数据到 sample_orders 和 bulk_orders 表。

根据 original_orders.order_type 将订单分类:
- 样品订单: "纯衣看样", "打样单"
- 批量订单: "新订单", "续订单", "纯衣单", "改版续订"

只同步目标表中不存在的 order_id。
"""

import sys
import os
import argparse
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, Session, select, create_engine

# 添加项目根目录到Python路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from app.core.database import get_engine  # 使用 get_engine 获取引擎实例
from app.models.orders import Order as OriginalOrder
from app.sample_orders.models import SampleOrder
from app.models.bulk_orders import BulkOrder # 确认 BulkOrder 的实际路径

# 定义订单类型映射
SAMPLE_ORDER_TYPES = ["纯衣看样", "打样单"]
BULK_ORDER_TYPES = ["新订单", "续订单", "纯衣单", "改版续订"]

def get_existing_order_ids(session: Session, model: SQLModel) -> set:
    """获取指定表中所有已存在的 order_id"""
    statement = select(model.order_id)
    results = session.execute(statement).scalars().all()
    return set(results)

def sync_orders(engine, dry_run=False):
    """执行订单同步"""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    inserted_sample_count = 0
    inserted_bulk_count = 0
    
    with SessionLocal() as session:
        try:
            # 1. 读取原始订单
            print("正在读取原始订单...")
            original_orders_df = pd.read_sql(select(OriginalOrder), engine)
            print(f"读取到 {len(original_orders_df)} 条原始订单。")
            
            if original_orders_df.empty:
                print("原始订单表中没有数据，无需同步。")
                return 0, 0

            # 2. 获取目标表中已存在的 order_id
            print("正在检查目标表中已存在的订单ID...")
            existing_sample_ids = get_existing_order_ids(session, SampleOrder)
            existing_bulk_ids = get_existing_order_ids(session, BulkOrder)
            print(f"发现 {len(existing_sample_ids)} 个已存在的样品订单ID。")
            print(f"发现 {len(existing_bulk_ids)} 个已存在的批量订单ID。")

            # 3. 筛选需要同步的样品订单
            sample_to_sync_df = original_orders_df[
                original_orders_df['order_type'].isin(SAMPLE_ORDER_TYPES) &
                ~original_orders_df['order_id'].isin(existing_sample_ids)
            ]
            print(f"筛选出 {len(sample_to_sync_df)} 条新的样品订单需要同步。")

            # 4. 筛选需要同步的批量订单
            bulk_to_sync_df = original_orders_df[
                original_orders_df['order_type'].isin(BULK_ORDER_TYPES) &
                ~original_orders_df['order_id'].isin(existing_bulk_ids)
            ]
            print(f"筛选出 {len(bulk_to_sync_df)} 条新的批量订单需要同步。")
            
            if dry_run:
                print("\n[模拟运行模式] 不会实际写入数据库。")
                inserted_sample_count = len(sample_to_sync_df)
                inserted_bulk_count = len(bulk_to_sync_df)
            else:
                # 5. 插入样品订单
                if not sample_to_sync_df.empty:
                    print(f"开始插入 {len(sample_to_sync_df)} 条样品订单...")
                    sample_orders_to_insert = []
                    # 获取 SampleOrder 模型的所有字段名
                    sample_fields = {field for field in SampleOrder.__fields__} 
                    for _, row in sample_to_sync_df.iterrows():
                         # 只选择 SampleOrder 中存在的字段进行创建
                        data = {k: v for k, v in row.items() if k in sample_fields and pd.notna(v)}
                        sample_orders_to_insert.append(SampleOrder(**data))
                    
                    if sample_orders_to_insert:
                        session.add_all(sample_orders_to_insert)
                        session.commit()
                        inserted_sample_count = len(sample_orders_to_insert)
                        print(f"成功插入 {inserted_sample_count} 条样品订单。")
                    else:
                        print("没有有效的样品订单数据可插入。")
                else:
                     print("没有新的样品订单需要插入。")


                # 6. 插入批量订单
                if not bulk_to_sync_df.empty:
                    print(f"开始插入 {len(bulk_to_sync_df)} 条批量订单...")
                    bulk_orders_to_insert = []
                    # 获取 BulkOrder 模型的所有字段名
                    bulk_fields = {field for field in BulkOrder.__fields__}
                    for _, row in bulk_to_sync_df.iterrows():
                        # 只选择 BulkOrder 中存在的字段进行创建
                        data = {k: v for k, v in row.items() if k in bulk_fields and pd.notna(v)}
                        bulk_orders_to_insert.append(BulkOrder(**data))

                    if bulk_orders_to_insert:
                        session.add_all(bulk_orders_to_insert)
                        session.commit()
                        inserted_bulk_count = len(bulk_orders_to_insert)
                        print(f"成功插入 {inserted_bulk_count} 条批量订单。")
                    else:
                         print("没有有效的批量订单数据可插入。")
                else:
                    print("没有新的批量订单需要插入。")

        except Exception as e:
            print(f"\n同步过程中发生错误: {e}")
            session.rollback()
            raise
        finally:
            # session 在 with 语句结束时会自动关闭，无需手动 close
            pass
            
    return inserted_sample_count, inserted_bulk_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='同步原始订单到样品订单和批量订单表')
    parser.add_argument('--dry-run', action='store_true', help='模拟运行，不实际写入数据库')
    args = parser.parse_args()
    
    print("开始执行订单同步脚本...")
    
    engine = get_engine() # 直接调用 get_engine 获取引擎
    
    try:
        new_sample, new_bulk = sync_orders(engine, dry_run=args.dry_run)
        print("\n同步完成！")
        print(f"新增样品订单: {new_sample} 条")
        print(f"新增批量订单: {new_bulk} 条")
    except Exception as e:
        print(f"脚本执行失败: {e}")
        sys.exit(1)
    
    print("订单同步脚本执行结束。")
