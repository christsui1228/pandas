#!/usr/bin/env python
# scripts/setup_original_orders_table.py
"""
初始化原始订单表结构 (original_orders)

这个脚本用于创建或重新创建原始订单表 (original_orders)。
此表存储从Excel导入的原始订单数据，是后续处理订单和客户数据的基础。
运行此脚本会先删除已存在的original_orders表，然后重新创建。

使用方法:
    pdm run python scripts/setup_original_orders_table.py

注意: 
    - 此脚本仅处理original_orders表，不影响其他表
    - 运行后需要使用import_excel.py导入实际数据
    - 请在setup_customer_tables.py和setup_order_tables.py之前运行此脚本
"""

import os
import sys
import argparse
from sqlalchemy import text

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import get_engine
from app.models.orders import Order  # 导入原始订单模型

def drop_original_orders_table(engine):
    """删除现有的原始订单表"""
    with engine.connect() as conn:
        try:
            conn.execute(text("DROP TABLE IF EXISTS original_orders"))
            conn.commit()
            print("成功删除原始订单表(如果存在)")
        except Exception as e:
            print(f"删除表时出错: {e}")

def create_original_orders_table(engine):
    """创建原始订单表结构"""
    from sqlmodel import SQLModel
    
    try:
        # 仅创建原始订单表
        Order.metadata.create_all(engine)
        print("成功创建原始订单表结构")
    except Exception as e:
        print(f"创建表时出错: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='设置原始订单表')
    parser.add_argument('--no-drop', action='store_true', help='不删除现有表，仅创建不存在的表')
    args = parser.parse_args()
    
    print("准备设置原始订单表...")
    
    # 获取数据库引擎
    engine = get_engine()
    
    # 1. 删除现有表(除非指定--no-drop)
    if not args.no_drop:
        drop_original_orders_table(engine)
    
    # 2. 创建表结构
    create_original_orders_table(engine)
    
    print("原始订单表设置完成！")
    print("下一步: 运行import_excel.py导入订单数据")
