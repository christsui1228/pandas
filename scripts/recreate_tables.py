#!/usr/bin/env python
# scripts/recreate_tables.py
"""重新创建数据库表结构，用于模型变更后重建表"""

import os
import sys
from sqlalchemy import text

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import get_engine, setup_database

def drop_tables():
    """删除现有的数据库表"""
    engine = get_engine()
    with engine.connect() as conn:
        try:
            conn.execute(text("DROP TABLE IF EXISTS original_orders"))
            conn.commit()
            print("成功删除现有表")
        except Exception as e:
            print(f"删除表时出错: {e}")

if __name__ == "__main__":
    print("准备重新创建数据库表...")
    
    # 1. 删除现有表
    drop_tables()
    
    # 2. 根据新的模型定义创建表
    setup_database()
    
    print("数据库表重新创建完成！")
