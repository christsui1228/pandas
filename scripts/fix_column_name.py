#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复数据库中的列名拼写错误，将wokr_cost改为work_cost
"""

import sys
import os
from dotenv import load_dotenv

# 添加项目根目录到Python路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from sqlmodel import Session, text
from app.core.database import get_engine

def fix_column_name():
    """修复列名拼写错误"""
    print("开始修复列名拼写错误 (wokr_cost -> work_cost)...")
    
    engine = get_engine()
    with Session(engine) as session:
        try:
            # 首先检查original_orders表中是否存在wokr_cost列
            result = session.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'original_orders'
                  AND column_name = 'wokr_cost'
            """))
            
            if result.first():
                # 修改original_orders表中的列名
                session.execute(text("""
                    ALTER TABLE original_orders
                    RENAME COLUMN wokr_cost TO work_cost
                """))
                print("已成功修改original_orders表中的列名")
            else:
                print("original_orders表中已经使用了正确的列名work_cost")
            
            # 同样检查并修改sample_orders表
            result = session.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'sample_orders'
                  AND column_name = 'wokr_cost'
            """))
            
            if result.first():
                session.execute(text("""
                    ALTER TABLE sample_orders
                    RENAME COLUMN wokr_cost TO work_cost
                """))
                print("已成功修改sample_orders表中的列名")
            else:
                print("sample_orders表中已经使用了正确的列名work_cost")
            
            # 检查并修改bulk_orders表
            result = session.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bulk_orders'
                  AND column_name = 'wokr_cost'
            """))
            
            if result.first():
                session.execute(text("""
                    ALTER TABLE bulk_orders
                    RENAME COLUMN wokr_cost TO work_cost
                """))
                print("已成功修改bulk_orders表中的列名")
            else:
                print("bulk_orders表中已经使用了正确的列名work_cost")
            
            # 提交事务
            session.commit()
            print("列名修复完成！")
            
        except Exception as e:
            session.rollback()
            print(f"修复列名时出错: {str(e)}")
            return False
        
        return True

if __name__ == "__main__":
    # 加载环境变量
    load_dotenv()
    
    # 执行列名修复
    success = fix_column_name()
    
    if success:
        print("列名修复操作完成")
    else:
        print("列名修复操作失败")
        sys.exit(1)
