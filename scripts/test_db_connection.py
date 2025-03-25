#!/usr/bin/env python3
"""
数据库连接测试脚本
用于验证PostgreSQL数据库连接是否正常
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入数据库模块
from app.core.database import get_engine, setup_database

def test_connection():
    """测试数据库连接并创建表"""
    print("正在测试数据库连接...")
    
    try:
        # 加载环境变量
        load_dotenv()
        
        # 获取数据库连接
        engine = get_engine()
        
        # 尝试连接
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();")).fetchone()
            print(f"连接成功! PostgreSQL版本: {result[0]}")
        
        # 测试表创建
        print("\n正在测试数据库表创建...")
        setup_database()
        
        print("\n测试完成! 数据库连接和表创建成功!")
        return True
        
    except SQLAlchemyError as e:
        print(f"\n错误: 数据库连接失败: {str(e)}")
        print("\n请检查以下可能的原因:")
        print("1. .env文件中的数据库配置是否正确")
        print("2. PostgreSQL服务是否运行")
        print("3. 数据库用户权限是否足够")
        print("4. 网络连接是否通畅(如果是远程数据库)")
        return False
    except Exception as e:
        print(f"\n未知错误: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection()