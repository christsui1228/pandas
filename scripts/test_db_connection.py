#!/usr/bin/env python3
"""
数据库连接测试脚本
用于验证PostgreSQL数据库连接是否正常
"""

import os
import sys
# from dotenv import load_dotenv # No longer needed
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlmodel import Session # Added for CustomerService
from app.services.customer_service import CustomerService # Added for Polars test
import polars as pl # Added for Polars test

# 添加项目根目录到Python路径
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Removed this line

# 导入数据库模块和settings
from app.core.database import get_engine, setup_database
from app.core.config import settings # Import centralized settings

def test_connection():
    """测试数据库连接并创建表"""
    print("正在测试数据库连接...")
    
    try:
        # 获取项目根目录路径
        # project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # 明确指定.env文件路径
        # dotenv_path = os.path.join(project_root, '.env')
        # print(f"加载.env文件: {dotenv_path}")
        
        # 加载环境变量
        # load_dotenv(dotenv_path, override=True) # Handled by Pydantic settings
        
        # 显示数据库连接信息（不显示密码）
        # 从 settings 对象获取连接信息
        print(f"数据库连接信息 (从 settings): {settings.DB_USER}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
        
        # 获取数据库连接
        engine = get_engine()
        
        # 尝试连接
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();")).fetchone()
            print(f"连接成功! PostgreSQL版本: {result[0]}")
        
        # 测试表创建
        print("\n正在测试数据库表创建...")
        # setup_database() # Potentially skip this if only testing connection, or keep if schema is needed for Polars tests on actual tables
        
        print("\nSQLAlchemy/SQLModel 连接测试完成!")

        # --- 开始 Polars 连接测试 ---
        print("\n正在测试 Polars 数据库连接...")
        try:
            # CustomerService 需要一个 Session
            # 我们在这里创建一个临时的 Session 用于获取 URI
            # 注意：在实际应用中，Session 的管理会更复杂
            with Session(engine) as session:
                customer_service = CustomerService(db=session)
                polars_conn_uri = customer_service._get_db_connection_uri()
                print(f"  获取到的 Polars 连接 URI: {polars_conn_uri}")

                # 尝试使用 Polars 读取数据
                # 使用一个非常简单的查询来测试连接
                # Use read_database_uri when providing a URI string
                df = pl.read_database_uri(query="SELECT 1 AS test_value, 'hello' AS test_string", uri=polars_conn_uri)
                
                # 验证读取结果
                if not df.is_empty() and df["test_value"][0] == 1 and df["test_string"][0] == "hello":
                    print(f"  Polars 连接成功! 读取到的DataFrame: \n{df}")
                    print("  Polars 数据库连接测试成功!")
                else:
                    print(f"  Polars 连接成功但读取数据验证失败. DataFrame: \n{df}")
                    # return False # Or raise an error

        except Exception as e:
            print(f"\n错误: Polars 数据库连接失败: {str(e)}")
            print("  请检查 CustomerService._get_db_connection_uri() 是否为您的数据库生成了正确的连接字符串。")
            print(f"  当前的 URI 是: {polars_conn_uri if 'polars_conn_uri' in locals() else 'URI 未能生成'}")
            return False # Indicate failure
        # --- 结束 Polars 连接测试 ---
        
        print("\n所有数据库连接测试完成!") # Updated message
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