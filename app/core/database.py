import os
from sqlalchemy import inspect
from sqlmodel import SQLModel, create_engine, Session
from dotenv import load_dotenv

# 加载.env文件中的环境变量
load_dotenv()

def get_engine():
    """获取数据库引擎连接"""
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    # 添加连接参数解决SSL问题
    database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=disable"
    
    # 优化连接池参数配置，基于阿里云RDS实例规格(400最大连接)
    return create_engine(
        database_url, 
        echo=False, 
        pool_pre_ping=True,       # 自动检测已经断开的连接
        pool_recycle=1800,       # 30分钟重新连接，防止连接过期
        pool_size=30,            # 连接池基础大小，大约为最大连接数的10%
        max_overflow=50,         # 允许创建的最大额外连接数
        pool_timeout=30          # 等待连接的超时时间
    )

def get_session():
    """获取数据库会话"""
    engine = get_engine()
    with Session(engine) as session:
        yield session

def setup_database():
    """初始化数据库表结构"""
    engine = get_engine()
    
    # 检查是否需要创建表
    inspector = inspect(engine)
    
    try:
        # 导入所有需要的模型
        from app.models import Order
        from app.models.sample_orders import SampleOrder
        from app.models.bulk_orders import BulkOrder
        
        # 创建表
        print("创建数据库表...")
        SQLModel.metadata.create_all(engine)
        print("数据库表创建完成！")
    except Exception as e:
        print(f"创建表时出错: {e}")
    
    return engine