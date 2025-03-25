import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 加载.env文件中的环境变量
load_dotenv()

# 从环境变量获取数据库连接信息
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# 构建数据库连接URL
database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# 创建数据库引擎连接
engine = create_engine(database_url)

# 示例：从数据库读取数据
df = pd.read_sql("SELECT * FROM orders", engine)

# 处理数据...
print(df.head())