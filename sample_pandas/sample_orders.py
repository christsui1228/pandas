import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

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

# 测试数据库连接
try:
    # 从数据库读取数据
    print("正在连接数据库...")
    # 查询全部订单数据
    df = pd.read_sql("SELECT * FROM orders", engine)
    print("数据库连接成功！")
    print("样例数据:")
    print(df.head())
    

    
    # ===== 阶段1：数据预处理 =====
    # 转换日期类型
    if 'order_date' in df.columns and df['order_date'].dtype == 'object':
        print("\n===== 阶段1：数据预处理 =====")
        print("转换日期格式...")
        df['order_date'] = pd.to_datetime(df['order_date'])
        print("日期转换完成！")
        print("数据信息:")
        print(df.info())
    
    # ===== 阶段2：数据分析 =====
    print("\n===== 阶段2：数据分析 =====")
    
    # 2.1 投影所有order_type的值看看有哪些类型
    if 'order_type' in df.columns:
        print("订单类型概要：")
        order_type_counts = df['order_type'].value_counts()
        print(order_type_counts)
        
        # 2.2 检查是否有看样相关的订单类型
        print("\n检查所有包含'样'字的订单类型：")
        sample_types = [t for t in df['order_type'].unique() if '样' in str(t)]
        print(sample_types)
        
        if sample_types:
            # 有包含'样'字的订单类型
            sample_orders = df[df['order_type'].isin(sample_types)].copy()
            print("\n样品相关订单数据：")
            print(sample_orders)
            
            # 2.3 按客户分组分析样品订单
            if 'customer_id' in df.columns:
                print("\n发现customer_id列，进行客户维度分析")
                
                # 查看是否有日期相关列
                date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
                print("\n日期相关列：", date_columns)
                
                if date_columns and not sample_orders.empty:
                    # 使用第一个日期列进行排序
                    date_col = date_columns[0]
                    print(f"\n使用{date_col}列作为日期列")
                    
                    # 需要先确保该列是日期格式
                    if df[date_col].dtype != 'datetime64[ns]':
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                        sample_orders[date_col] = pd.to_datetime(sample_orders[date_col], errors='coerce')
                    
                    # 按客户分组找到最早的样品订单日期
                    sample_dates = sample_orders.groupby('customer_id')[date_col].min().reset_index()
                    sample_dates.columns = ['customer_id', 'first_sample_date']
                    print("\n每个客户最早的样品订单日期:")
                    print(sample_dates)
                    
                    # 按客户统计样品订单数量
                    sample_counts = sample_orders.groupby('customer_id').size().reset_index(name='sample_count')
                    print("\n各客户样品订单数量:")
                    print(sample_counts)
                    
                    # 客户样品订单分析结果
                    # 合并最早样品订单日期和样品订单数量
                    customer_analysis = pd.merge(sample_dates, sample_counts, on='customer_id')
                    
                    # 统计所有客户的订单总数
                    all_counts = df.groupby('customer_id').size().reset_index(name='total_orders')
                    customer_analysis = pd.merge(customer_analysis, all_counts, on='customer_id')
                    
                    # 计算样品订单占比
                    customer_analysis['sample_ratio'] = customer_analysis['sample_count'] / customer_analysis['total_orders']
                    customer_analysis['sample_ratio'] = customer_analysis['sample_ratio'].apply(lambda x: f"{x:.2%}")
                    
                    print("\n客户样品订单分析:")
                    print(customer_analysis)
                    
                    # 按样品订单日期排序并保存结果
                    sorted_samples = sample_orders.sort_values(by=[date_col])
                    sorted_samples.to_csv('sample_orders.csv', index=False)
                    print("\n已将所有样品订单保存到sample_orders.csv")
                    
                    # 将客户分析结果保存
                    customer_analysis.to_csv('customer_sample_analysis.csv', index=False)
                    print("\n已将客户样品订单分析保存到customer_sample_analysis.csv")
                else:
                    print("\n没有找到日期相关列或没有样品订单")
            else:
                print("\n数据中依然没有customer_id列，请确认数据库已更新")
                
                # 如果没有customer_id列但有order_id，仍然按order_id进行分析
                if 'order_id' in df.columns and date_columns and not sample_orders.empty:
                    date_col = date_columns[0]
                    print(f"\n使用{date_col}列进行排序")
                    
                    # 先确保该列是日期格式
                    if df[date_col].dtype != 'datetime64[ns]':
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                        sample_orders[date_col] = pd.to_datetime(sample_orders[date_col], errors='coerce')
                    
                    # 排序并显示样品订单
                    sorted_samples = sample_orders.sort_values(by=[date_col])
                    print("\n按日期排序的样品订单：")
                    print(sorted_samples)
                    
                    # 保存结果到CSV
                    sorted_samples.to_csv('sample_orders.csv', index=False)
                    print("\n已将样品订单保存到sample_orders.csv")
        else:
            print("\n没有找到包含'样'字的订单类型")
    else:
        print("错误：数据中没有order_type列！")
        
except Exception as e:
    print(f"发生错误: {e}")
    print("请确认数据库连接和查询是否正确，或者联系数据库管理员")