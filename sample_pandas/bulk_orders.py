import os
import pandas as pd
import numpy as np
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

# 主程序
def main():
    try:
        # 从数据库读取数据
        print("正在连接数据库...")
        # 查询全部订单数据
        df = pd.read_sql("SELECT * FROM orders", engine)
        print("数据库连接成功！")
        print(f"共获取 {len(df)} 条订单记录")
        print("样例数据:")
        print(df.head())
        
        # ===== 阶段1：数据预处理 =====
        print("\n===== 阶段1：数据预处理 =====")
        
        # 检查必要的列是否存在
        required_columns = ['order_id', 'customer_id', 'order_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"错误：数据中缺少以下必要列: {', '.join(missing_columns)}")
            return
        
        # 查找日期相关列
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if not date_columns:
            print("错误：数据中没有找到日期相关列")
            return
        
        # 使用第一个找到的日期列
        date_col = date_columns[0]
        print(f"将使用 {date_col} 作为日期列进行分析")
        
        # 转换日期类型
        if df[date_col].dtype != 'datetime64[ns]':
            print(f"转换 {date_col} 为日期格式...")
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            print("日期转换完成！")
        
        # 查找可能的金额列
        amount_columns = [col for col in df.columns if 'amount' in col.lower() or 'price' in col.lower() or 'value' in col.lower()]
        amount_col = None
        if amount_columns:
            amount_col = amount_columns[0]
            print(f"将使用 {amount_col} 作为订单金额列")
            
            # 确保金额列是数值类型
            if df[amount_col].dtype == 'object':
                df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
        else:
            print("警告：未找到可能的订单金额列，某些分析将会受限")
        
        # ===== 阶段2：样品订单分析 =====
        print("\n===== 阶段2：样品订单分析 =====")
        
        # 订单类型概要
        print("订单类型概要：")
        order_type_counts = df['order_type'].value_counts()
        print(order_type_counts)
        
        # 检查是否有样品相关的订单类型
        print("\n检查所有包含'样'字的订单类型：")
        sample_types = [t for t in df['order_type'].unique() if '样' in str(t)]
        print(sample_types)
        
        if not sample_types:
            print("错误：没有找到包含'样'字的订单类型，无法进行后续分析")
            return
        
        # 提取样品订单
        sample_orders = df[df['order_type'].isin(sample_types)].copy()
        print(f"\n样品相关订单数据：共 {len(sample_orders)} 条记录")
        
        if sample_orders.empty:
            print("错误：没有找到样品订单，无法进行后续分析")
            return
        
        # 按客户分析样品订单
        print("\n客户样品订单分析:")
        
        # 按客户分组找到最早的样品订单日期
        sample_dates = sample_orders.groupby('customer_id')[date_col].min().reset_index()
        sample_dates.columns = ['customer_id', 'first_sample_date']
        print("\n每个客户最早的样品订单日期:")
        print(sample_dates.head())
        
        # 按客户统计样品订单数量
        sample_counts = sample_orders.groupby('customer_id').size().reset_index(name='sample_count')
        print("\n各客户样品订单数量:")
        print(sample_counts.head())
        
        # 合并最早样品订单日期和样品订单数量
        customer_analysis = pd.merge(sample_dates, sample_counts, on='customer_id')
        
        # 统计所有客户的订单总数
        all_counts = df.groupby('customer_id').size().reset_index(name='total_orders')
        customer_analysis = pd.merge(customer_analysis, all_counts, on='customer_id')
        
        # 计算样品订单占比
        customer_analysis['sample_ratio'] = customer_analysis['sample_count'] / customer_analysis['total_orders']
        customer_analysis['sample_ratio'] = customer_analysis['sample_ratio'].apply(lambda x: f"{x:.2%}")
        
        print("\n客户样品订单分析:")
        print(customer_analysis.head())
        
        # 保存样品订单分析结果
        sample_orders.sort_values(by=[date_col]).to_csv('sample_orders.csv', index=False)
        print("\n已将所有样品订单保存到sample_orders.csv")
        customer_analysis.to_csv('customer_sample_analysis.csv', index=False)
        print("\n已将客户样品订单分析保存到customer_sample_analysis.csv")
        
        # ===== 阶段3：批量订单分析 =====
        print("\n===== 阶段3：批量订单分析 =====")
        
        # 创建批量订单分析DataFrame
        print("\n计算每个客户从样品订单到批量订单的转化情况...")
        
        # 初始化转化分析列
        customer_analysis['bulk_count'] = 0
        customer_analysis['bulk_amount'] = 0.0
        customer_analysis['days_to_first_bulk'] = np.nan
        customer_analysis['conversion_status'] = '未转化'
        
        # 批量进行转化分析 - 使用pandas向量化操作
        # 1. 为每个客户创建一个布尔掩码Series的字典，标识哪些订单是该客户样品日期后的批量订单
        masks = {}
        for customer_id, first_sample_date in zip(customer_analysis['customer_id'], customer_analysis['first_sample_date']):
            # 该客户的所有订单
            customer_mask = df['customer_id'] == customer_id
            # 该客户样品日期后的订单
            date_mask = df[date_col] > first_sample_date
            # 该客户的非样品订单
            non_sample_mask = ~df['order_type'].isin(sample_types)
            # 组合掩码：该客户在样品日期后的批量订单
            masks[customer_id] = customer_mask & date_mask & non_sample_mask
        
        # 2. 根据掩码计算每个客户的批量订单数量
        for customer_id in customer_analysis['customer_id']:
            bulk_orders = df[masks[customer_id]]
            
            # 更新批量订单数量
            idx = customer_analysis.index[customer_analysis['customer_id'] == customer_id].tolist()[0]
            customer_analysis.at[idx, 'bulk_count'] = len(bulk_orders)
            
            # 如果有批量订单，填充其他指标
            if len(bulk_orders) > 0:
                # 更新转化状态
                customer_analysis.at[idx, 'conversion_status'] = '已转化'
                
                # 计算批量订单总金额
                if amount_col:
                    customer_analysis.at[idx, 'bulk_amount'] = bulk_orders[amount_col].sum()
                
                # 计算转化天数（从样品订单到首个批量订单）
                first_bulk_date = bulk_orders[date_col].min()
                days_diff = (first_bulk_date - customer_analysis.at[idx, 'first_sample_date']).days
                customer_analysis.at[idx, 'days_to_first_bulk'] = days_diff
        
        # 显示批量订单分析结果
        print("\n客户样品转批量订单分析:")
        print(customer_analysis.head())
        
        # 计算转化率
        conversion_count = (customer_analysis['conversion_status'] == '已转化').sum()
        total_sample_customers = len(customer_analysis)
        conversion_rate = conversion_count / total_sample_customers
        print(f"\n样品到批量订单的总体转化率: {conversion_rate:.2%} ({conversion_count}/{total_sample_customers})")
        
        # 计算平均转化天数
        converted_customers = customer_analysis[customer_analysis['conversion_status'] == '已转化']
        if not converted_customers.empty:
            avg_days = converted_customers['days_to_first_bulk'].mean()
            print(f"平均转化天数: {avg_days:.1f}天")
            
            # 计算转化天数分布
            print("\n转化天数分布:")
            days_bins = [0, 7, 14, 30, 60, 90, float('inf')]
            days_labels = ['0-7天', '8-14天', '15-30天', '31-60天', '61-90天', '90天以上']
            converted_customers['days_bin'] = pd.cut(converted_customers['days_to_first_bulk'], bins=days_bins, labels=days_labels)
            days_distribution = converted_customers['days_bin'].value_counts().sort_index()
            print(days_distribution)
            
            # 不生成可视化图表，仅提供数据分析
        
        # 计算批量订单金额分布
        if amount_col:
            print("\n批量订单金额分布:")
            amount_bins = [0, 1000, 5000, 10000, 50000, float('inf')]
            amount_labels = ['<1000', '1000-5000', '5000-10000', '10000-50000', '>50000']
            converted_customers['amount_bin'] = pd.cut(converted_customers['bulk_amount'], bins=amount_bins, labels=amount_labels)
            amount_distribution = converted_customers['amount_bin'].value_counts().sort_index()
            print(amount_distribution)
            
            # 不生成可视化图表，仅提供数据分析
        
        # 保存完整的转化分析结果
        customer_analysis.to_csv('customer_conversion_analysis.csv', index=False)
        print("\n已将完整的客户转化分析保存到customer_conversion_analysis.csv")
        
        # ===== 阶段4：批量订单与样品订单的时间序列分析 =====
        print("\n===== 阶段4：时间序列分析 =====")
        
        # 按月统计样品订单和批量订单的数量
        df['month'] = df[date_col].dt.to_period('M')
        sample_monthly = df[df['order_type'].isin(sample_types)].groupby('month').size()
        
        # 创建批量订单掩码
        bulk_mask = ~df['order_type'].isin(sample_types)
        bulk_monthly = df[bulk_mask].groupby('month').size()
        
        # 合并月度数据
        monthly_data = pd.DataFrame({'样品订单': sample_monthly, '批量订单': bulk_monthly})
        monthly_data = monthly_data.fillna(0)
        print("\n月度订单数量趋势:")
        print(monthly_data)
        
        # 不生成可视化图表，仅提供数据分析
        
        # 保存月度数据
        monthly_data.to_csv('monthly_order_trend.csv')
        print("\n已将月度订单趋势数据保存到monthly_order_trend.csv")
        
    except Exception as e:
        print(f"程序执行过程中发生错误: {e}")
        print("请确认数据库连接和查询是否正确，或者联系数据库管理员")

if __name__ == "__main__":
    main()
