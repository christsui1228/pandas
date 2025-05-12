#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
创建客户相关表结构并初始化客户数据同步
"""

import sys
import os
import argparse

# 添加项目根目录到Python路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from sqlmodel import SQLModel, Session
from app.core.database import get_engine
from app.customers.models import SampleCustomer, BulkCustomer, SampleOrderCustomer, BulkOrderCustomer, CustomerConversion
from app.services.customer_service import CustomerService

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='创建并同步客户相关表')
    parser.add_argument('--sync-only', action='store_true', help='仅执行同步，不创建表')
    parser.add_argument('--stats-only', action='store_true', help='仅显示客户统计信息，不执行其他操作')
    parser.add_argument('--show-unconverted', action='store_true', help='显示未转化的样品客户')
    parser.add_argument('--limit', type=int, default=10, help='显示未转化客户的数量限制')
    parser.add_argument('--recreate-tables', action='store_true', help='删除并重新创建表结构(警告:会删除现有数据)')
    args = parser.parse_args()
    
    # 加载环境变量
    # load_dotenv() # Removed, handled by Pydantic/config
    
    # 创建数据库会话
    engine = get_engine()
    with Session(engine) as session:
        # 创建客户服务实例
        customer_service = CustomerService(db=session)
        
        # 仅显示统计信息
        if args.stats_only:
            stats = customer_service.get_customer_summary()
            print("\n客户数据统计:")
            print(f"总客户数: {stats['total_customers']}")
            print(f"样品客户数: {stats['sample_customers']}")
            print(f"批量客户数: {stats['bulk_customers']}")
            print(f"已转化客户数: {stats['converted_customers']}")
            print(f"转化率: {stats['conversion_rate']}%")
            sys.exit(0)
        
        # 显示未转化的样品客户
        if args.show_unconverted:
            unconverted = customer_service.find_unconverted_customers(limit=args.limit)
            print(f"\n未转化的样品客户 (前{args.limit}个):")
            for i, customer in enumerate(unconverted, 1):
                print(f"{i}. {customer['name']} (电话: {customer['phone']}, 店铺: {customer['shop']})")
                print(f"   首次购买: {customer['first_purchase_date']}, 最近购买: {customer['last_purchase_date']}")
                print(f"   样品订单数: {customer['sample_orders_count']}")
                print("   最近订单:")
                for order in customer['sample_orders']:
                    print(f"      {order['order_id']} ({order['date']}): ¥{order['amount']}")
                print()
            sys.exit(0)
        
        # 创建表结构（如果需要）
        if not args.sync_only:
            # 如果指定了表重建选项
            if args.recreate_tables:
                print("删除并重新创建客户相关表...")
                
                # 从SQLModel元数据中获取表名列表
                tables_to_drop = [
                    'sample_customers', 'bulk_customers', 
                    'sample_order_customers', 'bulk_order_customers',
                    'customer_conversions'
                ]
                
                # 使用SQLAlchemy执行表删除
                from sqlalchemy import text
                with session.begin():
                    for table in tables_to_drop:
                        try:
                            session.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                            print(f"  - 表 {table} 已删除")
                        except Exception as e:
                            print(f"  - 删除表 {table} 时出错: {str(e)}")
            
            print("创建客户相关表...")
            SQLModel.metadata.create_all(session.get_bind())
            print("表结构创建完成！")
        
        # 执行数据同步
        print("开始从订单中提取客户数据...")
        result = customer_service.extract_customers_from_orders()
        
        # 输出同步结果
        print("\n同步结果汇总:")
        print(f"新增客户: {result['new']} 位")
        print(f"更新客户: {result['updated']} 位")
        print(f"样品订单关联: {result['sample_relations']} 个")
        print(f"批量订单关联: {result['bulk_relations']} 个")
        
        if result['errors'] > 0:
            print(f"出现错误: {result['errors']} 个")
            sys.exit(1)
        
        # 显示客户统计信息
        print("\n同步后客户数据统计:")
        stats = customer_service.get_customer_summary()
        print(f"总客户数: {stats['total_customers']}")
        print(f"样品客户数: {stats['sample_customers']}")
        print(f"批量客户数: {stats['bulk_customers']}")
        print(f"已转化客户数: {stats['converted_customers']}")
        print(f"转化率: {stats['conversion_rate']}%")
