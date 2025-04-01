#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
创建订单相关表结构并初始化同步
"""

import sys
import os
import argparse
from dotenv import load_dotenv

# 添加项目根目录到Python路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from sqlmodel import SQLModel
from app.core.database import get_engine
from app.models.sample_orders import SampleOrder
from app.models.bulk_orders import BulkOrder
from app.services.order_sync_service import OrderSyncService

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='创建并同步订单相关表')
    parser.add_argument('--sync-only', action='store_true', help='仅执行同步，不创建表')
    args = parser.parse_args()
    
    # 加载环境变量
    load_dotenv()
    
    # 创建表结构（如果需要）
    if not args.sync_only:
        print("创建订单相关表...")
        engine = get_engine()
        SQLModel.metadata.create_all(engine)
        print("表结构创建完成！")
    
    # 执行数据同步
    print("开始同步订单数据...")
    sync_service = OrderSyncService()
    result = sync_service.sync_all_orders()
    
    # 输出同步结果
    print("\n同步结果汇总:")
    print(f"样品订单: 新增 {result['sample_orders']['inserted']}行, 更新 {result['sample_orders']['updated']}行, 错误 {result['sample_orders']['errors']}个")
    print(f"批量订单: 新增 {result['bulk_orders']['inserted']}行, 更新 {result['bulk_orders']['updated']}行, 错误 {result['bulk_orders']['errors']}个")
