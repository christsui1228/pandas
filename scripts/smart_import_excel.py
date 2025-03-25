#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
智能导入Excel数据到PostgreSQL
使用PostgreSQL的UPSERT功能高效处理数据导入，已存在的记录会更新，不存在的会新增

特点：
1. 使用数据库端的UPSERT功能，在数据库内高效处理记录的插入和更新
2. 只检查当前导入批次中的记录是否已存在，不会扫描整个历史数据库
3. 性能显著优于传统的逐条处理方法
"""

import sys
import os
import argparse
from dotenv import load_dotenv

# 添加项目根目录到Python路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from app.services.import_service import ImportService

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Excel数据智能导入PostgreSQL工具')
    parser.add_argument('--dir', type=str, help='Excel文件所在目录')
    parser.add_argument('--file', type=str, help='单个Excel文件路径')
    parser.add_argument('--batch-size', type=int, default=500, help='每批处理的记录数量，默认500')
    args = parser.parse_args()
    
    # 加载环境变量
    load_dotenv()
    
    service = ImportService()
    
    # 单文件导入
    if args.file:
        if not os.path.exists(args.file):
            print(f"错误: 文件 {args.file} 不存在")
            sys.exit(1)
        service.upsert_excel_to_db(args.file, batch_size=args.batch_size)
    
    # 目录批量导入
    elif args.dir:
        if not os.path.isdir(args.dir):
            print(f"错误: 目录 {args.dir} 不存在")
            sys.exit(1)
        
        excel_files = service.list_excel_files(args.dir)
        print(f"找到 {len(excel_files)} 个Excel文件")
        
        for file_path in excel_files:
            try:
                service.upsert_excel_to_db(file_path)
            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {str(e)}")
    
    else:
        print("错误: 请指定 --file 或 --dir 参数")
        parser.print_help()
        sys.exit(1)
