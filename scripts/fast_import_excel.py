#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
快速导入Excel数据到PostgreSQL
使用PostgreSQL原生COPY命令进行批量数据导入，速度远快于常规方法

注意: 此脚本不会检查记录是否重复，全部作为新记录导入
如需检查重复并更新已有记录，请使用import_excel.py脚本
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
    parser = argparse.ArgumentParser(description='Excel数据快速导入PostgreSQL工具')
    parser.add_argument('--dir', type=str, help='Excel文件所在目录')
    parser.add_argument('--file', type=str, help='单个Excel文件路径')
    args = parser.parse_args()
    
    # 加载环境变量
    load_dotenv()
    
    service = ImportService()
    
    # 单文件导入
    if args.file:
        if not os.path.exists(args.file):
            print(f"错误: 文件 {args.file} 不存在")
            sys.exit(1)
        service.fast_import_excel_to_db(args.file)
    
    # 目录批量导入
    elif args.dir:
        if not os.path.isdir(args.dir):
            print(f"错误: 目录 {args.dir} 不存在")
            sys.exit(1)
        
        excel_files = service.list_excel_files(args.dir)
        print(f"找到 {len(excel_files)} 个Excel文件")
        
        for file_path in excel_files:
            try:
                service.fast_import_excel_to_db(file_path)
            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {str(e)}")
    
    else:
        print("错误: 请指定 --file 或 --dir 参数")
        parser.print_help()
        sys.exit(1)
