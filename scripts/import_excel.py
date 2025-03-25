import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入服务类
from app.services.import_service import ImportService
from app.core.database import setup_database

def main():
    """主程序入口"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Excel数据导入PostgreSQL工具')
    parser.add_argument('--dir', type=str, help='Excel文件所在目录')
    parser.add_argument('--file', type=str, help='单个Excel文件路径')
    parser.add_argument('--batch-size', type=int, default=20, help='批处理大小')
    args = parser.parse_args()
    
    # 加载环境变量
    load_dotenv()
    
    # 设置数据库
    setup_database()
    
    # 创建导入服务
    import_service = ImportService()
    
    # 处理文件或目录
    if args.file:
        import_service.import_excel_to_db(args.file, args.batch_size)
    elif args.dir:
        files = import_service.list_excel_files(args.dir)
        for file in files:
            import_service.import_excel_to_db(file, args.batch_size)
    else:
        print("请指定文件(--file)或目录(--dir)")

if __name__ == "__main__":
    main()