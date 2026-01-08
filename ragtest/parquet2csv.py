#!/usr/bin/env python3
"""
将parquet文件转换为CSV格式的脚本
"""

import pandas as pd
import sys
import os
import argparse

def parquet_to_csv(input_file, output_file=None, index=False):
    """
    将parquet文件转换为CSV格式
    
    参数:
        input_file (str): 输入的parquet文件路径
        output_file (str, optional): 输出的CSV文件路径，如果不提供，将使用与输入文件相同的名称但扩展名为.csv
        index (bool, optional): 是否在CSV中包含索引列，默认为False
    
    返回:
        bool: 转换是否成功
    """
    try:
        # 如果没有提供输出文件路径，则使用输入文件路径但扩展名改为.csv
        if output_file is None:
            output_file = os.path.splitext(input_file)[0] + '.csv'
        
        # 读取parquet文件
        print(f"正在读取parquet文件: {input_file}")
        df = pd.read_parquet(input_file)
        
        # 显示一些基本信息
        print(f"文件: {input_file}")
        print(f"总行数: {len(df)}")
        print(f"列名: {list(df.columns)}")
        
        # 保存为CSV
        df.to_csv(output_file, index=index)
        print(f"已成功将文件保存为CSV: {output_file}")
        
        return True
    except Exception as e:
        print(f"转换文件时出错: {e}")
        return False

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='将parquet文件转换为CSV格式')
    parser.add_argument('input_files', nargs='+', help='输入的parquet文件路径，可以提供多个文件')
    parser.add_argument('--index', action='store_true', help='在CSV中包含索引列')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 处理每个输入文件
    success = True
    for input_file in args.input_files:
        if not parquet_to_csv(input_file, index=args.index):
            success = False
    
    # 返回适当的退出代码
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())