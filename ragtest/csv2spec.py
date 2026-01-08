import csv
import os

def extract_column_to_txt(csv_file, column_name, output_file):
    """
    从CSV文件中提取指定列的内容，并保存到txt文件中
    
    Args:
        csv_file: CSV文件路径
        column_name: 要提取的列名
        output_file: 输出的txt文件路径
    """
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # 创建输出目录（如果不存在）
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as out_f:
                for row in reader:
                    # 检查列是否存在
                    if column_name in row and row[column_name]:
                        out_f.write(row[column_name] + '\n')
        
        print(f"已成功从 {csv_file} 提取 '{column_name}' 列到 {output_file}")
    except Exception as e:
        print(f"处理文件 {csv_file} 时出错: {e}")

def main():
    # 定义输入文件和要提取的列
    files_and_columns = [
        {
            'csv_file': 'ragtest/generate_prompt_tuning_text/12315数据_generated+id.csv',
            'column_name': '具体问题',
            'output_file': 'ragtest/generate_prompt_tuning_text/12315数据_具体问题.txt'
        },
        {
            'csv_file': 'ragtest/generate_prompt_tuning_text/汽车缺陷线索报告_generated+id.csv',
            'column_name': '缺陷简述',
            'output_file': 'ragtest/generate_prompt_tuning_text/汽车缺陷线索报告_缺陷简述.txt'
        },
        {
            'csv_file': 'ragtest/generate_prompt_tuning_text/消费品缺陷线索_generated+id.csv',
            'column_name': '缺陷简述',
            'output_file': 'ragtest/generate_prompt_tuning_text/费品缺陷线索_缺陷简述.txt'
        },
        {
            'csv_file': 'ragtest/generate_prompt_tuning_text/汽车技术服务公告_generated+id.csv',
            'column_name': '缺陷简述',
            'output_file': 'ragtest/generate_prompt_tuning_text/汽车技术服务公告_缺陷简述.txt'
        }
    ]
    
    # 处理每个文件
    for item in files_and_columns:
        extract_column_to_txt(item['csv_file'], item['column_name'], item['output_file'])

if __name__ == "__main__":
    main()