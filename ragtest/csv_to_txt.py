import os
import csv
import glob
import shutil

# 输入和输出目录
input_dir = 'ragtest/input'
output_dir = 'ragtest/output_file'

# 确保输出目录存在，如果已存在则先清空
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir, exist_ok=True)

# 获取所有CSV文件
csv_files = glob.glob(os.path.join(input_dir, '*.csv'))

# 处理每个CSV文件
for csv_file in csv_files:
    # 获取文件名（不包括路径和扩展名）
    base_name = os.path.basename(csv_file)
    file_name = os.path.splitext(base_name)[0]
    
    # 创建该CSV文件对应的输出子目录
    file_output_dir = os.path.join(output_dir, file_name)
    os.makedirs(file_output_dir, exist_ok=True)
    
    print(f"处理文件: {base_name}")
    
    # 读取CSV文件
    with open(csv_file, 'r', encoding='utf-8') as f:
        # 使用csv模块读取CSV文件
        csv_reader = csv.reader(f)
        
        # 读取标题行
        try:
            header = next(csv_reader)
            header_text = ','.join(header)
        except StopIteration:
            print(f"  警告: {base_name} 是空文件，跳过")
            continue
        
        # 遍历每一行数据
        for i, row in enumerate(csv_reader, 2):  # 从2开始，因为1是标题行
            # 跳过空行
            if not row:
                continue
                
            # 将行内容转换为文本（用逗号连接）
            content = ','.join(row)
            
            # 创建输出文件名
            output_file = os.path.join(file_output_dir, f"{file_name}_行{i}.txt")
            
            # 写入文本文件，包含标题行和内容行
            with open(output_file, 'w', encoding='utf-8') as out_f:
                out_f.write(header_text + '\n')  # 写入标题行
                out_f.write(content)  # 写入内容行
            
            print(f"  已创建: {output_file}")
    
    print(f"完成处理: {base_name}")

print("所有CSV文件处理完成！")