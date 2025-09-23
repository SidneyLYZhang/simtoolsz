"""
zip2db 函数使用示例

这个示例演示如何使用 zip2db 函数从ZIP文件中读取各种格式的数据文件到DuckDB数据库。
"""

import zipfile
import tempfile
import json
from pathlib import Path
from simtoolsz.io import zip2db

def create_sample_data(tmpdir):
    """创建示例数据"""
    tmpdir = Path(tmpdir)
    
    # 创建CSV文件 - 用户数据
    csv_content = """user_id,name,age,city,email
1,Alice,25,New York,alice@example.com
2,Bob,30,London,bob@example.com
3,Charlie,35,Tokyo,charlie@example.com
4,Diana,28,Paris,diana@example.com
5,Eve,32,Berlin,eve@example.com"""
    csv_file = tmpdir / 'users.csv'
    csv_file.write_text(csv_content, encoding='utf-8')
    
    # 创建JSON文件 - 产品数据
    json_content = [
        {"product_id": 101, "name": "笔记本电脑", "price": 8999, "category": "电子产品", "stock": 50},
        {"product_id": 102, "name": "无线鼠标", "price": 199, "category": "配件", "stock": 200},
        {"product_id": 103, "name": "USB硬盘", "price": 599, "category": "存储", "stock": 80},
        {"product_id": 104, "name": "显示器", "price": 1599, "category": "电子产品", "stock": 30},
        {"product_id": 105, "name": "键盘", "price": 299, "category": "配件", "stock": 150}
    ]
    json_file = tmpdir / 'products.json'
    json_file.write_text(json.dumps(json_content, ensure_ascii=False, indent=2), encoding='utf-8')
    
    # 创建ZIP文件
    zip_file = tmpdir / 'sample_data.zip'
    with zipfile.ZipFile(zip_file, 'w') as zf:
        zf.write(csv_file, 'users.csv')
        zf.write(json_file, 'products.json')
    
    return zip_file

def main():
    """主函数"""
    print("=== zip2db 函数使用示例 ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # 创建示例数据
        zip_file = create_sample_data(tmpdir)
        print(f"示例数据ZIP文件: {zip_file}")
        
        # 示例1: 读取所有文件
        print("\n--- 示例1: 读取所有文件 ---")
        db_file = tmpdir / 'all_data.db'
        con = zip2db(zip_file, db_file)
        
        # 显示所有表
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        # 显示每个表的数据
        for table_name in ['users', 'products']:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"\n{table_name} 表 - 记录数: {count}")
            
            # 显示表结构
            columns = con.execute(f"DESCRIBE {table_name}").fetchall()
            print(f"列信息: {[(col[0], col[1]) for col in columns]}")
            
            # 显示前3条数据
            data = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
            print(f"前3条数据: {data}")
        
        con.close()
        
        # 示例2: 只读取特定文件
        print("\n--- 示例2: 只读取特定文件 ---")
        db_file = tmpdir / 'users_only.db'
        con = zip2db(zip_file, db_file, filename='users.csv')
        
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        if 'users' in [t[0] for t in tables]:
            data = con.execute("SELECT * FROM users").fetchall()
            print(f"用户数据: {data}")
        
        con.close()
        
        # 示例3: 自定义表名
        print("\n--- 示例3: 自定义表名 ---")
        db_file = tmpdir / 'custom_names.db'
        table_mapping = {
            'users.csv': '客户表',
            'products.json': '商品表'
        }
        con = zip2db(zip_file, db_file, table=table_mapping)
        
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        # 查询中文表名
        if '客户表' in [t[0] for t in tables]:
            count = con.execute("SELECT COUNT(*) FROM 客户表").fetchone()[0]
            print(f"客户表记录数: {count}")
        
        con.close()
        
        # 示例4: 使用额外的DuckDB参数
        print("\n--- 示例4: 使用额外的DuckDB参数 ---")
        db_file = tmpdir / 'with_params.db'
        # 为CSV文件指定编码和其他参数
        con = zip2db(zip_file, db_file, 
                     filename='users.csv',
                     encoding='utf-8', 
                     header=True)
        
        if con:
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"创建的表: {[table[0] for table in tables]}")
            con.close()
        
        print("\n=== 所有示例完成 ===")
        print(f"\n数据库文件保存在: {tmpdir}")
        print("注意: 临时目录会在程序结束后自动删除")

if __name__ == "__main__":
    main()