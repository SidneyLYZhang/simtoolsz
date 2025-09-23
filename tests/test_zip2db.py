import pandas as pd
import zipfile
import tempfile
from pathlib import Path
from simtoolsz.io import zip2db
import duckdb

def test_zip2db():
    """测试zip2db函数"""
    
    # 创建测试数据
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # 创建CSV文件
        csv_data = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'city': ['New York', 'London', 'Tokyo']
        })
        csv_file = tmpdir / 'test_data.csv'
        csv_data.to_csv(csv_file, index=False)
        
        # 创建Excel文件
        excel_data = pd.DataFrame({
            'product': ['A', 'B', 'C'],
            'price': [100, 200, 300],
            'quantity': [10, 20, 30]
        })
        excel_file = tmpdir / 'test_data.xlsx'
        excel_data.to_excel(excel_file, index=False)
        
        # 创建JSON文件
        json_data = pd.DataFrame({
            'id': [1, 2, 3],
            'status': ['active', 'inactive', 'pending']
        })
        json_file = tmpdir / 'test_data.json'
        json_data.to_json(json_file, orient='records')
        
        # 创建Parquet文件
        parquet_data = pd.DataFrame({
            'category': ['X', 'Y', 'Z'],
            'value': [1.5, 2.5, 3.5]
        })
        parquet_file = tmpdir / 'test_data.parquet'
        parquet_data.to_parquet(parquet_file)
        
        # 创建ZIP文件
        zip_file = tmpdir / 'test_files.zip'
        with zipfile.ZipFile(zip_file, 'w') as zf:
            zf.write(csv_file, 'users.csv')
            zf.write(excel_file, 'products.xlsx')
            zf.write(json_file, 'orders.json')
            zf.write(parquet_file, 'categories.parquet')
        
        # 测试1: 读取所有文件
        print("测试1: 读取所有文件")
        db_file = tmpdir / 'test_all.db'
        con = zip2db(zip_file, db_file)
        
        # 验证数据
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        for table_name in ['users', 'products', 'orders', 'categories']:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"{table_name} 表记录数: {count}")
        
        con.close()
        
        # 测试2: 指定单个文件
        print("\n测试2: 指定单个文件")
        db_file = tmpdir / 'test_single.db'
        con = zip2db(zip_file, db_file, filename='users.csv')
        
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        con.close()
        
        # 测试3: 自定义表名
        print("\n测试3: 自定义表名")
        db_file = tmpdir / 'test_custom_names.db'
        table_mapping = {
            'users.csv': 'user_table',
            'products.xlsx': 'product_table',
            'orders.json': 'order_table',
            'categories.parquet': 'category_table'
        }
        con = zip2db(zip_file, db_file, table=table_mapping)
        
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        con.close()
        
        print("\n所有测试完成!")

if __name__ == "__main__":
    test_zip2db()