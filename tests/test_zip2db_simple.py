import zipfile
import tempfile
import json
from pathlib import Path
from simtoolsz.io import zip2db

def test_basic_functionality():
    """测试基本功能"""
    print("创建测试ZIP文件...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # 创建CSV文件
        csv_content = """name,age,city
Alice,25,New York
Bob,30,London
Charlie,35,Tokyo"""
        csv_file = tmpdir / 'users.csv'
        csv_file.write_text(csv_content, encoding='utf-8')
        
        # 创建JSON文件
        json_content = [
            {"id": 1, "product": "A", "price": 100},
            {"id": 2, "product": "B", "price": 200},
            {"id": 3, "product": "C", "price": 300}
        ]
        json_file = tmpdir / 'products.json'
        json_file.write_text(json.dumps(json_content, ensure_ascii=False), encoding='utf-8')
        
        # 创建ZIP文件
        zip_file = tmpdir / 'test_data.zip'
        with zipfile.ZipFile(zip_file, 'w') as zf:
            zf.write(csv_file, 'users.csv')
            zf.write(json_file, 'products.json')
        
        print(f"ZIP文件创建在: {zip_file}")
        print(f"ZIP文件是否存在: {zip_file.exists()}")
        
        # 测试1: 读取所有文件
        print("\n测试1: 读取所有文件")
        db_file = tmpdir / 'test_all.db'
        try:
            con = zip2db(zip_file, db_file)
            
            # 验证数据
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"创建的表: {[table[0] for table in tables]}")
            
            for table_name in ['users', 'products']:
                if table_name in [t[0] for t in tables]:
                    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"{table_name} 表记录数: {count}")
                    
                    # 显示前几条数据
                    data = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                    print(f"{table_name} 表数据示例: {data}")
            
            con.close()
        except Exception as e:
            print(f"测试1出错: {e}")
            import traceback
            traceback.print_exc()
        
        # 测试2: 指定单个文件
        print("\n测试2: 指定单个文件")
        db_file = tmpdir / 'test_single.db'
        try:
            con = zip2db(zip_file, db_file, filename='users.csv')
            
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"创建的表: {[table[0] for table in tables]}")
            
            con.close()
        except Exception as e:
            print(f"测试2出错: {e}")
            import traceback
            traceback.print_exc()
        
        # 测试3: 自定义表名
        print("\n测试3: 自定义表名")
        db_file = tmpdir / 'test_custom_names.db'
        table_mapping = {
            'users.csv': 'user_table',
            'products.json': 'product_table'
        }
        try:
            con = zip2db(zip_file, db_file, table=table_mapping)
            
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"创建的表: {[table[0] for table in tables]}")
            
            con.close()
        except Exception as e:
            print(f"测试3出错: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n所有测试完成!")

if __name__ == "__main__":
    test_basic_functionality()