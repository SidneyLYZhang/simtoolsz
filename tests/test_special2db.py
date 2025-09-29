"""
测试 special2db 函数
"""

import tempfile
from pathlib import Path
from simtoolsz.db import special2db

def create_sample_data():
    """创建测试数据"""
    
    tmpdir = tempfile.mkdtemp()
    tmpdir = Path(tmpdir)
    
    # 创建TSV文件
    tsv_content = """name\tage\tcity
Alice\t25\tNew York
Bob\t30\tLondon
Charlie\t35\tTokyo"""
    tsv_file = tmpdir / 'users.tsv'
    tsv_file.write_text(tsv_content, encoding='utf-8')
    
    # 创建Arrow文件（使用pandas和pyarrow）
    arrow_file = None
    try:
        import pandas as pd
        import pyarrow as pa
        
        # 创建数据
        data = {
            'product_id': [1, 2, 3, 4, 5],
            'name': ['笔记本电脑', '无线鼠标', 'USB硬盘', '显示器', '键盘'],
            'price': [8999, 199, 599, 1599, 299],
            'stock': [50, 200, 80, 30, 150]
        }
        df = pd.DataFrame(data)
        
        # 保存为Arrow格式
        arrow_file = tmpdir / 'products.arrow'
        table = pa.Table.from_pandas(df)
        pa.ipc.write_feather(table, arrow_file)
        
    except ImportError:
        print("警告: 缺少pyarrow，跳过Arrow文件创建")
    
    # 创建Avro文件（如果fastavro可用）
    avro_file = None
    try:
        import fastavro
        
        # 创建Avro数据
        avro_schema = {
            'doc': '订单数据',
            'name': 'Order',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name': 'order_id', 'type': 'int'},
                {'name': 'customer', 'type': 'string'},
                {'name': 'amount', 'type': 'float'},
                {'name': 'status', 'type': 'string'}
            ]
        }
        
        avro_data = [
            {'order_id': 1001, 'customer': 'Alice', 'amount': 150.50, 'status': 'completed'},
            {'order_id': 1002, 'customer': 'Bob', 'amount': 275.00, 'status': 'pending'},
            {'order_id': 1003, 'customer': 'Charlie', 'amount': 99.99, 'status': 'completed'}
        ]
        
        avro_file = tmpdir / 'orders.avro'
        with open(avro_file, 'wb') as f:
            fastavro.writer(f, avro_schema, avro_data)
            
    except ImportError:
        print("警告: 缺少fastavro，跳过Avro文件创建")
    
    return tmpdir, tsv_file, arrow_file, avro_file

def test_single_files():
    """测试单个文件"""
    print("=== 测试单个文件 ===")
    
    tmpdir, tsv_file, arrow_file, avro_file = create_sample_data()
    
    with tempfile.TemporaryDirectory() as test_dir:
        test_dir = Path(test_dir)
        
        # 测试TSV文件
        print("\n--- 测试TSV文件 ---")
        db_file = test_dir / 'test_tsv.db'
        try:
            con = special2db(tsv_file, db_file)
            
            # 验证数据
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"TSV - 创建的表: {[table[0] for table in tables]}")
            
            if 'users' in [t[0] for t in tables]:
                count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                print(f"TSV - users表记录数: {count}")
                
                # 显示数据
                data = con.execute("SELECT * FROM users").fetchall()
                print(f"TSV - 数据: {data}")
            
            con.close()
        except Exception as e:
            print(f"TSV测试出错: {e}")
        
        # 测试Arrow文件（如果存在）
        if arrow_file and arrow_file.exists():
            print("\n--- 测试Arrow文件 ---")
            db_file = test_dir / 'test_arrow.db'
            try:
                con = special2db(arrow_file, db_file, table='products')
                
                tables = con.execute("SHOW TABLES").fetchall()
                print(f"Arrow - 创建的表: {[table[0] for table in tables]}")
                
                if 'products' in [t[0] for t in tables]:
                    count = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]
                    print(f"Arrow - products表记录数: {count}")
                    
                    # 显示数据
                    data = con.execute("SELECT * FROM products LIMIT 3").fetchall()
                    print(f"Arrow - 数据示例: {data}")
                
                con.close()
            except Exception as e:
                print(f"Arrow测试出错: {e}")
        
        # 测试Avro文件（如果存在）
        if avro_file and avro_file.exists():
            print("\n--- 测试Avro文件 ---")
            db_file = test_dir / 'test_avro.db'
            try:
                con = special2db(avro_file, db_file)
                
                tables = con.execute("SHOW TABLES").fetchall()
                print(f"Avro - 创建的表: {[table[0] for table in tables]}")
                
                if 'orders' in [t[0] for t in tables]:
                    count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                    print(f"Avro - orders表记录数: {count}")
                    
                    # 显示数据
                    data = con.execute("SELECT * FROM orders").fetchall()
                    print(f"Avro - 数据: {data}")
                
                con.close()
            except Exception as e:
                print(f"Avro测试出错: {e}")

def test_directory():
    """测试目录处理"""
    print("\n=== 测试目录处理 ===")
    
    with tempfile.TemporaryDirectory() as test_dir:
        test_dir = Path(test_dir)
        
        # 创建TSV文件
        tsv_content = """name\tage\tcity
Alice\t25\tNew York
Bob\t30\tLondon
Charlie\t35\tTokyo"""
        test_tsv = test_dir / 'users.tsv'
        test_tsv.write_text(tsv_content, encoding='utf-8')
        
        db_file = test_dir / 'test_dir.db'
        try:
            con = special2db(test_dir, db_file)
            
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"目录测试 - 创建的表: {[table[0] for table in tables]}")
            
            con.close()
        except Exception as e:
            print(f"目录测试出错: {e}")

def test_error_handling():
    """测试错误处理"""
    print("\n=== 测试错误处理 ===")
    
    with tempfile.TemporaryDirectory() as test_dir:
        test_dir = Path(test_dir)
        
        # 测试空目录
        empty_dir = test_dir / 'empty'
        empty_dir.mkdir()
        db_file = test_dir / 'test_empty.db'
        
        try:
            con = special2db(empty_dir, db_file)
            print("空目录测试 - 不应该到达这里")
        except ValueError as e:
            print(f"空目录测试 - 预期错误: {e}")
        except Exception as e:
            print(f"空目录测试 - 意外错误: {e}")
        
        # 测试不支持的文件类型
        unsupported_file = test_dir / 'test.txt'
        unsupported_file.write_text("这是不支持的内容")
        db_file = test_dir / 'test_unsupported.db'
        
        try:
            con = special2db(unsupported_file, db_file)
            print("不支持的文件类型测试 - 不应该创建表")
        except ValueError as e:
            print(f"不支持的文件类型测试 - 预期错误: {e}")
        except Exception as e:
            print(f"不支持的文件类型测试 - 意外错误: {e}")

if __name__ == "__main__":
    print("开始测试 special2db 函数...")
    
    test_single_files()
    test_directory()
    test_error_handling()
    
    print("\n=== 所有测试完成 ===")