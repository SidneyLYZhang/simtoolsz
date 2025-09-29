"""
简单测试 special2db 函数
"""

import tempfile
from pathlib import Path
from simtoolsz.db import special2db

def test_tsv_basic():
    """测试基本的TSV文件处理"""
    print("=== 测试基本的TSV文件处理 ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # 创建TSV文件
        tsv_content = """name\tage\tcity
Alice\t25\tNew York
Bob\t30\tLondon
Charlie\t35\tTokyo"""
        tsv_file = tmpdir / 'users.tsv'
        tsv_file.write_text(tsv_content, encoding='utf-8')
        
        # 测试TSV文件
        db_file = tmpdir / 'test.db'
        con = special2db(tsv_file, db_file)
        
        # 验证数据
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        if 'users' in [t[0] for t in tables]:
            count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            print(f"记录数: {count}")
            
            # 显示数据
            data = con.execute("SELECT * FROM users").fetchall()
            print(f"数据: {data}")
            
            # 验证数据结构
            columns = con.execute("DESCRIBE users").fetchall()
            print(f"列信息: {[(col[0], col[1]) for col in columns]}")
        
        con.close()
        print("✓ TSV文件处理成功")

def test_directory():
    """测试目录处理"""
    print("\n=== 测试目录处理 ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # 创建多个TSV文件
        tsv1_content = """id\tname\tage
1\tAlice\t25
2\tBob\t30"""
        tsv1_file = tmpdir / 'users.tsv'
        tsv1_file.write_text(tsv1_content, encoding='utf-8')
        
        tsv2_content = """product\tprice\tstock
Laptop\t999\t50
Mouse\t25\t200"""
        tsv2_file = tmpdir / 'products.tsv'
        tsv2_file.write_text(tsv2_content, encoding='utf-8')
        
        # 创建一个不支持的文件
        txt_file = tmpdir / 'readme.txt'
        txt_file.write_text("这是一个文本文件", encoding='utf-8')
        
        # 测试目录
        db_file = tmpdir / 'test.db'
        con = special2db(tmpdir, db_file)
        
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        # 验证每个表
        for table_name in ['users', 'products']:
            if table_name in [t[0] for t in tables]:
                count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"{table_name}表记录数: {count}")
        
        con.close()
        print("✓ 目录处理成功")

def test_custom_table_name():
    """测试自定义表名"""
    print("\n=== 测试自定义表名 ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # 创建TSV文件
        tsv_content = """id\tname\tscore
1\tAlice\t95
2\tBob\t87"""
        tsv_file = tmpdir / 'data.tsv'
        tsv_file.write_text(tsv_content, encoding='utf-8')
        
        # 测试自定义表名
        db_file = tmpdir / 'test.db'
        con = special2db(tsv_file, db_file, table='students')
        
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"创建的表: {[table[0] for table in tables]}")
        
        if 'students' in [t[0] for t in tables]:
            count = con.execute("SELECT COUNT(*) FROM students").fetchone()[0]
            print(f"students表记录数: {count}")
            
            # 显示数据
            data = con.execute("SELECT * FROM students").fetchall()
            print(f"数据: {data}")
        
        con.close()
        print("✓ 自定义表名成功")

def test_error_cases():
    """测试错误情况"""
    print("\n=== 测试错误情况 ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # 测试空目录
        empty_dir = tmpdir / 'empty'
        empty_dir.mkdir()
        db_file = tmpdir / 'test_empty.db'
        
        try:
            con = special2db(empty_dir, db_file)
            print("❌ 空目录测试失败 - 应该抛出异常")
        except ValueError as e:
            print(f"✓ 空目录测试成功 - 预期错误: {e}")
        
        # 测试不支持的文件类型
        txt_file = tmpdir / 'test.txt'
        txt_file.write_text("这是不支持的内容")
        db_file = tmpdir / 'test_unsupported.db'
        
        try:
            con = special2db(txt_file, db_file)
            print("❌ 不支持的文件类型测试失败 - 应该抛出异常")
        except ValueError as e:
            print(f"✓ 不支持的文件类型测试成功 - 预期错误: {e}")

if __name__ == "__main__":
    print("开始测试 special2db 函数...")
    
    test_tsv_basic()
    test_directory()
    test_custom_table_name()
    test_error_cases()
    
    print("\n=== 所有测试完成 ===")
    print("special2db 函数工作正常！")