"""
special2db 函数使用示例

这个示例演示如何使用 special2db 函数将特殊格式（TSV、Avro、Arrow）的数据文件转换为DuckDB数据库。
"""

import tempfile
from pathlib import Path
from simtoolsz.db import special2db

def create_sample_data():
    """创建示例数据"""
    
    tmpdir = Path(tempfile.mkdtemp())
    
    # 创建TSV文件 - 用户数据
    tsv_content = """user_id\tname\tage\tcity\temail
1\tAlice\t25\tNew York\talice@example.com
2\tBob\t30\tLondon\tbob@example.com
3\tCharlie\t35\tTokyo\tcharlie@example.com
4\tDiana\t28\tParis\tdiana@example.com
5\tEve\t32\tBerlin\teve@example.com"""
    tsv_file = tmpdir / 'users.tsv'
    tsv_file.write_text(tsv_content, encoding='utf-8')
    
    print(f"示例数据已创建在: {tmpdir}")
    print(f"TSV文件: {tsv_file}")
    
    return tmpdir, tsv_file

def main():
    """主函数"""
    print("=== special2db 函数使用示例 ===\n")
    
    tmpdir, tsv_file = create_sample_data()
    
    # 示例1: 读取单个TSV文件
    print("--- 示例1: 读取单个TSV文件 ---")
    with tempfile.TemporaryDirectory() as test_dir:
        test_dir = Path(test_dir)
        db_file = test_dir / 'users.db'
        
        con = special2db(tsv_file, db_file)
        
        try:
            # 显示表信息
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"创建的表: {[table[0] for table in tables]}")
            
            # 显示数据
            if 'users' in [t[0] for t in tables]:
                count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                print(f"记录数: {count}")
                
                # 显示表结构
                columns = con.execute("DESCRIBE users").fetchall()
                print(f"列信息: {[(col[0], col[1]) for col in columns]}")
                
                # 显示前3条数据
                data = con.execute("SELECT * FROM users LIMIT 3").fetchall()
                print(f"前3条数据:")
                for row in data:
                    print(f"  {row}")
        finally:
            con.close()
    
    # 示例2: 使用自定义表名
    print("\n--- 示例2: 使用自定义表名 ---")
    with tempfile.TemporaryDirectory() as test_dir:
        test_dir = Path(test_dir)
        db_file = test_dir / 'custom_name.db'
        
        con = special2db(tsv_file, db_file, table='客户表')
        
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"创建的表: {[table[0] for table in tables]}")
            
            if '客户表' in [t[0] for t in tables]:
                count = con.execute("SELECT COUNT(*) FROM 客户表").fetchone()[0]
                print(f"客户表记录数: {count}")
                
                # 查询特定数据
                data = con.execute("SELECT * FROM 客户表 WHERE age > 30").fetchall()
                print(f"年龄大于30的客户: {data}")
        finally:
            con.close()
    
    # 示例3: 处理目录
    print("\n--- 示例3: 处理目录 ---")
    with tempfile.TemporaryDirectory() as test_dir:
        test_dir = Path(test_dir)
        
        # 创建多个TSV文件
        users_tsv = test_dir / 'users.tsv'
        users_tsv.write_text(tsv_file.read_text(), encoding='utf-8')
        
        products_tsv = test_dir / 'products.tsv'
        products_content = """product_id\tname\tprice\tcategory\tstock
101\t笔记本电脑\t8999\t电子产品\t50
102\t无线鼠标\t199\t配件\t200
103\tUSB硬盘\t599\t存储\t80
104\t显示器\t1599\t电子产品\t30
105\t键盘\t299\t配件\t150"""
        products_tsv.write_text(products_content, encoding='utf-8')
        
        db_file = test_dir / 'directory.db'
        con = special2db(test_dir, db_file)
        
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            print(f"创建的表: {[table[0] for table in tables]}")
            
            # 查询每个表
            for table_name in ['users', 'products']:
                if table_name in [t[0] for t in tables]:
                    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"{table_name}表记录数: {count}")
        finally:
            con.close()
    
    # 示例4: 使用额外的DuckDB参数
    print("\n--- 示例4: 使用额外的DuckDB参数 ---")
    with tempfile.TemporaryDirectory() as test_dir:
        test_dir = Path(test_dir)
        db_file = test_dir / 'with_params.db'
        
        # 为TSV文件指定编码和其他参数
        con = special2db(tsv_file, db_file, 
                        encoding='utf-8',
                        header=True)
        
        if con:
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                print(f"创建的表: {[table[0] for table in tables]}")
            finally:
                con.close()
    
    print("\n=== 所有示例完成 ===")
    print("special2db 函数使用示例结束")

if __name__ == "__main__":
    main()