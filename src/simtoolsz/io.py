from typing import Optional, Dict, List, Union
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import duckdb

__all__ = [ 'zip2db' ]

def zip2db(zip_file: Path, db_file: Path, 
           filename: Optional[str] = None,
           table: Optional[Union[Dict[str, str], List[str], str]] = None,
           **kwargs
) -> duckdb.DuckDBPyConnection :
    """
    读取zip中的csv、xlsx、parquet、json数据到duckdb数据库
    
    Args:
        zip_file: zip文件路径
        db_file: duckdb数据库文件路径
        filename: 指定要读取的具体文件名，如果不指定则读取所有支持的数据文件
        table: 指定表名，可以是:
               - dict: {文件名: 表名} 的映射
               - list: 与文件顺序对应的表名列表
               - str: 单个表名（仅当读取单个文件时）
        **kwargs: 传递给duckdb读取文件的额外参数
    
    Returns:
        duckdb连接对象
    """
    with TemporaryDirectory() as tmpdir:
        with ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        
        tmpdir_path = Path(tmpdir)
        
        # 获取要处理的文件列表
        if filename:
            # 如果指定了具体文件名
            data_files = [tmpdir_path / filename]
        else:
            # 获取所有支持的数据文件
            supported_extensions = ['*.csv', '*.xlsx', '*.parquet', '*.json']
            data_files = []
            for ext in supported_extensions:
                data_files.extend(tmpdir_path.glob(ext))
        
        if not data_files:
            raise ValueError("未找到支持的数据文件")
        
        # 建立数据库连接
        con = duckdb.connect(db_file)
        
        # 处理每个文件
        for i, data_file in enumerate(data_files):
            if not data_file.exists():
                continue
                
            # 确定表名
            if isinstance(table, dict):
                # 如果table是字典，按文件名查找
                table_name = table.get(data_file.name)
                if not table_name:
                    # 如果字典中没有这个文件，使用文件名（不含扩展名）
                    table_name = data_file.stem
            elif isinstance(table, list):
                # 如果table是列表，按顺序取
                if i < len(table):
                    table_name = table[i]
                else:
                    table_name = data_file.stem
            elif isinstance(table, str) and len(data_files) == 1:
                # 如果table是字符串且只有一个文件
                table_name = table
            else:
                # 默认使用文件名（不含扩展名）
                table_name = data_file.stem
            
            # 清理表名（移除特殊字符）
            table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
            
            # 根据文件扩展名选择合适的读取方式
            suffix = data_file.suffix.lower()
            
            try:
                # 构建参数字符串
                kwargs_str = ', '.join([f"{k}='{v}'" for k, v in kwargs.items()]) if kwargs else ''
                
                if suffix == '.csv':
                    # 读取CSV文件
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}')"
                elif suffix == '.xlsx':
                    # 读取Excel文件
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM st_read('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM st_read('{data_file}')"
                elif suffix == '.parquet':
                    # 读取Parquet文件
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{data_file}')"
                elif suffix == '.json':
                    # 读取JSON文件
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{data_file}')"
                else:
                    continue
                
                # 如果表已存在，先删除
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # 执行读取查询
                con.execute(read_query.strip())
                
            except Exception as e:
                print(f"处理文件 {data_file.name} 时出错: {e}")
                continue
    
    return con
