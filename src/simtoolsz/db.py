from typing import Optional, Dict, List, Union
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import duckdb

__all__ = [ 'zip2db', 'special2db', 'multizip2db' ]

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

def special2db(data_path: Path, db_path: Path, 
               table: Optional[str] = None, **kwargs
) -> duckdb.DuckDBPyConnection :
    """
    将特殊格式的文件（如tsv）转换为DuckDB数据库。
    
    支持的文件格式：
    - tsv: 制表符分隔的文本文件
    - avro: Apache Avro格式文件
    - arrow: Apache Arrow格式文件

    Args:
        data_path: 包含数据文件的路径
        db_path: 输出的DuckDB数据库文件路径
        table: 表名（如果是压缩包，每个文件对应一个表）
        **kwargs: 传递给duckdb读取文件的额外参数
    
    Returns:
        duckdb连接对象
    
    Raises:
        ValueError: 当找不到支持的数据文件或文件格式不支持时
    
    Examples:
        >>> # 读取单个TSV文件
        >>> con = special2db('data/users.tsv', 'users.db')
        
        >>> # 使用自定义表名
        >>> con = special2db('data/customers.tsv', 'customers.db', table='客户表')
        
        >>> # 处理目录中的多个文件
        >>> con = special2db('data_directory', 'all_data.db')
        
        >>> # 指定编码和其他参数
        >>> con = special2db('data/data.tsv', 'output.db', encoding='utf-8', header=True)
        
        >>> # 查询数据
        >>> tables = con.execute("SHOW TABLES").fetchall()
        >>> count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        >>> con.close()
    """
    data_path = Path(data_path)
    db_path = Path(db_path)
    
    # 建立数据库连接
    con = duckdb.connect(db_path)
    
    # 获取要处理的文件列表
    if data_path.is_file():
        # 如果是单个文件，检查扩展名是否支持
        suffix = data_path.suffix.lower()
        if suffix not in ['.tsv', '.avro', '.arrow']:
            raise ValueError(f"不支持的文件格式: {suffix}。支持的格式: tsv, avro, arrow")
        data_files = [data_path]
    else:
        # 如果是目录，获取所有支持的文件
        supported_extensions = ['*.tsv', '*.avro', '*.arrow']
        data_files = []
        for ext in supported_extensions:
            data_files.extend(data_path.glob(ext))
    
    if not data_files:
        raise ValueError("未找到支持的数据文件（tsv、avro、arrow）")
    
    # 处理每个文件
    for i, data_file in enumerate(data_files):
        if not data_file.exists():
            continue
        
        # 确定表名
        if table and len(data_files) == 1:
            # 如果指定了表名且只有一个文件
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
            
            if suffix == '.tsv':
                # 读取TSV文件（制表符分隔）
                if kwargs_str:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}', delim='\\t', {kwargs_str})"
                else:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}', delim='\\t')"
            elif suffix == '.avro':
                # 读取Avro文件
                if kwargs_str:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_avro('{data_file}', {kwargs_str})"
                else:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_avro('{data_file}')"
            elif suffix == '.arrow':
                # 读取Arrow文件
                if kwargs_str:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_arrow('{data_file}', {kwargs_str})"
                else:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_arrow('{data_file}')"
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


def multizip2db(ziplist:list[Path], filenames:str|list[str], 
                db_path: Optional[Path] = None,
                table:Optional[str] = None, **kwargs
) -> duckdb.DuckDBPyConnection:
    """
    将多个压缩包中指定的文件的数据合并后转换到DuckDB数据库中。
    主要支持的数据：tsv、csv、xlsx、parquet、json。

    注意：
        1. 每个压缩包中的文件会被合并到一个表中。
        2. 如果指定了表名，所有数据将合并到该表中。
        3. 如果未指定表名，每个文件将使用其文件名（不含扩展名）作为表名。

    Args:
        ziplist: 包含压缩包路径的列表
        filenames: 要处理的文件名（支持通配符）
        table: 可选的表名，默认使用文件名（不含扩展名）
        kwargs: 传递给special2db的其他参数
    
    Returns:
        duckdb连接对象
    """
    # 确保filenames是列表
    if isinstance(filenames, str):
        filenames = [filenames]
    
    # 创建数据库连接
    if db_path is None:
        db_path = ":memory:"
    else:
        db_path = Path(db_path)
    con = duckdb.connect(db_path)
    
    try:
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            # 处理每个压缩包
            for zip_path in ziplist:
                if not zip_path.exists():
                    print(f"压缩包不存在: {zip_path}")
                    continue
                
                with ZipFile(zip_path, 'r') as zip_ref:
                    # 提取压缩包内容
                    zip_ref.extractall(tmpdir_path)
                    
                    # 查找匹配的文件
                    for filename_pattern in filenames:
                        # 支持通配符匹配
                        matched_files = list(tmpdir_path.glob(filename_pattern))
                        
                        for data_file in matched_files:
                            if not data_file.is_file():
                                continue
                            
                            # 确定表名
                            if table:
                                table_name = table
                            else:
                                table_name = data_file.stem
                            
                            # 清理表名（移除特殊字符）
                            table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
                            
                            # 根据文件扩展名选择合适的读取方式
                            suffix = data_file.suffix.lower()
                            
                            try:
                                # 构建参数字符串
                                kwargs_str = ', '.join([f"{k}='{v}'" for k, v in kwargs.items()]) if kwargs else ''
                                
                                # 构建读取查询
                                if suffix == '.csv':
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}')"
                                elif suffix == '.xlsx':
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM st_read('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM st_read('{data_file}')"
                                elif suffix == '.parquet':
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_parquet('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM read_parquet('{data_file}')"
                                elif suffix == '.json':
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_json_auto('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM read_json_auto('{data_file}')"
                                elif suffix == '.tsv':
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}', delim='\\t', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}', delim='\\t')"
                                else:
                                    continue
                                
                                # 检查表是否已存在
                                existing_tables = con.execute("SHOW TABLES").fetchall()
                                table_exists = any(table_name == t[0] for t in existing_tables)
                                
                                if table_exists:
                                    # 如果表已存在，插入数据
                                    con.execute(f"INSERT INTO {table_name} {read_query}")
                                else:
                                    # 如果表不存在，创建新表
                                    con.execute(f"CREATE TABLE {table_name} AS {read_query}")
                                
                            except Exception as e:
                                print(f"处理文件 {data_file.name} 时出错: {e}")
                                continue
                            
                            # 清理已处理的文件，避免重复处理
                            data_file.unlink(missing_ok=True)
    
    except Exception as e:
        print(f"处理压缩包时出错: {e}")
        raise
    
    return con
    