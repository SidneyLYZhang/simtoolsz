"""
数据库操作模块

提供将压缩包中的数据文件导入DuckDB数据库的功能。

主要功能:
    - zip2db: 将zip压缩包中的数据导入DuckDB
    - special2db: 将特殊格式文件（tsv、avro、arrow）导入DuckDB
    - multizip2db: 将多个压缩包中的数据合并导入DuckDB

支持的数据格式:
    - CSV: 逗号分隔值文件
    - TSV: 制表符分隔值文件
    - XLSX: Excel文件
    - Parquet: Apache Parquet格式
    - JSON: JSON数据文件
    - Avro: Apache Avro格式
    - Arrow: Apache Arrow格式
"""

from typing import Optional, Dict, List, Union
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import duckdb

__all__ = ["zip2db", "special2db", "multizip2db"]


def zip2db(
    zip_file: Path,
    db_file: Path,
    filename: Optional[str] = None,
    table: Optional[Union[Dict[str, str], List[str], str]] = None,
    **kwargs,
) -> duckdb.DuckDBPyConnection:
    """
    读取zip压缩包中的数据文件并导入到DuckDB数据库。

    支持的数据格式: CSV、XLSX、Parquet、JSON

    Args:
        zip_file: zip压缩包文件路径
        db_file: DuckDB数据库文件路径
        filename: 指定要读取的具体文件名，如果不指定则读取所有支持的数据文件
        table: 指定表名，可以是:
               - dict: {文件名: 表名} 的映射
               - list: 与文件顺序对应的表名列表
               - str: 单个表名（仅当读取单个文件时）
        **kwargs: 传递给duckdb读取文件的额外参数

    Returns:
        duckdb.DuckDBPyConnection: DuckDB数据库连接对象

    Raises:
        ValueError: 当未找到支持的数据文件时

    Examples:
        >>> # 读取zip中所有数据文件
        >>> con = zip2db('data.zip', 'output.db')

        >>> # 读取指定文件
        >>> con = zip2db('data.zip', 'output.db', filename='users.csv')

        >>> # 指定表名映射
        >>> con = zip2db('data.zip', 'output.db', table={'users.csv': '用户表'})
    """
    with TemporaryDirectory() as tmpdir:
        with ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(tmpdir)

        tmpdir_path = Path(tmpdir)

        if filename:
            data_files = [tmpdir_path / filename]
        else:
            supported_extensions = ["*.csv", "*.xlsx", "*.parquet", "*.json"]
            data_files = []
            for ext in supported_extensions:
                data_files.extend(tmpdir_path.glob(ext))

        if not data_files:
            raise ValueError("未找到支持的数据文件")

        con = duckdb.connect(db_file)

        for i, data_file in enumerate(data_files):
            if not data_file.exists():
                continue

            if isinstance(table, dict):
                table_name = table.get(data_file.name)
                if not table_name:
                    table_name = data_file.stem
            elif isinstance(table, list):
                if i < len(table):
                    table_name = table[i]
                else:
                    table_name = data_file.stem
            elif isinstance(table, str) and len(data_files) == 1:
                table_name = table
            else:
                table_name = data_file.stem

            table_name = "".join(c for c in table_name if c.isalnum() or c == "_")

            suffix = data_file.suffix.lower()

            try:
                kwargs_str = (
                    ", ".join([f"{k}='{v}'" for k, v in kwargs.items()])
                    if kwargs
                    else ""
                )

                if suffix == ".csv":
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}')"
                elif suffix == ".xlsx":
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM st_read('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM st_read('{data_file}')"
                elif suffix == ".parquet":
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{data_file}')"
                elif suffix == ".json":
                    if kwargs_str:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{data_file}', {kwargs_str})"
                    else:
                        read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{data_file}')"
                else:
                    continue

                con.execute(f"DROP TABLE IF EXISTS {table_name}")

                con.execute(read_query.strip())

            except Exception as e:
                print(f"处理文件 {data_file.name} 时出错: {e}")
                continue

    return con


def special2db(
    data_path: Path, db_path: Path, table: Optional[str] = None, **kwargs
) -> duckdb.DuckDBPyConnection:
    """
    将特殊格式的文件转换为DuckDB数据库。

    支持的文件格式:
        - TSV: 制表符分隔的文本文件
        - Avro: Apache Avro格式文件
        - Arrow: Apache Arrow格式文件

    Args:
        data_path: 包含数据文件的路径（文件或目录）
        db_path: 输出的DuckDB数据库文件路径
        table: 表名（如果是目录，每个文件对应一个表）
        **kwargs: 传递给duckdb读取文件的额外参数

    Returns:
        duckdb.DuckDBPyConnection: DuckDB数据库连接对象

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
    """
    data_path = Path(data_path)
    db_path = Path(db_path)

    con = duckdb.connect(db_path)

    if data_path.is_file():
        suffix = data_path.suffix.lower()
        if suffix not in [".tsv", ".avro", ".arrow"]:
            raise ValueError(
                f"不支持的文件格式: {suffix}。支持的格式: tsv, avro, arrow"
            )
        data_files = [data_path]
    else:
        supported_extensions = ["*.tsv", "*.avro", "*.arrow"]
        data_files = []
        for ext in supported_extensions:
            data_files.extend(data_path.glob(ext))

    if not data_files:
        raise ValueError("未找到支持的数据文件（tsv、avro、arrow）")

    for i, data_file in enumerate(data_files):
        if not data_file.exists():
            continue

        if table and len(data_files) == 1:
            table_name = table
        else:
            table_name = data_file.stem

        table_name = "".join(c for c in table_name if c.isalnum() or c == "_")

        suffix = data_file.suffix.lower()

        try:
            kwargs_str = (
                ", ".join([f"{k}='{v}'" for k, v in kwargs.items()]) if kwargs else ""
            )

            if suffix == ".tsv":
                if kwargs_str:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}', delim='\\t', {kwargs_str})"
                else:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data_file}', delim='\\t')"
            elif suffix == ".avro":
                if kwargs_str:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_avro('{data_file}', {kwargs_str})"
                else:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_avro('{data_file}')"
            elif suffix == ".arrow":
                if kwargs_str:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_arrow('{data_file}', {kwargs_str})"
                else:
                    read_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_arrow('{data_file}')"
            else:
                continue

            con.execute(f"DROP TABLE IF EXISTS {table_name}")

            con.execute(read_query.strip())

        except Exception as e:
            print(f"处理文件 {data_file.name} 时出错: {e}")
            continue

    return con


def multizip2db(
    ziplist: list[Path],
    filenames: str | list[str],
    db_path: Optional[Path] = None,
    table: Optional[str] = None,
    **kwargs,
) -> duckdb.DuckDBPyConnection:
    """
    将多个压缩包中指定的文件的数据合并后转换到DuckDB数据库中。

    主要支持的数据格式: TSV、CSV、XLSX、Parquet、JSON

    注意:
        1. 每个压缩包中的文件会被合并到一个表中
        2. 如果指定了表名，所有数据将合并到该表中
        3. 如果未指定表名，每个文件将使用其文件名（不含扩展名）作为表名

    Args:
        ziplist: 包含压缩包路径的列表
        filenames: 要处理的文件名（支持通配符）
        db_path: DuckDB数据库文件路径，默认为内存数据库
        table: 可选的表名，默认使用文件名（不含扩展名）
        **kwargs: 传递给duckdb读取文件的额外参数

    Returns:
        duckdb.DuckDBPyConnection: DuckDB数据库连接对象

    Examples:
        >>> # 从多个zip中读取同名文件并合并
        >>> con = multizip2db(['data1.zip', 'data2.zip'], 'users.csv', 'merged.db')

        >>> # 使用通配符匹配文件
        >>> con = multizip2db(['data1.zip', 'data2.zip'], '*.csv', 'all_data.db', table='combined')
    """
    if isinstance(filenames, str):
        filenames = [filenames]

    if db_path is None:
        db_path = ":memory:"
    else:
        db_path = Path(db_path)
    con = duckdb.connect(db_path)

    try:
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            for zip_path in ziplist:
                if not zip_path.exists():
                    print(f"压缩包不存在: {zip_path}")
                    continue

                with ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdir_path)

                    for filename_pattern in filenames:
                        matched_files = list(tmpdir_path.glob(filename_pattern))

                        for data_file in matched_files:
                            if not data_file.is_file():
                                continue

                            if table:
                                table_name = table
                            else:
                                table_name = data_file.stem

                            table_name = "".join(
                                c for c in table_name if c.isalnum() or c == "_"
                            )

                            suffix = data_file.suffix.lower()

                            try:
                                kwargs_str = (
                                    ", ".join([f"{k}='{v}'" for k, v in kwargs.items()])
                                    if kwargs
                                    else ""
                                )

                                if suffix == ".csv":
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}')"
                                elif suffix == ".xlsx":
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM st_read('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = (
                                            f"SELECT * FROM st_read('{data_file}')"
                                        )
                                elif suffix == ".parquet":
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_parquet('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = (
                                            f"SELECT * FROM read_parquet('{data_file}')"
                                        )
                                elif suffix == ".json":
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_json_auto('{data_file}', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM read_json_auto('{data_file}')"
                                elif suffix == ".tsv":
                                    if kwargs_str:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}', delim='\\t', {kwargs_str})"
                                    else:
                                        read_query = f"SELECT * FROM read_csv_auto('{data_file}', delim='\\t')"
                                else:
                                    continue

                                existing_tables = con.execute("SHOW TABLES").fetchall()
                                table_exists = any(
                                    table_name == t[0] for t in existing_tables
                                )

                                if table_exists:
                                    con.execute(
                                        f"INSERT INTO {table_name} {read_query}"
                                    )
                                else:
                                    con.execute(
                                        f"CREATE TABLE {table_name} AS {read_query}"
                                    )

                            except Exception as e:
                                print(f"处理文件 {data_file.name} 时出错: {e}")
                                continue

                            data_file.unlink(missing_ok=True)

    except Exception as e:
        print(f"处理压缩包时出错: {e}")
        raise

    return con
