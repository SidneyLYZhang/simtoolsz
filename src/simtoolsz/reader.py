"""
数据文件读取模块

提供统一的数据文件读取接口，支持多种文件格式和压缩文件。

主要功能:
    - read_tsv/scan_tsv: 读取TSV文件
    - getreader: 获取适合的文件读取器
    - load_data: 统一的数据加载函数
    - read_archive: 读取压缩包中的数据文件
    - excel_sheet_names: 获取Excel文件的工作表名称
    - load_excel: 加载Excel文件

支持的文件格式:
    - CSV: 逗号分隔值文件
    - TSV: 制表符分隔值文件
    - XLSX/XLS/ODS: Excel文件
    - JSON: JSON数据文件
    - Parquet: Apache Parquet格式
    - IPC/Feather: Apache Arrow IPC格式
    - Avro: Apache Avro格式

支持的压缩格式:
    - ZIP: ZIP压缩包
    - TAR/TAR.GZ/TAR.BZ2: TAR压缩包
"""

import warnings
import re
import polars as pl

from polars.io.csv.batched_reader import BatchedCsvReader

from pathlib import Path
from typing import Optional, Callable
from zipfile import ZipFile, is_zipfile
from tarfile import TarFile, is_tarfile
from tempfile import TemporaryDirectory


__all__ = [
    "read_tsv", "scan_tsv", "getreader", "load_data", "read_archive",
    "is_archive_file", "excel_sheet_names", "load_excel"
]


def read_tsv(filepath: Path, lazy: bool = False, **kwargs
             ) -> pl.DataFrame | pl.LazyFrame:
    """
    读取TSV文件（制表符分隔值文件）。

    Args:
        filepath: TSV文件路径
        lazy: 是否使用惰性读取模式（默认False）
        **kwargs: 传递给polars读取函数的额外参数

    Returns:
        pl.DataFrame | pl.LazyFrame: 加载的数据

    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 文件为空或无效

    Examples:
        >>> df = read_tsv("data.tsv")
        >>> lazy_df = read_tsv("data.tsv", lazy=True)
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"TSV file not found: {filepath}")

    if filepath.stat().st_size == 0:
        raise ValueError(f"TSV file is empty: {filepath}")

    try:
        if lazy:
            return pl.scan_csv(
                filepath,
                separator='\t',
                quote_char=None,
                **kwargs
            )
        else:
            return pl.read_csv(
                filepath,
                separator='\t',
                quote_char=None,
                **kwargs
            )
    except Exception as e:
        raise ValueError(f"Failed to read TSV file {filepath}: {e}")


def scan_tsv(filepath: Path, **kwargs) -> pl.LazyFrame:
    """
    惰性读取TSV文件。

    Args:
        filepath: TSV文件路径
        **kwargs: 传递给polars.scan_csv的额外参数

    Returns:
        pl.LazyFrame: 惰性数据帧

    Examples:
        >>> lazy_df = scan_tsv("data.tsv")
    """
    return read_tsv(filepath, lazy=True, **kwargs)


def _validate_input(file_path: Path | str, format_type: Optional[str]) -> tuple[Path, str]:
    """
    验证并规范化输入参数。

    Args:
        file_path: 文件路径
        format_type: 可选的格式覆盖

    Returns:
        tuple: (验证后的路径, 格式字符串)

    Raises:
        TypeError: 文件路径无效
        ValueError: 格式类型无效
    """
    if not file_path:
        raise TypeError("file_path cannot be empty")

    try:
        validated_path = Path(file_path)
    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid file_path: {e}")

    if format_type:
        fmt = format_type.lower().strip()
        if not fmt:
            raise ValueError("format_type cannot be empty string")
    else:
        if not validated_path.suffix:
            raise ValueError(f"Cannot determine format from file path: {validated_path}")
        fmt = validated_path.suffix.lower().lstrip('.')

    return validated_path, fmt


def _get_reader_mapping(lazy: bool) -> dict[str, Callable]:
    """
    获取格式到读取器的映射。

    Args:
        lazy: 是否使用惰性读取器

    Returns:
        dict: 格式到读取函数的映射
    """
    if lazy:
        return {
            'csv': pl.scan_csv,
            'parquet': pl.scan_parquet,
            'ipc': pl.scan_ipc,
            'tsv': scan_tsv
        }

    return {
        'csv': pl.read_csv,
        'tsv': read_tsv,
        'xls': pl.read_excel,
        'xlsx': pl.read_excel,
        'ods': pl.read_ods,
        'json': pl.read_json,
        'parquet': pl.read_parquet,
        'ipc': pl.read_ipc,
        'avro': pl.read_avro,
    }


def _handle_focus_mode(fmt: str) -> Callable:
    """
    处理焦点模式，严格检查格式支持。

    Args:
        fmt: 格式字符串

    Returns:
        Callable: 读取器函数

    Raises:
        ValueError: 格式不支持
    """
    reader = getattr(pl, f"read_{fmt}", None)
    if reader is None:
        raise ValueError(
            f"Unsupported format '{fmt}' in focus mode. "
        )
    return reader


def _get_fallback_reader(lazy: bool) -> Callable:
    """
    获取不支持的格式的后备读取器。

    Args:
        lazy: 是否使用惰性读取器

    Returns:
        Callable: 后备读取器函数
    """
    return pl.scan_csv if lazy else pl.read_csv


def getreader(
    file_path: Path | str,
    format_type: Optional[str] = None,
    in_batch: bool = False,
    lazy: bool = False,
    focus: bool = False
) -> Callable:
    """
    根据文件扩展名或指定格式获取适当的读取器函数。

    Args:
        file_path: 文件路径
        format_type: 可选的格式覆盖（如 'csv', 'json', 'parquet'）
        in_batch: 是否批量读取模式，仅适用于CSV文件（默认False）
        lazy: 是否惰性读取模式，仅适用于csv, ipc, parquet文件（默认False）
        focus: 是否聚焦于指定格式，如果不支持则抛出异常（默认False）

    Returns:
        Callable: 指定格式的Polars读取器函数

    Raises:
        ValueError: 格式不支持且focus=True
        TypeError: 文件路径无效

    Examples:
        >>> reader = getreader("data.csv")
        >>> df = reader("data.csv")
        
        >>> reader = getreader("data.parquet", lazy=True)
        >>> lazy_df = reader("data.parquet")
    """
    _, fmt = _validate_input(file_path, format_type)

    if focus:
        return _handle_focus_mode(fmt)

    if fmt == 'csv' and in_batch:
        return pl.read_csv_batched

    reader_mapping = _get_reader_mapping(lazy)

    if fmt in reader_mapping:
        return reader_mapping[fmt]

    supported = ', '.join(sorted(reader_mapping.keys()))
    warnings.warn(
        f"Unknown format '{fmt}', falling back to {'lazy' if lazy else 'eager'} CSV reader. "
        f"Supported formats: {supported}",
        UserWarning,
        stacklevel=2
    )
    return _get_fallback_reader(lazy)


def _is_archive_file(file_path: Path) -> bool:
    """
    检查文件是否为压缩文件。

    Args:
        file_path: 文件路径

    Returns:
        bool: 如果是压缩文件返回True，否则返回False
    """
    if not file_path.is_file():
        return False
    if file_path.suffix in ('.xlsx', '.xls', '.ods'):
        return False
    return is_zipfile(file_path) or is_tarfile(file_path)


def is_archive_file(file_path: Path) -> bool:
    """
    检查文件是否为压缩文件或文件在压缩文件中。

    Args:
        file_path: 文件路径

    Returns:
        bool: 如果是压缩文件或在压缩文件中返回True
    """
    return _is_archive_file(file_path) or any(
        _is_archive_file(p) for p in file_path.parents
    )


def read_archive(file_path: str | Path,
                 filename: Optional[str] = None,
                 format_type: Optional[str] = None,
                 **kwargs
                 ) -> pl.DataFrame:
    """
    读取压缩包中的数据文件。

    支持的路径格式:
        - path/to/compressed.zip/data.csv
        - path/to/compressed.tar.gz/data.csv

    Args:
        file_path: 压缩包中数据文件的路径
        filename: 可选的文件名，如果为None则读取压缩包中的第一个文件
        format_type: 可选的格式覆盖（如 'csv', 'json', 'parquet'）
        **kwargs: 传递给读取函数的额外参数

    Returns:
        pl.DataFrame: 加载的数据

    Examples:
        >>> df = read_archive("data.zip/users.csv")
        >>> df = read_archive("data.zip", filename="users.csv")
    """
    file_path = Path(file_path)

    archive_path, target_filename = _resolve_archive_and_filename(file_path, filename)

    focus = format_type is not None
    reader = getreader(target_filename, format_type=format_type, focus=focus)

    return _read_from_archive(archive_path, target_filename, reader, **kwargs)


def _resolve_archive_and_filename(file_path: Path, filename: Optional[str]) -> tuple[Path, str]:
    """
    从文件路径解析压缩包路径和目标文件名。

    Args:
        file_path: 压缩包中数据文件的路径
        filename: 可选的文件名覆盖

    Returns:
        tuple: (压缩包路径, 目标文件名)

    Raises:
        ValueError: 文件路径不在压缩包中
    """
    if filename is not None:
        archive_path = file_path.parent
        if not _is_archive_file(archive_path):
            raise ValueError(f"Parent directory is not an archive file: {archive_path}")
        return archive_path, filename

    if _is_archive_file(file_path.parent):
        return file_path.parent, file_path.name

    if _is_archive_file(file_path):
        return file_path, _get_archive_filename(file_path)

    for parent in file_path.parents:
        if _is_archive_file(parent):
            return parent, file_path.name

    raise ValueError("file_path must be a file inside an archive file (zip or tar)")


def _get_archive_filename(archive_path: Path) -> str:
    """
    获取压缩包中的第一个数据文件名。

    Args:
        archive_path: 压缩包路径

    Returns:
        str: 第一个数据文件的文件名
    """
    if is_zipfile(archive_path):
        with ZipFile(archive_path, 'r') as zf:
            for name in zf.namelist():
                if not name.endswith('/'):
                    return name
    elif is_tarfile(archive_path):
        with TarFile(archive_path, 'r:*') as tf:
            for member in tf.getmembers():
                if member.isfile():
                    return member.name
    raise ValueError(f"Cannot determine filename from archive: {archive_path}")


def _read_from_archive(archive_path: Path, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    使用指定的读取器从压缩包中读取数据。

    Args:
        archive_path: 压缩包路径
        filename: 压缩包内的文件名
        reader: 读取器函数
        **kwargs: 传递给读取器的额外参数

    Returns:
        pl.DataFrame: 加载的数据
    """
    if is_zipfile(archive_path):
        return _read_from_zip(archive_path, filename, reader, **kwargs)
    elif is_tarfile(archive_path):
        return _read_from_tar(archive_path, filename, reader, **kwargs)
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")


def _read_from_zip(archive_path: Path, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    从ZIP压缩包中读取数据。

    Args:
        archive_path: ZIP文件路径
        filename: ZIP内的文件名
        reader: 读取器函数
        **kwargs: 传递给读取器的额外参数

    Returns:
        pl.DataFrame: 加载的数据
    """
    with ZipFile(archive_path, 'r') as zf:
        try:
            with zf.open(filename) as f:
                return reader(f, **kwargs)
        except Exception:
            return _extract_and_read_zip(zf, filename, reader, **kwargs)


def _read_from_tar(archive_path: Path, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    从TAR压缩包中读取数据。

    Args:
        archive_path: TAR文件路径
        filename: TAR内的文件名
        reader: 读取器函数
        **kwargs: 传递给读取器的额外参数

    Returns:
        pl.DataFrame: 加载的数据
    """
    with TarFile(archive_path, 'r:*') as tf:
        try:
            with tf.extractfile(filename) as f:
                return reader(f, **kwargs)
        except Exception:
            return _extract_and_read_tar(tf, filename, reader, **kwargs)


def _extract_and_read_zip(zf: ZipFile, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    从ZIP中提取文件并读取。

    Args:
        zf: ZipFile对象
        filename: 要提取和读取的文件名
        reader: 读取器函数
        **kwargs: 传递给读取器的额外参数

    Returns:
        pl.DataFrame: 加载的数据
    """
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / filename
        zf.extract(filename, tmpdir)
        return reader(tmp_path, **kwargs)


def _extract_and_read_tar(tf: TarFile, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    从TAR中提取文件并读取。

    Args:
        tf: TarFile对象
        filename: 要提取和读取的文件名
        reader: 读取器函数
        **kwargs: 传递给读取器的额外参数

    Returns:
        pl.DataFrame: 加载的数据
    """
    import tarfile
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / filename

        if hasattr(tarfile, 'data_filter'):
            tf.extract(filename, tmpdir, filter='data')
        else:
            warnings.warn(
                "Extracting may be unsafe; consider updating Python",
                UserWarning,
                stacklevel=2
            )
            tf.extract(filename, tmpdir)

        return reader(tmp_path, **kwargs)


def load_data(
    file_path: Path | str,
    format_type: Optional[str] = None,
    in_batch: bool = False,
    lazy: bool = False,
    focus: bool = False,
    transtype: pl.Expr | list[pl.Expr] | None = None,
    **kwargs
) -> pl.DataFrame | pl.LazyFrame | BatchedCsvReader:
    """
    统一的数据加载函数。

    自动识别文件格式并使用适当的读取器加载数据，支持压缩文件和类型转换。

    Args:
        file_path: 文件路径
        format_type: 可选的格式覆盖（如 'csv', 'json', 'parquet'）
        in_batch: 是否批量读取模式，仅适用于CSV文件（默认False）
        lazy: 是否惰性读取模式，仅适用于csv, ipc, parquet文件（默认False）
        focus: 是否聚焦于指定格式，如果不支持则抛出异常（默认False）
        transtype: 可选的类型转换表达式或表达式列表
        **kwargs: 传递给读取函数的额外参数

    Returns:
        pl.DataFrame | pl.LazyFrame | BatchedCsvReader: 加载的数据

    Examples:
        >>> df = load_data("data.csv")
        >>> lazy_df = load_data("data.parquet", lazy=True)
        >>> df = load_data("data.zip/users.csv")
    """
    if is_archive_file(file_path):
        df = read_archive(file_path, format_type=format_type, **kwargs)
    else:
        reader = getreader(
            file_path,
            format_type=format_type,
            in_batch=in_batch,
            lazy=lazy,
            focus=focus
        )
        df = reader(file_path, **kwargs)

    if transtype is not None:
        if isinstance(transtype, pl.Expr):
            df = df.with_columns(transtype)
        if isinstance(transtype, list):
            df = df.with_columns(*transtype)
    return df


def excel_sheet_names(
    file_path: Path | str
) -> list[str]:
    """
    获取Excel文件中所有工作表的名称。

    Args:
        file_path: Excel文件路径

    Returns:
        list[str]: 所有工作表名称的列表

    Examples:
        >>> names = excel_sheet_names("data.xlsx")
        >>> print(names)
        ['Sheet1', 'Sheet2', 'Sheet3']
    """
    with ZipFile(file_path, 'r') as zf:
        xml_content = zf.read("xl/workbook.xml").decode('utf-8')
        sheet_names = re.findall(r'<sheet name="([^"]+)"', xml_content)
    return sheet_names


def _get_excel_samecolumns_sheet(
    file_path: Path | str
) -> list[list[str]]:
    """
    获取具有相同列的Excel工作表分组。

    Args:
        file_path: Excel文件路径

    Returns:
        list[list[str]]: 具有相同列的工作表名称分组
    """
    sheet_names = excel_sheet_names(file_path)
    if len(sheet_names) == 0:
        raise ValueError("Excel file has no sheets")
    if len(sheet_names) == 1:
        return sheet_names
    sheet_cols = {
        sn: pl.read_excel(file_path, sheet_name=sn).columns
        for sn in sheet_names
    }
    grouped = {}
    for k, v in sheet_cols.items():
        key = tuple(v) if isinstance(v, list) else v
        grouped.setdefault(key, []).append(k)
    return list(grouped.values())


def load_excel(
    file_path: Path | str,
    sheet_name: str = "Sheet1",
    **kwargs
) -> pl.DataFrame:
    """
    加载Excel文件数据。

    Args:
        file_path: Excel文件路径
        sheet_name: 工作表名称（默认"Sheet1"）
            特殊值:
            - "@all": 加载所有工作表（要求所有工作表列相同）
            - "@most": 加载列数最多的工作表组
        **kwargs: 传递给polars.read_excel的额外参数

    Returns:
        pl.DataFrame: 加载的数据

    Raises:
        ValueError: 工作表不存在或工作表列不同

    Examples:
        >>> df = load_excel("data.xlsx")
        >>> df = load_excel("data.xlsx", sheet_name="Sheet2")
        >>> df = load_excel("data.xlsx", sheet_name="@all")
    """
    sheet_names = excel_sheet_names(file_path)
    sheet_parts = _get_excel_samecolumns_sheet(file_path)
    if sheet_name.lower() == "@all":
        if len(sheet_parts) != 1:
            raise ValueError("Excel file has multiple sheets with different columns")
        df = pl.concat([
            pl.read_excel(file_path, sheet_name=sn, **kwargs)
            for sn in sheet_names
        ])
        return df
    elif sheet_name.lower() == "@most":
        sheet_name = []
        for i in sheet_parts:
            if len(i) > len(sheet_name):
                sheet_name = i
        df = pl.concat([
            pl.read_excel(file_path, sheet_name=sn, **kwargs)
            for sn in sheet_name
        ])
    elif sheet_name in sheet_names:
        df = pl.read_excel(file_path, sheet_name=sheet_name, **kwargs)
    else:
        raise ValueError(f"Sheet {sheet_name} not found in Excel file")
    return df
