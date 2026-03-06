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
import io
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
    "is_archive_file", "excel_sheet_names", "load_excel", "read_csv_advanced"
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


def read_csv_advanced(
    path: str | Path,
    csv_name: Optional[str] = None,
    start_marker: Optional[str] = "#-------------------------",
    end_marker: Optional[str] = None,
    encoding: str = "utf-8",
    **read_kwargs
) -> pl.DataFrame:
    """
    从 ZIP 压缩包或文件夹中读取被标记行包裹的 CSV 数据。

    匹配规则：只要行首（忽略前导空格）以标识符字符串开头即视为边界，
    例如 "#------------------------- 数据开始" 会被正确识别。
    支持不同的起始和结束标识符，标识符只需匹配行的前缀即可。

    Args:
        path: ZIP 文件路径或文件夹路径
        csv_name: ZIP 内或文件夹中的 CSV 文件名（可选，单 CSV 文件时可自动检测）
        start_marker: 起始边界标记的前缀字符串（默认 "#-------------------------"）
                      设为 None 则从文件开头读取
        end_marker: 结束边界标记的前缀字符串（默认 None，表示与 start_marker 相同）
                    设为 None 则使用 start_marker 作为结束标记
        encoding: 文件编码（默认 utf-8，中文环境常见 gbk/utf-8-sig）
        **read_kwargs: 传递给 polars.read_csv 的额外参数（如 separator, header 等）

    Returns:
        pl.DataFrame: 解析后的数据帧

    Raises:
        FileNotFoundError: 路径不存在
        ValueError: 未找到 CSV 文件或数据区域为空

    Examples:
        >>> # 从 ZIP 文件读取（默认标记）
        >>> df = read_csv_from_zip("data.zip", encoding="gbk")
        
        >>> # 从文件夹读取
        >>> df = read_csv_from_zip("./data_folder", separator="|")
        
        >>> # 使用不同的起始和结束标记
        >>> df = read_csv_from_zip("data.zip", start_marker="=== BEGIN ===", end_marker="=== END ===")
        
        >>> # 无标记，读取整个文件
        >>> df = read_csv_from_zip("data.zip", start_marker=None)
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"路径不存在: {path}")

    if end_marker is None:
        end_marker = start_marker

    content = _read_csv_content(path, csv_name, encoding)
    data_lines = _extract_data_lines(content, start_marker, end_marker)

    if not data_lines:
        raise ValueError("提取的数据区域为空，请检查标记是否正确")

    csv_content = "\n".join(data_lines)
    
    return pl.read_csv(
        io.BytesIO(csv_content.encode(encoding)),
        **read_kwargs
    )


def _read_csv_content(path: Path, csv_name: Optional[str], encoding: str) -> str:
    """
    从 ZIP 文件或文件夹中读取 CSV 文件的文本内容。

    Args:
        path: ZIP 文件路径或文件夹路径
        csv_name: CSV 文件名（可选）
        encoding: 文件编码

    Returns:
        str: CSV 文件的文本内容
    """
    if path.is_file() and path.suffix.lower() == '.zip':
        return _read_from_zip_file(path, csv_name, encoding)
    elif path.is_dir():
        return _read_from_directory(path, csv_name, encoding)
    else:
        raise ValueError(f"路径必须是 ZIP 文件或文件夹: {path}")


def _read_from_zip_file(zip_path: Path, csv_name: Optional[str], encoding: str) -> str:
    """
    从 ZIP 文件中读取 CSV 内容。

    Args:
        zip_path: ZIP 文件路径
        csv_name: CSV 文件名（可选）
        encoding: 文件编码

    Returns:
        str: CSV 文件的文本内容
    """
    with ZipFile(zip_path, "r") as zf:
        csv_name = _resolve_csv_name_zip(zf, csv_name, zip_path)
        with zf.open(csv_name) as f:
            actual_encoding = "utf-8-sig" if encoding == "utf-8" else encoding
            return f.read().decode(actual_encoding)


def _read_from_directory(dir_path: Path, csv_name: Optional[str], encoding: str) -> str:
    """
    从文件夹中读取 CSV 内容。

    Args:
        dir_path: 文件夹路径
        csv_name: CSV 文件名（可选）
        encoding: 文件编码

    Returns:
        str: CSV 文件的文本内容
    """
    csv_file = _resolve_csv_name_dir(dir_path, csv_name)
    actual_encoding = "utf-8-sig" if encoding == "utf-8" else encoding
    return csv_file.read_text(encoding=actual_encoding)


def _resolve_csv_name_zip(zf, csv_name: Optional[str], zip_path: Path) -> str:
    """
    解析 ZIP 文件中的 CSV 文件名。

    Args:
        zf: ZipFile 对象
        csv_name: 指定的 CSV 文件名（可选）
        zip_path: ZIP 文件路径（用于错误信息）

    Returns:
        str: CSV 文件名
    """
    if csv_name is not None:
        if csv_name not in zf.namelist():
            raise ValueError(f"ZIP 中不存在文件: {csv_name}")
        return csv_name

    candidates = [
        n for n in zf.namelist()
        if n.lower().endswith(".csv") and not n.startswith("__MACOSX")
    ]

    if len(candidates) == 0:
        raise ValueError(f"在 {zip_path} 中未找到 CSV 文件")
    if len(candidates) > 1:
        raise ValueError(f"存在多个 CSV 文件，请指定 csv_name: {candidates}")

    return candidates[0]


def _resolve_csv_name_dir(dir_path: Path, csv_name: Optional[str]) -> Path:
    """
    解析文件夹中的 CSV 文件路径。

    Args:
        dir_path: 文件夹路径
        csv_name: 指定的 CSV 文件名（可选）

    Returns:
        Path: CSV 文件路径
    """
    if csv_name is not None:
        csv_file = dir_path / csv_name
        if not csv_file.exists():
            raise ValueError(f"文件夹中不存在文件: {csv_name}")
        return csv_file

    candidates = [
        f for f in dir_path.iterdir()
        if f.is_file() and f.suffix.lower() == ".csv"
    ]

    if len(candidates) == 0:
        raise ValueError(f"在 {dir_path} 中未找到 CSV 文件")
    if len(candidates) > 1:
        raise ValueError(f"存在多个 CSV 文件，请指定 csv_name: {[f.name for f in candidates]}")

    return candidates[0]


def _extract_data_lines(
    content: str,
    start_marker: Optional[str],
    end_marker: Optional[str]
) -> list[str]:
    """
    从内容中提取数据行。

    Args:
        content: 文件内容
        start_marker: 起始标记（None 表示从开头开始）
        end_marker: 结束标记（None 表示到结尾结束）

    Returns:
        list[str]: 数据行列表
    """
    lines = content.splitlines()

    if start_marker is None and end_marker is None:
        return lines

    start_idx = _find_marker_index(lines, start_marker, find_first=True)
    end_idx = _find_marker_index(lines, end_marker, find_first=False, start_from=start_idx)

    if start_idx is None:
        warnings.warn(f"未找到起始标记 '{start_marker}'，将解析整个文件内容")
        return lines

    data_start = start_idx + 1
    data_end = end_idx if end_idx is not None else len(lines)

    return lines[data_start:data_end]


def _find_marker_index(
    lines: list[str],
    marker: Optional[str],
    find_first: bool = True,
    start_from: Optional[int] = None
) -> Optional[int]:
    """
    查找标记行的索引。

    Args:
        lines: 行列表
        marker: 标记字符串（匹配行的前缀）
        find_first: True 返回第一个匹配，False 返回最后一个匹配
        start_from: 开始搜索的索引

    Returns:
        Optional[int]: 标记行的索引，未找到返回 None
    """
    if marker is None:
        return None

    search_range = range(
        start_from + 1 if start_from is not None else 0,
        len(lines)
    )

    if find_first:
        for i in search_range:
            if lines[i].lstrip().startswith(marker):
                return i
        return None
    else:
        result = None
        for i in search_range:
            if lines[i].lstrip().startswith(marker):
                result = i
        return result
