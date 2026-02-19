"""
工具函数模块

提供常用的工具函数，包括列表查找、日期时间处理、文件夹检查和文件查找等功能。

主要功能:
    - take_from_list: 从列表中查找匹配元素
    - today/yesterday: 获取当前/昨日日期时间
    - checkFolders: 检查并创建文件夹
    - lastFile: 获取文件夹中最后一个文件

类型定义:
    - Number: 数值类型（int | float | complex）
    - PathLike: 路径类型（str | Path）
    - FolderPaths: 文件夹路径类型（单个或列表）
    - CheckResult: 检查结果类型（bool | list[bool] | None）
"""

from typing import List, NewType, Optional, TypeVar, Union
from collections.abc import Iterable
from datetime import datetime as dt
from pathlib import Path
import sys

import pendulum as plm

T = TypeVar('T')

__all__ = [
    'take_from_list', 'Number', 'today', 'PathLike', 'FolderPaths', 'CheckResult',
    'checkFolders', 'lastFile', 'yesterday'
]

Number = NewType('Number', int | float | complex)

PathLike = Union[str, Path]
FolderPaths = Union[PathLike, list[PathLike]]
CheckResult = Union[bool, list[bool], None]


def take_from_list(target: T, source: List[T]) -> Optional[T]:
    """
    从列表中查找并返回第一个匹配的元素。

    根据 target 的类型采用不同的匹配策略：
    - 如果是字符串，检查是否包含关系（双向）
    - 如果是可迭代对象（如列表、元组），检查 source 中的元素是否存在于 target 中
    - 其他类型使用相等性比较

    Args:
        target: 要查找的目标值或可迭代对象
        source: 要搜索的源列表

    Returns:
        找到的第一个匹配元素，如果未找到则返回 None

    Examples:
        >>> take_from_list(3, [1, 2, 3, 4])
        3
        >>> take_from_list([2, 3], [1, 2, 3, 4])
        2
        >>> take_from_list("hello", ["he", "world"])
        "he"
    """
    if not source:
        return None

    if isinstance(target, str):
        return next((item for item in source
                    if isinstance(item, str) and (item in target or target in item)), None)

    if isinstance(target, Iterable):
        try:
            target_set = set(target)
            return next((item for item in source if item in target_set), None)
        except TypeError:
            return next((item for item in source if item in target), None)

    return next((item for item in source if item == target), None)


def today(tz: Optional[str] = None,
          fmt: Optional[str] = None,
          addtime: bool = False,
          return_std: bool = False
) -> Union[str, plm.DateTime, dt]:
    """
    获取当前日期或日期时间。

    根据参数返回当前日期或日期时间，支持指定时区和格式化字符串。

    Args:
        tz: 时区名称，如 'Asia/Shanghai'、'UTC' 等。如果为 None，使用本地时区
        fmt: 格式化字符串，如 'YYYY-MM-DD'、'YYYY-MM-DD HH:mm:ss' 等。
             如果为 None，返回 pendulum.DateTime 对象
        addtime: 是否包含时间部分。True 返回日期时间，False 返回日期部分
        return_std: 是否返回标准 datetime 对象（仅当 fmt 为 None 时有效）

    Returns:
        - 如果 fmt 有值，返回格式化后的字符串
        - 如果 fmt 为 None 且 return_std 为 False，返回 pendulum.DateTime 对象
        - 如果 fmt 为 None 且 return_std 为 True，返回标准 datetime 对象

    Examples:
        >>> today()
        <DateTime object>

        >>> today(addtime=True)
        <DateTime object>

        >>> today(return_std=True)
        <datetime object>

        >>> today(fmt='YYYY-MM-DD')
        '2024-01-15'

        >>> today(addtime=True, fmt='YYYY-MM-DD HH:mm:ss')
        '2024-01-15 14:30:45'

        >>> today(tz='UTC', fmt='YYYY-MM-DD HH:mm:ss')
        '2024-01-15 06:30:45'

        >>> today(fmt='YYYY年MM月DD日')
        '2024年01月15日'
    """
    if tz is None:
        xtz = plm.local_timezone().name
    else:
        xtz = tz

    res = plm.now(tz=xtz) if addtime else plm.today(tz=xtz)

    if fmt:
        return res.format(fmt)

    if return_std:
        return dt(
            res.year, res.month, res.day,
            res.hour, res.minute, res.second, res.microsecond,
            tzinfo=res.tzinfo
        )

    return res


def yesterday(
    tz: Optional[str] = None,
    fmt: Optional[str] = None,
    addtime: bool = False,
    return_std: bool = False
) -> Union[str, plm.DateTime, dt]:
    """
    获取昨日日期或一个自然日前的日期时间。

    根据参数返回昨日日期或一个自然日前的日期时间，支持指定时区和格式化字符串。

    Args:
        tz: 时区名称，如 'Asia/Shanghai'、'UTC' 等。如果为 None，使用本地时区
        fmt: 格式化字符串，如 'YYYY-MM-DD'、'YYYY-MM-DD HH:mm:ss' 等。
             如果为 None，返回 pendulum.DateTime 对象
        addtime: 是否包含时间部分。True 返回日期时间，False 返回日期部分
        return_std: 是否返回标准 datetime 对象（仅当 fmt 为 None 时有效）

    Returns:
        - 如果 fmt 有值，返回格式化后的字符串
        - 如果 fmt 为 None 且 return_std 为 False，返回 pendulum.DateTime 对象
        - 如果 fmt 为 None 且 return_std 为 True，返回标准 datetime 对象

    Examples:
        >>> yesterday()
        <DateTime object>

        >>> yesterday(addtime=True)
        <DateTime object>

        >>> yesterday(return_std=True)
        <datetime object>

        >>> yesterday(fmt='YYYY-MM-DD')
        '2024-01-14'
    """
    if tz is None:
        xtz = plm.local_timezone().name
    else:
        xtz = tz

    res = plm.now(tz=xtz).subtract(days=1) if addtime else plm.yesterday(tz=xtz)

    if fmt:
        return res.format(fmt)

    if return_std:
        return dt(
            res.year, res.month, res.day,
            res.hour, res.minute, res.second, res.microsecond,
            tzinfo=res.tzinfo
        )

    return res


def checkFolders(folders: FolderPaths,
                 mkdir: bool = True, output: bool = False) -> CheckResult:
    """
    检查多个文件夹路径是否存在，可选择自动创建不存在的文件夹。

    Args:
        folders: 文件夹路径 (str/Path) 或路径列表
        mkdir: 是否创建不存在的文件夹（默认创建）
        output: 是否返回检查结果（默认不输出）

    Returns:
        CheckResult: 当output为True时返回检查结果，为False时返回None
        - 单个路径: bool
        - 多个路径: list[bool]

    Examples:
        >>> checkFolders("/path/to/folder")
        None

        >>> checkFolders("/path/to/folder", output=True)
        True

        >>> checkFolders(["/path1", "/path2"], output=True)
        [True, False]
    """
    folder_list = [folders] if isinstance(folders, (str, Path)) else folders
    paths = [Path(folder) for folder in folder_list]

    exists_flags = [path.exists() for path in paths]

    if mkdir:
        for path, exists in zip(paths, exists_flags):
            if not exists and path != Path("No Path"):
                path.mkdir(parents=True, exist_ok=True)

    if output:
        return exists_flags[0] if len(exists_flags) == 1 else exists_flags


def lastFile(folder: str | Path, filename: str,
             last_: str = "mtime", mode: str = "desc") -> Path:
    """
    获取指定文件夹下的最后一个文件。

    根据指定的排序依据和排序方式，返回匹配文件名模式的文件中
    排在最后（或最前）的文件路径。

    Args:
        folder: 目标文件夹路径
        filename: 文件名或匹配模式（支持通配符，如 "*.csv"）
        last_: 排序依据，可选值：
            - "mtime": 修改时间（默认）
            - "createtime": 创建时间
            - "atime": 访问时间
            - "size": 文件大小
        mode: 排序方式，可选值：
            - "desc": 降序（默认，返回最新/最大的文件）
            - "asc": 升序（返回最旧/最小的文件）

    Returns:
        Path: 匹配条件的文件路径，若无匹配则返回 Path("No Path")

    Raises:
        ValueError: mode 或 last_ 参数值无效

    Examples:
        >>> lastFile("/data", "*.csv")
        Path('/data/latest.csv')

        >>> lastFile("/data", "*.csv", last_="size", mode="desc")
        Path('/data/largest.csv')

        >>> lastFile("/data", "*.csv", last_="createtime", mode="asc")
        Path('/data/oldest.csv')
    """
    if mode not in ["desc", "asc"]:
        raise ValueError(f"mode must be one of {['desc', 'asc']}")

    attr_map = {
        "createtime": "birthtime" if sys.version_info >= (3, 12) else "ctime",
        "mtime": "mtime",
        "atime": "atime",
        "size": "size"
    }

    if last_ not in attr_map:
        raise ValueError(f"last_ must be one of {list(attr_map.keys())}")

    folder_path = Path(folder)
    matched_files = list(folder_path.glob(filename))

    if not matched_files:
        return Path("No Path")

    attr_name = f"st_{attr_map[last_]}"

    reverse = mode == "desc"
    key_func = lambda f: getattr(f.stat(), attr_name)

    return sorted(matched_files, key=key_func, reverse=reverse)[0]
