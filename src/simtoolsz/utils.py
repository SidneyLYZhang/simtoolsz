from typing import List, NewType, Optional, TypeVar, Union
from collections.abc import Iterable
from datetime import datetime as dt
from pathlib import Path

import pendulum as plm

T = TypeVar('T')

__all__ = [
    'take_from_list', 'Number', 'today', 'PathLike', 'FolderPaths', 'CheckResult'
]

Number = NewType('Number', int | float | complex)

# 类型别名定义
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
    
    # 根据 target 类型选择匹配策略
    if isinstance(target, str):
        # 字符串：检查双向包含关系
        return next((item for item in source 
                    if isinstance(item, str) and (item in target or target in item)), None)
    
    if isinstance(target, Iterable):
        # 可迭代对象：检查元素是否存在
        try:
            target_set = set(target)  # 优化查找效率
            return next((item for item in source if item in target_set), None)
        except TypeError:
            # 处理不可哈希的元素
            return next((item for item in source if item in target), None)
    
    # 单个值：使用相等性比较
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
        fmt: 格式化字符串，如 'YYYY-MM-DD'、'YYYY-MM-DD HH:mm:ss' 等。如果为 None，返回 pendulum.DateTime 对象
        addtime: 是否包含时间部分。True 返回日期时间，False 返回日期部分
        return_std: 是否返回标准 datetime 对象（仅当 fmt 为 None 时有效）
        
    Returns:
        - 如果 fmt 有值，返回格式化后的字符串
        - 如果 fmt 为 None 且 return_std 为 False，返回 pendulum.DateTime 对象
        - 如果 fmt 为 None 且 return_std 为 True，返回标准 datetime 对象
        
    Examples:
        >>> # 获取当前日期（不包含时间）
        >>> today()
        <DateTime object>
        
        >>> # 获取当前日期时间
        >>> today(addtime=True)
        <DateTime object>
        
        >>> # 获取标准 datetime 对象
        >>> today(return_std=True)
        <datetime object>
        
        >>> # 获取格式化的当前日期
        >>> today(fmt='YYYY-MM-DD')
        '2024-01-15'
        
        >>> # 获取格式化的当前日期时间
        >>> today(addtime=True, fmt='YYYY-MM-DD HH:mm:ss')
        '2024-01-15 14:30:45'
        
        >>> # 指定时区
        >>> today(tz='UTC', fmt='YYYY-MM-DD HH:mm:ss')
        '2024-01-15 06:30:45'
        
        >>> # 获取中文格式的日期
        >>> today(fmt='YYYY年MM月DD日')
        '2024年01月15日'
    """
    # 确定时区
    if tz is None:
        xtz = plm.local_timezone().name
    else:
        xtz = tz
    
    # 获取日期时间对象
    res = plm.now(tz=xtz) if addtime else plm.today(tz=xtz)
    
    # 如果需要格式化，直接返回格式化字符串
    if fmt:
        return res.format(fmt)
    
    # 如果需要标准 datetime 对象
    if return_std:
        return dt(
            res.year, res.month, res.day,
            res.hour, res.minute, res.second, res.microsecond,
            tzinfo=res.tzinfo
        )
    
    # 返回 pendulum DateTime 对象
    return res

def checkFolders(folders: FolderPaths, 
                 mkdir: bool = True, output: bool = False) -> CheckResult:
    """
    检查多个文件夹路径是否存在
    
    Args:
        folders: 文件夹路径 (str/Path) 或路径列表
        mkdir: 是否创建不存在的文件夹（默认创建）
        output: 是否返回检查结果（默认不输出）
    
    Returns:
        CheckResult: 当output为True时返回检查结果，为False时返回None
        - 单个路径: bool
        - 多个路径: list[bool]
    """
    # 统一转换为列表
    folder_list = [folders] if isinstance(folders, (str, Path)) else folders
    paths = [Path(folder) for folder in folder_list]
    
    # 检查存在性
    exists_flags = [path.exists() for path in paths]
    
    # 创建不存在的文件夹
    if mkdir:
        for path, exists in zip(paths, exists_flags):
            if not exists and path != Path("No Path"):
                path.mkdir(parents=True, exist_ok=True)
    
    # 返回结果
    if output:
        return exists_flags[0] if len(exists_flags) == 1 else exists_flags