from typing import List, Optional, TypeVar, Any
from collections.abc import Iterable

T = TypeVar('T')

__all__ = [
    'take_from_list'
]


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