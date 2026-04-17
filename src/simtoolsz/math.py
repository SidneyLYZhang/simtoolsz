from numbers import Number
from decimal import Decimal, ROUND_HALF_UP
from collections.abc import Iterable
import math
import numpy as np


__all__ = ["around_right", "round"]


def _is_null_na(value) -> bool:
    if value is None:
        return True
    try:
        return np.isnan(value)
    except (TypeError, ValueError):
        return False


def _decimal_round(value: str, keep: int) -> Decimal:
    if keep > 0:
        quant = Decimal("0." + "0" * keep)
    elif keep == 0:
        quant = Decimal("1")
    else:
        quant = Decimal("1E" + str(abs(keep)))
    return Decimal(value).quantize(quant, rounding=ROUND_HALF_UP)


def around_right(
    nums: Number | None,
    keep_n: int = 2,
    null_na_handle: bool | float = True,
    precise: bool = True,
):
    """
    用于更准确的四舍五入操作。

    对于 None、NaN 等空值，可通过 null_na_handle 参数处理：
      - True（默认）：将空值转为 0.0
      - False：保留原空值返回
      - 数值：将空值转为指定数值

    通过 precise 参数启用精准四舍五入计算（基于 Decimal，默认开启）。

    keep_n 支持负数，表示四舍五入到十位、百位等：
      - keep_n=2  -> 3.1415 -> 3.14
      - keep_n=0  -> 3.6    -> 4.0
      - keep_n=-1 -> 36     -> 40.0
    """
    if _is_null_na(nums):
        if isinstance(null_na_handle, bool):
            tNum = np.float64(0.0) if null_na_handle else nums
            if _is_null_na(tNum):
                return tNum
        else:
            tNum = np.float64(null_na_handle)
    elif math.isinf(nums):
        return nums
    else:
        tNum = nums

    if precise:
        middleNum = _decimal_round(str(tNum), keep_n + 4)
        return np.float64(_decimal_round(str(middleNum), keep_n))
    else:
        middleNum = np.around(tNum, decimals=(keep_n + 4))
        return np.around(middleNum, decimals=keep_n)


def round(
    numbs: Number | Iterable,
    n: int = 2,
    null_na_handle: bool | float = True,
    precise: bool = True,
) -> float | list[float]:
    """
    批量或单值四舍五入操作。

    numbs 可以是单个数值，也可以是可迭代对象。
    n 为保留小数位数，支持负数（四舍五入到十位、百位等）。
    null_na_handle 控制空值处理方式（同 around_right）。
    precise 控制是否使用精准四舍五入（默认 True）。
    """
    if isinstance(numbs, Number) and not isinstance(numbs, bool):
        return around_right(
            numbs, keep_n=n, null_na_handle=null_na_handle, precise=precise
        )
    return [
        around_right(x, keep_n=n, null_na_handle=null_na_handle, precise=precise)
        for x in numbs
    ]
