"""
国家代码转换模块

提供国家代码在不同格式之间的转换功能，支持ISO2、ISO3、UN代码、FAO代码、
中文名称、英文名称等多种格式。

主要功能:
    - CountryCode: 国家代码转换器类
    - country_convert: 快捷转换函数
    - is_data_container: 判断对象是否为数据容器

支持的国家代码格式:
    - ISO2: 两位字母代码 (如 CN, US)
    - ISO3: 三位字母代码 (如 CHN, USA)
    - ISOnumeric: 数字代码 (如 156, 840)
    - UNcode: 联合国代码
    - FAOcode: FAO代码
    - name_short: 英文简称
    - name_zh: 中文简称
    - name_official: 英文正式名称
    - official_name_zh: 中文正式名称
"""

import re
import json
import polars as pl

from pathlib import Path
from functools import reduce
from typing import Any, Iterable

__all__ = ["CountryCode", "is_data_container", "country_convert"]


UNIQUE_IDS = ['ISO2', 'ISO3', "name_short", "name_zh", "official_name_zh", "name_official"]

codedata = Path(__file__).parent.resolve() / "country.parquet"
infodata = Path(__file__).parent.resolve() / "columns_info"

valid_trans = {
    "name_short": ["short", "short_name", "name", "names"],
    "name_zh": ["zh", "short_zh", "name_short_zh", "short_name_zh", "names_zh", "zh_name", "zh_names", "中文"],
    "name_official": ["official", "long_name", "long"],
    "official_name_zh": ["official_zh", "long_name_zh", "long_zh", "langzh", "正式中文"],
    "UNcode": ["un", "unnumeric", "M49"],
    "ISO3": ["alpha_3", "ISO_3", "iso3", "iso3166_alpha_3", "ISO3166-2"],
    "ISO2": ["alpha_2", "ISO_2", "iso2", "iso3166_alpha_2", "ISO3166-1"],
    "ISOnumeric": ["isocode", "baci", "unido", "ISOnum", "iso3166_num"],
    "FAOcode": ["fao", "faonumeric"],
    "EXIO3": ["exio_hybrid_3", "exio_hybrid_3_cons"],
}


def is_data_container(data: Any) -> bool:
    """
    判断对象是否为数据容器类型。

    支持的数据容器类型:
        - pandas/polars DataFrame 或 Series (通过 shape 属性判断)
        - list, tuple, set, dict

    Args:
        data: 待判断的对象

    Returns:
        bool: 如果是数据容器返回 True，否则返回 False

    Examples:
        >>> is_data_container([1, 2, 3])
        True
        >>> is_data_container("hello")
        False
    """
    if hasattr(data, 'shape'):
        return True
    elif isinstance(data, (list, tuple, set, dict)):
        return True
    else:
        return False


class CountryCode:
    """
    国家代码转换器类。

    提供各种国家代码格式之间的转换功能，支持自动识别输入格式和正则表达式匹配。

    Attributes:
        _data: 国家代码数据的 LazyFrame
        _add_data: 额外的自定义数据
        _reges: 用于正则匹配的模式列表
        _reges_ISO2: ISO2 代码的正则模式列表
        _reges_ISO3: ISO3 代码的正则模式列表

    Examples:
        >>> cc = CountryCode()
        >>> cc.convert("CHN", source="ISO3", target="name_zh")
        '中国'
        >>> cc.convert("中国", source="name_zh", target="ISO2")
        'CN'
    """

    def __init__(self, additional_data: Any = None) -> None:
        """
        初始化转换器。

        Args:
            additional_data: 额外的自定义数据，用于扩展国家代码数据库
        """
        self._data = pl.scan_parquet(codedata)
        self._add_data = additional_data

        regex_list = self._data.select(pl.col("regex")).collect()["regex"].to_list()
        self._reges = [
            re.compile(entry, re.IGNORECASE)
            for entry in regex_list if entry is not None]

        iso2_list = self._data.select(pl.col("ISO2")).collect()["ISO2"].to_list()
        self._reges_ISO2 = [
            re.compile(entry, re.IGNORECASE)
            for entry in iso2_list if entry is not None]

        iso3_list = self._data.select(pl.col("ISO3")).collect()["ISO3"].to_list()
        self._reges_ISO3 = [
            re.compile(entry, re.IGNORECASE)
            for entry in iso3_list if entry is not None]

    @property
    def all_valid_class(self) -> list[str]:
        """
        获取所有有效的代码类别名称（包括别名）。

        Returns:
            list[str]: 所有有效类别名称的列表
        """
        datcol = self._data.collect_schema().names()
        datcol = datcol + reduce(lambda x, y: x + y, valid_trans.values())
        if self._add_data is not None:
            datcol = datcol + self._add_data.columns
        return datcol

    @property
    def core_valid_class(self) -> list[str]:
        """
        获取核心代码类别名称（不包括别名）。

        Returns:
            list[str]: 核心类别名称的列表
        """
        return self._data.collect_schema().names()

    @staticmethod
    def search_info(colname: str) -> str:
        """
        查询指定代码类别的说明信息。

        Args:
            colname: 代码类别名称或别名

        Returns:
            str: 该类别的说明信息

        Examples:
            >>> CountryCode.search_info("ISO2")
            '两位字母国家代码...'
        """
        with open(infodata, "r", encoding="utf-8") as f:
            colinf = json.load(f)
        for k, v in valid_trans.items():
            if colname in v:
                return colinf.get(k, f"未找到关于 {colname} 的信息")
        if colname.lower() == "all":
            return ", ".join(colinf.keys())
        return colinf.get(colname, f"未找到关于 {colname} 的信息")

    @staticmethod
    def _guess_source(code: int | str | Iterable[str | int]) -> str | list[str]:
        """
        自动识别输入代码的格式类型。

        识别规则:
            - 整数: ISOnumeric
            - 2位字符串: ISO2
            - 3位字符串: ISO3
            - 其他字符串: regex (正则匹配)

        Args:
            code: 输入的国家代码（单个值或可迭代对象）

        Returns:
            str | list[str]: 识别出的格式类型
        """
        def _guess_single(xc: int | str) -> str:
            try:
                int(xc)
                return "ISOnumeric"
            except ValueError:
                if len(str(xc)) == 2:
                    return "ISO2"
                elif len(str(xc)) == 3:
                    return "ISO3"
                else:
                    return "regex"

        if isinstance(code, str) or isinstance(code, int):
            return _guess_single(code)
        else:
            try:
                iter(code)
                return [_guess_single(i) for i in code]
            except TypeError:
                return _guess_single(code)

    def _get_valid_codename(self, src: str) -> str:
        """
        将代码类别名称或别名转换为标准名称。

        Args:
            src: 输入的类别名称或别名

        Returns:
            str: 标准化的类别名称

        Raises:
            ValueError: 当输入无法识别时抛出
        """
        original_src = src
        lower_src = src.lower()

        for k, v in valid_trans.items():
            if lower_src in [alias.lower() for alias in v]:
                src = k
                break

        if src in valid_trans or src in self.core_valid_class:
            return src

        if original_src in self.core_valid_class:
            return original_src

        lower_case_valid_class = [et.lower() for et in self.core_valid_class]
        if lower_src in lower_case_valid_class:
            return self.core_valid_class[lower_case_valid_class.index(lower_src)]

        for k in valid_trans:
            if lower_src == k.lower():
                return k

        raise ValueError(f"无法识别的参数 {original_src}")

    def _which_regex(self, colname: str) -> list[re.Pattern] | None:
        """
        根据列名返回对应的正则表达式列表。

        Args:
            colname: 列名

        Returns:
            list[re.Pattern] | None: 正则表达式列表，不支持的列名返回 None
        """
        if colname == "ISO2":
            return self._reges_ISO2
        elif colname == "ISO3":
            return self._reges_ISO3
        elif colname in ["regex", "name_short"]:
            return self._reges
        else:
            return None

    def _lazy_find(
        self, txt: str | int, colname: str,
        use_regex: bool = False
    ) -> pl.DataFrame:
        """
        在数据中查找匹配的记录。

        Args:
            txt: 要查找的值
            colname: 查找的列名
            use_regex: 是否使用正则表达式匹配

        Returns:
            pl.DataFrame: 匹配的记录
        """
        res = pl.DataFrame()
        if use_regex:
            clength = len(self._data.select(pl.col(colname)).collect())
            for i in range(clength):
                row = self._data.slice(i, 1).collect()
                irex = self._which_regex(colname)[i] if i < len(self._which_regex(colname)) else None
                if irex and irex.search(str(txt)):
                    res = pl.concat([res, row])
        else:
            clength = len(self._data.select(pl.col(colname)).collect())
            for i in range(clength):
                row = self._data.slice(i, 1).collect()
                if row[colname].item() == txt:
                    res = pl.concat([res, row])
        return res

    def get_(self, ctype_: str, extra: list[str] | None = None) -> pl.DataFrame:
        """
        获取指定国家代码的核心信息。

        Args:
            ctype_: 代码类别名称
            extra: 额外需要获取的列名列表

        Returns:
            pl.DataFrame: 包含指定列的数据

        Examples:
            >>> cc = CountryCode()
            >>> cc.get_("ISO2")
            # 返回包含 name_short, name_zh, ISO2 列的数据
        """
        type_n = [self._get_valid_codename(ctype_)]
        extra_l = [self._get_valid_codename(i) for i in extra] if extra is not None else []
        oricols = list(set[str](["name_short", "name_zh"] + type_n + extra_l))
        return self._data.select(pl.col(oricols)).drop_nulls().collect()

    def convert(
        self, code: int | str | Iterable[str | int],
        source: str = "auto", target: str = "name_zh",
        not_found: str | None = None,
        use_regex: bool = False
    ) -> str | list[str]:
        """
        转换国家代码到指定格式。

        Args:
            code: 输入的国家代码（字符串、整数或可迭代对象）
            source: 源格式，默认为"auto"即自动识别
            target: 目标格式，默认为"name_zh"即转换为中文通称
            not_found: 未找到时的返回值
            use_regex: 是否使用正则表达式匹配

        Returns:
            str | list[str]: 转换后的国家代码

        Raises:
            ValueError: 当目标格式不支持时

        Examples:
            >>> cc = CountryCode()
            >>> cc.convert("CHN", source="ISO3", target="name_zh")
            '中国'
            >>> cc.convert(["CHN", "USA"], source="ISO3", target="ISO2")
            ['CN', 'US']
        """
        is_single = False
        if isinstance(code, (str, int)):
            is_single = True
            code_list = [code]
        else:
            code_list = list(code)

        if target == "name":
            tgt = "name_short"
        elif target in reduce(lambda x, y: x + y, valid_trans.values()):
            for k, v in valid_trans.items():
                if target in v:
                    tgt = k
                    break
        else:
            tgt = target

        results = []
        for single_code in code_list:
            if source == "auto":
                src = self._guess_source(single_code)
            elif source in reduce(lambda x, y: x + y, valid_trans.values()):
                for k, v in valid_trans.items():
                    if source in v:
                        src = k
                        break
            else:
                src = source

            if use_regex:
                res = self._lazy_find(single_code, 'regex', use_regex)
            else:
                res = self._lazy_find(single_code, src, use_regex)

            if len(res) == 0:
                results.append(not_found)
            else:
                if len(res) > 1:
                    print(f"警告：输入 {single_code} 对应多个国家代码，仅返回第一个结果")
                results.append(res[tgt].to_list()[0])

        if is_single:
            return results[0]
        else:
            return results

    def covert_series(
        self, series: Any,
        source: str = "auto", target: str = "name_zh",
        not_found: str | None = None,
        use_regex: bool = False,
        out_type: str = "series"
    ) -> pl.Series | pl.DataFrame | list[Any]:
        """
        转换可迭代对象中的国家代码。

        Args:
            series: 输入的可迭代对象（字符串或整数）
            source: 源格式，默认为"auto"即自动识别
            target: 目标格式，默认为"name_zh"即转换为中文通称
            not_found: 未找到时的返回值
            use_regex: 是否使用正则表达式匹配
            out_type: 输出类型，可选 "series", "dataframe", "list"

        Returns:
            pl.Series | pl.DataFrame | list[Any]: 转换后的数据

        Raises:
            ValueError: 当 out_type 不支持时
        """
        res_list = [
            self.convert(i, source, target, not_found, use_regex) for i in series
        ]
        if out_type == "series":
            return pl.Series(name=target, values=res_list)
        elif out_type == "dataframe":
            return pl.DataFrame({
                source: series,
                target: res_list
            })
        elif out_type == "list":
            return res_list
        else:
            raise ValueError(f"out_type {out_type} 不支持")


def country_convert(
    txt: str | Iterable[str | int],
    src: str = "ISO3", to: str = "name_zh",
    not_found: str | None = None,
    use_regex: bool = False,
    additional_data: dict | pl.DataFrame | None = None
) -> str | list[str]:
    """
    转换各类国家代码到指定类型的快捷函数。

    支持多种输入类型:
        - 单个字符串或整数
        - 列表、元组、集合
        - polars Series
        - pandas/polars DataFrame
        - 字典

    Args:
        txt: 输入的国家代码
        src: 源格式，默认为"ISO3"，可选自动方式'auto'
        to: 目标格式，默认为"name_zh"
        not_found: 未找到时的返回值
        use_regex: 是否使用正则表达式匹配
        additional_data: 额外的自定义数据

    Returns:
        str | list[str]: 转换后的国家代码

    Examples:
        >>> country_convert("CHN")
        '中国'
        >>> country_convert(["CHN", "USA"])
        ['中国', '美国']
        >>> country_convert("中国", src="name_zh", to="ISO2")
        'CN'
    """
    converter = CountryCode(additional_data)
    if is_data_container(txt):
        if hasattr(txt, 'shape') and hasattr(txt, 'columns'):
            return converter.covert_series(
                txt[src], source=src, target=to,
                not_found=not_found, use_regex=use_regex, out_type="list")
        elif isinstance(txt, dict):
            temp = txt[src]
            if isinstance(temp, (list, tuple, set, pl.Series)):
                return converter.covert_series(
                    temp,
                    source=src, target=to,
                    not_found=not_found, use_regex=use_regex, out_type="list")
            elif isinstance(temp, (str, int)):
                return converter.convert(
                    temp, source=src, target=to,
                    not_found=not_found, use_regex=use_regex)
        elif isinstance(txt, pl.Series):
            return converter.covert_series(
                txt, source=src, target=to,
                not_found=not_found, use_regex=use_regex, out_type="list")
        elif isinstance(txt, (list, tuple, set)):
            return converter.covert_series(
                txt, source=src, target=to,
                not_found=not_found, use_regex=use_regex, out_type="list")
    return converter.convert(txt, source=src, target=to, not_found=not_found, use_regex=use_regex)
