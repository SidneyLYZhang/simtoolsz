import country_converter as coco
import gettext
from typing import Iterable, Union, Optional
import re
import polars as pl
import pandas as pd

__all__ = ['convert_country_code', 'local_name', 'convert_country_code_batch']

def local_name(name: str, local: str = "zh", not_found: Optional[str] = None) -> str:
    """
    将国家名称转换为指定语言的本地化名称，处理特殊地区的中文名称。
    
    Args:
        name: 国家名称（英文）
        local: 目标语言代码（如 'zh' 表示中文）
        not_found: 当找不到对应名称时的默认值
        
    Returns:
        本地化后的国家名称
    """
    # 特殊地区的中文名称映射
    special_names_zh = {
        'Taiwan': '中国台湾省',
        'Hong Kong': '中国香港',
        'Macao': '中国澳门',
        'Macau': '中国澳门',
        'Japan': '日本',
        'Korea, Republic of': '韩国',
        'South Korea': '韩国',
        'Korea, Democratic People\'s Republic of': '朝鲜',
        'North Korea': '朝鲜'
    }
    
    # 如果目标语言是中文，使用特殊名称映射
    if local == "zh":
        if name in special_names_zh:
            return special_names_zh[name]
        
        # 扩展映射，处理更多变体
        name_lower = name.lower()
        if 'taiwan' in name_lower or 'taipei' in name_lower:
            return '中国台湾省'
        elif 'hong kong' in name_lower or 'hongkong' in name_lower:
            return '中国香港'
        elif 'macao' in name_lower or 'macau' in name_lower:
            return '中国澳门'
        elif 'japan' in name_lower:
            return '日本'
        elif 'korea' in name_lower and ('republic' in name_lower or 'south' in name_lower):
            return '韩国'
        elif 'korea' in name_lower and ('democratic' in name_lower or 'north' in name_lower):
            return '朝鲜'
    
    # 对于其他语言或不在特殊映射中的名称，使用gettext进行翻译
    try:
        # 设置gettext
        translation = gettext.translation('iso3166', languages=[local], fallback=True)
        translated = translation.gettext(name)
        
        # 如果翻译结果与原文相同且设置了not_found，则返回not_found
        if translated == name and not_found is not None:
            return not_found
            
        return translated
    except Exception:
        # 如果翻译失败，返回原文或not_found
        return not_found if not_found is not None else name


def convert_country_code_batch(codes: list[str], to: str = "name_zh",
                              additional_data: Optional[Union[pd.DataFrame, pl.DataFrame]] = None,
                              not_found: Optional[str] = None,
                              use_regex: bool = False) -> list[str]:
    """
    批量转换国家代码，提供更好的性能和错误处理。
    
    Args:
        codes: 国家代码列表
        to: 目标格式
        additional_data: 额外的数据
        not_found: 默认值
        use_regex: 是否使用正则表达式
        
    Returns:
        转换后的国家代码列表
    """
    if not codes:
        return []
    
    # 处理additional_data参数
    processed_additional_data = None
    if additional_data is not None:
        if isinstance(additional_data, pl.DataFrame):
            processed_additional_data = additional_data.to_pandas()
        elif isinstance(additional_data, pd.DataFrame):
            processed_additional_data = additional_data
        else:
            raise ValueError("additional_data必须是pandas DataFrame或polars DataFrame")
    
    # 验证目标格式
    SRCS_trans = {
        "alpha_2": "ISO2", "alpha_3": "ISO3", "numeric": "ISOnumeric",
        "ISO": "ISOnumeric", "name": "name_short"
    }
    
    if re.match(r"^name_.", to):
        langu = to.split("_")[1]
        SRCS_trans = {**SRCS_trans, **{to: "ISO3"}}
    else:
        langu = None
    
    SRCS = ['APEC', 'BASIC', 'BRIC', 'CC41', 'CIS', 'Cecilia2050', 'Continent_7',
            'DACcode', 'EEA', 'EU', 'EU12', 'EU15', 'EU25', 'EU27', 'EU27_2007',
            'EU28', 'EURO', 'EXIO1', 'EXIO1_3L', 'EXIO2', 'EXIO2_3L', 'EXIO3',
            'EXIO3_3L', 'Eora', 'FAOcode', 'G20', 'G7', 'GBDcode', 'GWcode', 'IEA',
            'IMAGE', 'IOC', 'ISO2', 'ISO3', 'ISOnumeric', 'MESSAGE', 'OECD',
            'REMIND', 'Schengen', 'UN', 'UNcode', 'UNmember', 'UNregion', 'WIOD',
            'ccTLD', 'continent', 'name_official', 'name_short', 'obsolete', 'regex']
    
    if to not in (list(SRCS_trans.keys()) + SRCS):
        raise ValueError(f"目标格式 `{to}` 不支持！")
    
    try:
        converter = coco.CountryConverter(additional_data=processed_additional_data)
        tto = SRCS_trans[to] if to in SRCS_trans.keys() else to
        kargs = {"names": codes, "to": tto, "not_found": not_found}
        
        if use_regex:
            kargs["src"] = 'regex'
        
        results = converter.convert(**kargs)
        
        # 处理语言本地化
        if langu is not None:
            results = [local_name(str(result), local=langu, not_found=not_found) 
                      for result in results]
        
        return results
        
    except Exception as e:
        if not_found is not None:
            return [not_found] * len(codes)
        else:
            raise ValueError(f"批量转换失败: {str(e)}")


def convert_country_code(code: str | Iterable, to: str = "name_zh",
                          additional_data: Optional[Union[pd.DataFrame, pl.DataFrame]] = None,
                          not_found: Optional[str] = None,
                          use_regex: bool = False) -> str | list[str]:
    """
    转换各类国家代码，到指定类型。
    
    支持多种国家代码格式之间的转换，包括ISO代码、国家名称等。
    特别优化了中文名称的转换，对台湾、香港、澳门等地区使用规范的中文名称，
    对日本、韩国、朝鲜使用简体中文常用名称。
    
    Args:
        code: 输入的国家代码或名称，可以是字符串或字符串列表
        to: 目标格式，默认为 "name_zh"（中文名称）
        additional_data: 额外的数据，可以是pandas DataFrame或polars DataFrame
        not_found: 当找不到对应国家时的默认值
        use_regex: 是否使用正则表达式匹配
        
    Returns:
        转换后的国家代码或名称，输入为字符串时返回字符串，输入为列表时返回字符串列表
        
    Raises:
        ValueError: 当指定的目标格式不支持时抛出
        
    Examples:
        >>> convert_country_code("US", to="name_zh")
        '美国'
        >>> convert_country_code(["US", "JP", "KR"], to="name_zh")
        ['美国', '日本', '韩国']
        >>> convert_country_code("Taiwan", to="name_zh")
        '中国台湾省'
        >>> convert_country_code("Hong Kong", to="name_zh")
        '中国香港'
    """
    SRCS_trans = {
         "alpha_2":"ISO2", "alpha_3":"ISO3", "numeric":"ISOnumeric",
         "ISO":"ISOnumeric", "name":"name_short"}
    if re.match(r"^name_.", to) :
        langu = to.split("_")[1]
        SRCS_trans = {**SRCS_trans, **{to:"ISO3"}}
    else:
        langu = None
    SRCS = ['APEC', 'BASIC', 'BRIC', 'CC41', 'CIS', 'Cecilia2050', 'Continent_7',
            'DACcode', 'EEA', 'EU', 'EU12', 'EU15', 'EU25', 'EU27', 'EU27_2007',
            'EU28', 'EURO', 'EXIO1', 'EXIO1_3L', 'EXIO2', 'EXIO2_3L', 'EXIO3',
            'EXIO3_3L', 'Eora', 'FAOcode', 'G20', 'G7', 'GBDcode', 'GWcode', 'IEA',
            'IMAGE', 'IOC', 'ISO2', 'ISO3', 'ISOnumeric', 'MESSAGE', 'OECD',
            'REMIND', 'Schengen', 'UN', 'UNcode', 'UNmember', 'UNregion', 'WIOD',
            'ccTLD', 'continent', 'name_official', 'name_short', 'obsolete', 'regex']
    if to not in (list(SRCS_trans.keys())+SRCS) :
        raise ValueError("This value `{}` for `to` is not supported !".format(to))
    # 处理additional_data参数，支持polars DataFrame
    processed_additional_data = None
    if additional_data is not None:
        if isinstance(additional_data, pl.DataFrame):
            # 将polars DataFrame转换为pandas DataFrame
            processed_additional_data = additional_data.to_pandas()
        elif isinstance(additional_data, pd.DataFrame):
            processed_additional_data = additional_data
        else:
            raise ValueError("additional_data必须是pandas DataFrame或polars DataFrame")
    
    # 对于列表输入，使用批量处理函数以提高性能
    if isinstance(code, list):
        return convert_country_code_batch(code, to=to, additional_data=additional_data,
                                          not_found=not_found, use_regex=use_regex)
    
    converter = coco.CountryConverter(additional_data=processed_additional_data)
    tto = SRCS_trans[to] if to in SRCS_trans.keys() else to
    kargs = { "names" : code, "to" : tto, "not_found" : not_found }
    if use_regex :
        kargs = {**kargs, **{"src":'regex'}}
    res = converter.convert(**kargs)
    if langu is not None :
        if isinstance(res, str) :
            res = local_name(res, local=langu,not_found=not_found)
        else :
            res = [local_name(x, local=langu,not_found=not_found) for x in res]
    
    # 处理特殊情况：如果结果是字符串且与输入相同，可能是转换失败
    if isinstance(res, str) and res == code and not_found is None:
        # 尝试直接进行local_name转换
        if langu is not None:
            res = local_name(code, local=langu, not_found=code)
    
    return res

if __name__ == "__main__":
    import sys
    sys.path.insert(0, 'src')

    from simtoolsz.countrycode import convert_country_code, local_name, convert_country_code_batch
    print(convert_country_code("US", to="name_zh"))
    print(convert_country_code(["US", "JP", "KR"], to="name_zh"))
    print(convert_country_code("Taiwan", to="name_zh"))
    print(convert_country_code("Hong Kong", to="name_zh"))