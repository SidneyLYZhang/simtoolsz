from typing import Iterable, Union, List, Optional
import re
import polars as pl
import pycountry as pct


class CountryCodeConverter:
    """
    国家代码转换器类，提供各种国家代码格式之间的转换功能。
    """
    
    # 支持的转换目标类型
    SUPPORTED_TARGETS = [
        'APEC', 'BASIC', 'BRIC', 'CC41', 'CIS', 'Cecilia2050', 'Continent_7',
        'DACcode', 'EEA', 'EU', 'EU12', 'EU15', 'EU25', 'EU27', 'EU27_2007',
        'EU28', 'EURO', 'EXIO1', 'EXIO1_3L', 'EXIO2', 'EXIO2_3L', 'EXIO3',
        'EXIO3_3L', 'Eora', 'FAOcode', 'G20', 'G7', 'GBDcode', 'GWcode', 'IEA',
        'IMAGE', 'IOC', 'ISO2', 'ISO3', 'ISOnumeric', 'MESSAGE', 'OECD',
        'REMIND', 'Schengen', 'UN', 'UNcode', 'UNmember', 'UNregion', 'WIOD',
        'ccTLD', 'continent', 'name_official', 'name_short', 'obsolete', 'regex',
        'alpha_2', 'alpha_3', 'numeric', 'ISO', 'name'
    ]
    
    # 特殊地区映射
    SPECIAL_TERRITORIES = {
        "zh": {
            "香港": "中国香港",
            "澳门": "中国澳门",
            "Macao": "Macao ,China",
            "Hong Kong": "Hong Kong ,China", 
            "Hongkong": "Hongkong ,China",
            "Macau": "Macau ,China"
        },
        "en": {
            "香港": "Hong Kong ,China",
            "澳门": "Macao ,China"
        }
    }
    
    # 科索沃特殊处理
    KOSOVO_CODES = ["XK", "XKS"]
    KOSOVO_NAMES = {"zh": "科索沃", "en": "Kosovo"}
    
    def __init__(self, additional_data: Optional[pl.DataFrame] = None):
        """
        初始化转换器。
        
        Args:
            additional_data: 额外的数据，用于country_converter
        """
        self.converter = coco.CountryConverter(additional_data=additional_data)
    
    def _get_localized_name(self, code: str, language: str = "zh", 
                           not_found: Optional[str] = None) -> str:
        """
        获取指定语言的国家名称。
        
        Args:
            code: ISO3国家代码
            language: 语言代码
            not_found: 未找到时的返回值
            
        Returns:
            本地化的国家名称
        """
        # 特殊地区处理
        if code in self.SPECIAL_TERRITORIES.get(language, {}):
            return self.SPECIAL_TERRITORIES[language][code]
            
        try:
            translator = gettext.translation('iso3166-1', pct.LOCALES_DIR, 
                                           languages=[language])
            translator.install()
            return translator.gettext(code)
        except:
            return code if not_found is None else not_found
    
    def convert(self, code: Union[str, Iterable[str]], to: str = "name_zh",
                not_found: Optional[str] = None, use_regex: bool = False) -> Union[str, List[str]]:
        """
        转换国家代码到指定格式。
        
        Args:
            code: 输入的国家代码（字符串或迭代器）
            to: 目标格式
            not_found: 未找到时的返回值
            use_regex: 是否使用正则表达式匹配
            
        Returns:
            转换后的国家代码（字符串或列表）
            
        Raises:
            ValueError: 当目标格式不支持时
        """
        # 验证目标格式
        if to not in self.SUPPORTED_TARGETS and not re.match(r"^name_.", to):
            raise ValueError(f"不支持的目标格式: {to}")
        
        # 处理name_xx格式
        language = None
        target_format = to
        if re.match(r"^name_.", to):
            language = to.split("_")[1]
            target_format = "ISO3"
        
        # 特殊处理科索沃
        if isinstance(code, str) and code.upper() in self.KOSOVO_CODES:
            if language:
                return self.KOSOVO_NAMES.get(language, self.KOSOVO_NAMES["en"])
            return code if not_found is None else not_found
        
        # 使用country_converter进行转换
        kwargs = {"names": code, "to": target_format, "not_found": not_found}
        if use_regex:
            kwargs["src"] = "regex"
            
        result = self.converter.convert(**kwargs)
        
        # 如果需要本地化名称
        if language:
            if isinstance(result, str):
                result = self._get_localized_name(result, language, not_found)
            else:
                result = [self._get_localized_name(name, language, not_found) 
                         for name in result]
        
        return result


def local_name(code: str, local: str = "zh", not_found: Optional[str] = None) -> str:
    """
    转换国家代码为指定语言的国家名称（兼容旧接口）。
    
    Args:
        code: 国家代码
        local: 语言代码
        not_found: 未找到时的返回值
        
    Returns:
        本地化的国家名称
    """
    converter = CountryCodeConverter()
    return converter.convert(code, f"name_{local}", not_found)


def convert_country_code(code: Union[str, Iterable[str]], to: str = "name_zh",
                        additional_data: Optional[pl.DataFrame] = None,
                        not_found: Optional[str] = None, 
                        use_regex: bool = False) -> Union[str, List[str]]:
    """
    转换各类国家代码到指定类型（兼容旧接口）。
    
    Args:
        code: 输入的国家代码
        to: 目标格式
        additional_data: 额外的数据（polars.DataFrame）
        not_found: 未找到时的返回值
        use_regex: 是否使用正则表达式匹配
        
    Returns:
        转换后的国家代码
    """
    converter = CountryCodeConverter(additional_data)
    return converter.convert(code, to, not_found, use_regex)