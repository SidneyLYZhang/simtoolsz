from audioop import add
import re
import json
import polars as pl

from pathlib import Path
from functools import reduce
from typing import Any


UNIQUE_IDS = ['ISO2','ISO3',"name_short","name_zh","official_name_zh","name_official"]
codedata = Path(__file__).parent.resolve() / "country.parquet"
infodata = Path(__file__).parent.resolve() / "columns_info"

valid_trans = {
    "name_short": ["short", "short_name", "name", "names"],
    "name_zh": ["zh", "short_zh", "name_short_zh", "short_name_zh", "names_zh", "zh_name", "zh_names", "中文"],
    "name_official": ["official", "long_name", "long"],
    "official_name_zh": ["official_zh", "long_name_zh", "long_zh", "langzh", "正式中文"],
    "UNcode": ["un", "unnumeric", "M49"],
    "ISO3":["alpha_3","ISO_3","iso3","iso3166_alpha_3","ISO3166-2"],
    "ISO2":["alpha_2","ISO_2","iso2","iso3166_alpha_2","ISO3166-1"],
    "ISOnumeric": ["isocode", "baci", "unido", "ISOnum", "iso3166_num"],
    "FAOcode": ["fao", "faonumeric"],
    "EXIO3": ["exio_hybrid_3", "exio_hybrid_3_cons"],
}

def is_data_container(data: Any) -> bool:
    # 处理各种数据容器
    if hasattr(data, 'shape') and hasattr(data, 'columns'):
        # 检测 pandas/polars DataFrame
        return True
    elif isinstance(data, (list, tuple, set, dict)):
        return True
    else:
        return False

class CountryCode:
    """
    国家代码转换器类，提供各种国家代码格式之间的转换功能。
    """
    
    def __init__(
        self, 
        additional_data: pl.DataFrame|dict[str, list|pl.Series]|None = None
    ) -> None :
        """
        初始化转换器。
        
        Args:
            additional_data: 额外的数据, 用于增补用户定义数据或者其他未包含数据情况。
        """
        if additional_data is None:
            self._add_data = None
        else:
            data = pl.DataFrame(additional_data) if isinstance(additional_data, dict) else additional_data
            self._add_data = data
        self._data = pl.scan_parquet(codedata)
    
    @property
    def all_valid_class(self) -> list[str] :
        datcol = self._data.collect_schema().names()
        datcol = datcol + reduce(lambda x,y: x+y, valid_trans.values())
        if self._add_data is not None:
            datcol = datcol + self._add_data.columns
        return datcol
    
    @property
    def core_valid_class(self) -> list[str] :
        return self._data.collect_schema().names()
    
    @staticmethod
    def search_info(colname:str) -> str:
        """
        进行可转换信息的说明。
        """
        with open(infodata, "r", encoding="utf-8") as f:
            colinf = json.load(f)
        for k,v in CTN.items():
            if colname in v:
                return colinf.get(k, f"未找到关于 {colname} 的信息")
        if colname.lower() == "all":
            return ", ".join(colinf.keys())
        return colinf.get(colname, f"未找到关于 {colname} 的信息")
    
    @staticmethod
    def _guess_source(code: int|str|Iterable[str|int]) -> str|list[str] :
        """
        自动识别输入代码的格式。
        """
        def _guess_single(xc: int|str) -> str:
            try:
                int(xc)
                return "ISOnumeric"
            except ValueError:
                if len(xc) == 2:
                    return "ISO2"
                elif len(xc) == 3:
                    return "ISO3"
                else:
                    return "regex"
        
        if isinstance(code, Iterable):
            return [_guess_single(i) for i in code]
        else:
            return _guess_single(code)
    
    def _get_valid_codename(self, src:str) -> str :
        lower_case_valid_class = [et.lower() for et in self.core_valid_class]
        for k,v in valid_trans.items():
            if src.lower() in v:
                src = k
                break
        try:
            validated_para = self.core_valid_class[lower_case_valid_class.index(src.lower())]
        except ValueError:
            raise ValueError(f"无法识别的参数 {src}")
        return validated_para
    
    def get_(self, type_:str, extra:list[str]|None = None) -> pl.DataFrame :
        """
        获取指定国家代码的核心信息。
        """
        type_n = [self._get_valid_codename(type_)]
        extra_l = [self._get_valid_codename(i) for i in extra] if extra is not None else []
        oricols = list(set[str](["name_short","name_zh"] + type_n + extra_l))
        return self._data.select(pl.col(oricols)).drop_nulls().collect()
    
    def convert(
        self, code: int|str, 
        source: str = "auto", target: str = "name_zh",
        not_found: str|None = None, 
        use_regex: bool = False
    ) -> str|list[str]:
        """
        转换国家代码到指定格式。
        
        Args:
            code: 输入的国家代码（字符串或迭代器）
            source: 源格式, 默认为"auto"，即自动识别
            target: 目标格式, 默认为"name_zh"，即转换为中文通称
            not_found: 未找到时的返回值
            use_regex: 是否使用正则表达式匹配
            
        Returns:
            转换后的国家代码（字符串或列表）
            
        Raises:
            ValueError: 当目标格式不支持时
        """
        # 确认原始格式
        if source == "auto":
            src = self._guess_source(code)
        elif source in reduce(lambda x,y:x+y,valid_trans.values()):
            for k,v in valid_trans.items():
                if source in v:
                    src = k
                    break
        else :
            src = source
        
        # 确认目标格式
        if target =="name" :
            tgt = "name_short"
        elif target in reduce(lambda x,y:x+y,valid_trans.values()):
            for k,v in valid_trans.items():
                if target in v:
                    tgt = k
                    break
        else:
            tgt = target
        
        # 进行转化
        if use_regex :
            if isinstance(code, Iterable):
                return 
            else:
                return self.convert(code, src, tgt, not_found, use_regex)
        else:
            if isinstance(code, Iterable):
                return [self.convert(c, src, tgt, not_found, use_regex) for c in code]
            else:
                return self.convert(code, src, tgt, not_found, use_regex)
    
    def covert_series(self, series: Iterable[str|int]) -> pl.Series:
        """
        转换可迭代对象中的国家代码到指定格式。
        """
        return pl.Series(self.convert(series))



def country_convert(
    txt: str|Iterable[str|int], 
    src:str = "ISO3", to: str = "name_zh",
    not_found: str|None = None,
    use_regex: bool = False,
    additional_data: dict|pl.DataFrame|None = None
) -> str|List[str] :
    """
    转换各类国家代码到指定类型——快捷函数。
    
    Args:
        txt: 输入的国家代码
        src: 源格式, 默认为"ISO3"，可选自动方式'auto'
        to: 目标格式, 默认为"name_zh"
        not_found: 未找到时的返回值
        
    Returns:
        转换后的国家代码
    """
    converter = CountryCode(additional_data)
    return converter.convert(txt, source=src, target=to, not_found=not_found, use_regex=use_regex)
