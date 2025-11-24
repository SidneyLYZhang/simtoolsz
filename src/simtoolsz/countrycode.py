import re
import json
import polars as pl

from pathlib import Path
from functools import reduce
from typing import Iterable


UNIQUE_IDS = ['ISO2','ISO3',"name_short","name_zh","official_name_zh","name_official"]
codedata = Path(__file__).parent / "country.parquet"
infodata = Path(__file__).parent / "columns_info"

CTN = {
    "name" : ["name_short","name_zh","official_name_zh","name_official"],
    "ISO2" : ["ISO2","alpha_2"],
    "ISO3" : ["ISO3","alpha_3"],
    "ISOnumeric" : ["ISOnumeric","ISOnum"]
}

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
            additional_data: 额外的数据, 用于补充ISO3166的代码数据不包含的情况。
        """
        if isinstance(additional_data, dict):
            idx = list(additional_data.keys())
        elif isinstance(additional_data, pl.DataFrame):
            idx = additional_data.columns
        else:
            raise ValueError("additional_data 必须是 DataFrame 或 dict 类型")
        if not any([i in UNIQUE_IDS for i in idx]):
            raise ValueError(f"additional_data 信息必须至少包含一个唯一标识类型 {UNIQUE_IDS}")
        data = pl.DataFrame(additional_data) if isinstance(additional_data, dict) else additional_data
        self._add_data = data
        self._data = pl.scan_parquet(codedata)
    
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
    def _guess_source(code: int|str|Iterable[str|int]) -> str:
        """
        自动识别输入代码的格式。
        """
        if isinstance(code, int):
            return "ISOnumeric"
        elif isinstance(code, str):
            return "ISO2" if len(code) == 2 else "ISO3"
        elif isinstance(code, Iterable):
            if all(isinstance(i, int) for i in code if i is not None) :
                return "ISOnumeric"
            elif all(isinstance(i, str) for i in code if i is not None):
                if any([len(c) == 2 for c in code]):
                    return "ISO2"
                elif any([len(c) == 3 for c in code]):
                    return "ISO3"
                else:
                    return "regex"
            else:
                return "regex"
        else:
            raise ValueError("无法确定的代码格式，请自主指定适合的类型")
    
    def _get_data
    
    def convert(
        self, code: int|str|Iterable[str|int], 
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
        elif source in reduce(lambda x,y:x+y,CTN.values()):
            for k,v in CTN.items():
                if source in v and k != "name":
                    src = k
                    break
                elif source in v and k == "name":
                    src = source
                    break
        else :
            src = source
        
        # 确认目标格式
        if target =="name" :
            tgt = "name_short"
        elif target in reduce(lambda x,y:x+y,CTN.values()):
            for k,v in CTN.items():
                if target in v:
                    tgt = k
                    break
        
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



def convert_country_code(
    code: str|Iterable[str], 
    src:str = "ISO3", to: str = "name_zh",
    not_found: str|None = None
) -> str|List[str] :
    """
    转换各类国家代码到指定类型——快捷函数。
    
    Args:
        code: 输入的国家代码
        src: 源格式, 默认为"ISO3"
        to: 目标格式, 默认为"name_zh"
        not_found: 未找到时的返回值
        
    Returns:
        转换后的国家代码
    """
    converter = CountryCode()
    return converter.convert(code, source=src, target=to, not_found=not_found)