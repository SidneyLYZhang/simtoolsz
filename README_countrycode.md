# 国家代码转换模块 - CountryCode

## 简介

CountryCode 是一个功能强大的国家代码转换模块，支持多种国际组织、国际标准的国家代码之间的转换。该库收集了各种国际组织、国际标准的代码，以及各种国家组织的成员信息，并尽可能包含了加入时间等信息。

## 安装

```bash
pip install simtoolsz
```

## 核心功能

- 支持多种国家代码格式之间的转换
- 自动识别输入代码格式
- 支持批量转换
- 支持正则表达式匹配
- 支持多种数据类型输入（字符串、列表、元组、集合、字典、DataFrame、Series）

## 快速开始

### 基本使用

```python
from simtoolsz.countrycode import country_convert

# 单个国家代码转换
country_convert("CHN", src="ISO3", to="name_zh")  # 输出: "中国"
country_convert("CN", src="ISO2", to="ISO3")  # 输出: "CHN"

# 批量转换
country_convert(["CHN", "USA", "JPN"], src="ISO3", to="name_zh")  # 输出: ["中国", "美国", "日本"]

# 自动识别源格式
country_convert("CN", to="name_zh")  # 输出: "中国"
```

### 使用 CountryCode 类

```python
from simtoolsz.countrycode import CountryCode

# 初始化转换器
converter = CountryCode()

# 转换国家代码
converter.convert("CHN", source="ISO3", target="name_zh")  # 输出: "中国"

# 转换可迭代对象
converter.covert_series(["CHN", "USA", "JPN"], source="ISO3", target="name_zh")
```

## API 文档

### 核心类

#### CountryCode

国家代码转换器模块，提供各种国家代码格式之间的转换功能。

##### 初始化

```python
CountryCode(additional_data: dict|pl.DataFrame|None = None)
```

- `additional_data`：额外的数据，用于扩展国家代码信息

##### 属性

- `all_valid_class`：获取所有有效类别
- `core_valid_class`：获取核心有效类别

##### 方法

###### search_info

```python
@staticmethod
CountryCode.search_info(colname: str) -> str
```

搜索指定列名的信息说明。

- `colname`：列名
- 返回：列名的信息说明

###### convert

```python
def convert(self, code: int|str|Iterable[str|int], 
            source: str = "auto", target: str = "name_zh",
            not_found: str|None = None, 
            use_regex: bool = False) -> str|list[str]
```

转换国家代码到指定格式。

- `code`：输入的国家代码（字符串、整数或可迭代对象）
- `source`：源格式，默认为"auto"，即自动识别
- `target`：目标格式，默认为"name_zh"，即转换为中文通称
- `not_found`：未找到时的返回值
- `use_regex`：是否使用正则表达式匹配
- 返回：转换后的国家代码（字符串或列表）

###### covert_series

```python
def covert_series(self, series: Any, 
                  source: str = "auto", target: str = "name_zh",
                  not_found: str|None = None, 
                  use_regex: bool = False,
                  out_type: str = "series") -> pl.Series | pl.DataFrame | list[Any]
```

转换可迭代对象中的国家代码。

- `series`：输入的可迭代对象（字符串或整数）
- `source`：源格式，默认为"auto"，即自动识别
- `target`：目标格式，默认为"name_zh"，即转换为中文通称
- `not_found`：未找到时的返回值
- `use_regex`：是否使用正则表达式匹配
- `out_type`：输出类型，默认为"series"，即返回Series；可选"dataframe"，返回DataFrame；可选"list"，返回列表
- 返回：转换后的国家代码（Series，DataFrame或List）

###### get_

```python
def get_(self, ctype_: str, extra: list[str]|None = None) -> pl.DataFrame
```

获取指定国家代码的核心信息。

- `ctype_`：国家代码类型
- `extra`：额外的列名列表
- 返回：包含指定国家代码核心信息的DataFrame

### 核心函数

#### country_convert

```python
def country_convert(txt: str|Iterable[str|int], 
                    src: str = "ISO3", to: str = "name_zh",
                    not_found: str|None = None,
                    use_regex: bool = False,
                    additional_data: dict|pl.DataFrame|None = None) -> str|list[str]
```

转换各类国家代码到指定类型的快捷函数。

- `txt`：输入的国家代码
- `src`：源格式，默认为"ISO3"，可选自动方式"auto"
- `to`：目标格式，默认为"name_zh"
- `not_found`：未找到时的返回值
- `use_regex`：是否使用正则表达式匹配
- `additional_data`：额外的数据，用于扩展国家代码信息
- 返回：转换后的国家代码

#### is_data_container

```python
def is_data_container(data: Any) -> bool
```

检查是否为数据容器。

- `data`：要检查的数据
- 返回：如果是数据容器则返回True，否则返回False

## 支持的代码类型

1.  ISO2 (ISO 3166-1 alpha-2) - 国际标准化组织（ISO）国家代码 - 字母编码（2位），包括使用 UK/EL 指代英国/希腊的情况（但需始终转换为 GB/GR）
2.  ISO3 (ISO 3166-1 alpha-3) 国际标准化组织（ISO）国家代码 - 字母编码（3位）
3.  ISO - numeric (ISO 3166-1 numeric) 国际标准化组织（ISO）国家代码 - 数字编码
4.  UN numeric code (M.49 - follows to a large extend ISO-numeric) 联合国区域编号（M.49）
5.  A standard or short name 国家标准或简称
6.  The "official" name 国家官方名称
7.  Continent 6大洲分类
8.  [Continent_7 classification](https://ourworldindata.org/world-region-map-definitions) 7大洲分类（区分南北美洲）
9.  UN region 联合国区域
10. [EXIOBASE 1](http://exiobase.eu/) 供应链分析的最佳环境经济核算数据中的分类 2000
11. [EXIOBASE 2](http://exiobase.eu/) 供应链分析的最佳环境经济核算数据中的分类 2007
12. [EXIOBASE 3](https://zenodo.org/doi/10.5281/zenodo.3583070) 供应链分析的最佳环境经济核算数据中的分类 1995-2020
13. [WIOD](http://www.wiod.org/home) 世界输入输出数据分类
14. [Eora](http://www.worldmrio.com/) 全球供应链数据
15. [OECD](http://www.oecd.org/about/membersandpartners/list-oecd-member-countries.htm) 经济合作与发展组织成员
16. [MESSAGE](http://www.iiasa.ac.at/web/home/research/researchPrograms/Energy/MESSAGE-model-regions.en.html) 11区域分类
17. [IMAGE](https://models.pbl.nl/image/index.php/Welcome_to_IMAGE_3.0_Documentation) IMAGE模型 代码（巴黎气候协定）
18. [REMIND](https://www.pik-potsdam.de/en/institute/departments/transformation-pathways/models/remind) REMIND模型 代码
19. [UN](http://www.un.org/en/members/) 联合国
20. [EU](https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:EU_enlargements) 欧盟成员国（包括EU12, EU15, EU25, EU27, EU27_2007, EU28）
21. [CoE (Council of Europe,欧洲议会)](https://www.coe.int/en/web/portal/members-states) 成员
22. [EEA](https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:European_Economic_Area_(EEA)) 欧洲经济区成员
23. [Schengen](https://en.wikipedia.org/wiki/Schengen_Area) 申根区域
24. [Cecilia](https://www.ecologic.eu/sites/default/files/publication/2024/2715-Drummond-2014-Sectoral-Scenarios-for-a-Low-Carbon-Europe.pdf) 2050欧洲低碳愿景分类
25. [APEC](https://en.wikipedia.org/wiki/Asia-Pacific_Economic_Cooperation) 亚太经济合作组织。
26. [BRIC](https://en.wikipedia.org/wiki/BRIC) 金砖4国
27. [BASIC](https://en.wikipedia.org/wiki/BASIC_countries) 基础四国（G4发展中国家）。
28. [CIS](https://en.wikipedia.org/wiki/Commonwealth_of_Independent_States) 独立国家联合体（基于2019, excl. Turkmenistan）
29. [G7](https://en.wikipedia.org/wiki/Group_of_Seven) G7国家列表。
30. [G20](https://en.wikipedia.org/wiki/G20) G20国家列表。
31. [FAOcode](http://www.fao.org/faostat/en/#definitions) (联合国粮农组织统计数据库 国家/地区数字编码
32. [GBDcode](http://ghdx.healthdata.org/) 全球疾病负担数据，国家代码（数字）
33. [IEA](https://www.iea.org/countries) 世界能源平衡在线数据编码（2021）
34. [DACcode](https://www.oecd.org/dac/financing-sustainable-development/development-finance-standards/dacandcrscodelists.htm)
    国际发展援助委员会 数字编码
35. [ccTLD](https://en.wikipedia.org/wiki/Country_code_top-level_domain) - 国家顶级域名编码
36. [GWcode](https://www.tandfonline.com/doi/abs/10.1080/03050629908434958) - Gledisch & Ward 数字编码（Gledisch & Ward,1999；[元数据](https://www.andybeger.com/states/articles/statelists.html)）
37. CC41 - MRIOs通用分类（在所有公开的MRIO中均可找到的国家列表；MRIO投入产出表）
38. [IOC](https://en.wikipedia.org/wiki/List_of_IOC_country_codes) 国际奥委会国家或地区编码列表
39. [BACI](https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37) - BACI: 产品层面国际贸易数据库（双边贸易数据）
40. [UNIDO](https://stat.unido.org/portal/dataset/getDataset/COUNTRY_PROFILE) - 联合国工业发展组织（UNIDO）数据库代码
41. [EXIOBASE hybrid 3](https://zenodo.org/records/10148587) 分类
42. [EXIOBASE hybrid 3 consequential](https://zenodo.org/records/13926939) 分类
43. [GEOnumeric](https://ec.europa.eu/eurostat/comext/newxtweb/openNom.do) GEO地理代码（也用于Prodcom统计中）（GEO代码是欧盟统计局Eurostat用于标识地理区域（如国家、地区）的数值代码列表；Prodcom为欧盟工业产品生产统计）
44. [FIFA](https://en.wikipedia.org/wiki/List_of_FIFA_country_codes) 国际足联国家/地区代码列表。 
45. [BRICS](https://infobrics.org/en/) 金砖国家组织。
46. [ASEAN](https://asean.org/) 东南亚国家联盟。
47. [SCO](https://chn.sectsco.org/) 上海合作组织。
48. [OPEC](https://www.opec.org/) 石油输出国组织。
49. [RCEP](https://en.wikipedia.org/wiki/Regional_Comprehensive_Economic_Partnership) 区域合作经济伙伴关系（RCEP）。
50. [ISO-4217 Currency Code](https://www.iso.org/iso-4217-currency-codes.html) 国际标准化组织（ISO）货币代码（4217）基于对应国家，含货币名称。

## 高级用法

### 使用正则表达式匹配

```python
from simtoolsz.countrycode import country_convert

# 使用正则表达式匹配
country_convert("China", src="regex", to="ISO3", use_regex=True)  # 输出: "CHN"
```

### 处理未找到的情况

```python
from simtoolsz.countrycode import country_convert

# 设置未找到时的返回值
country_convert("XYZ", src="ISO3", to="name_zh", not_found="未知国家")  # 输出: "未知国家"
```

## 注意事项

1. 当使用自动识别源格式（`src="auto"`）时，会根据输入代码的长度和格式进行判断
2. 支持的输入数据类型包括：字符串、列表、元组、集合、字典、DataFrame、Series
3. 批量转换时，返回值的类型与输入值的类型相对应
4. 当使用正则表达式匹配时，可能会返回多个结果，此时会输出警告并返回第一个结果

## 许可证

本项目采用 MulanPSL-2.0 许可证。

## 更新日志

### 0.2.12

- 修复了正则表达式编译时的 None 值处理
- 修复了 __init__ 方法参数问题
- 修复了 is_data_container 函数对 Series 的处理
- 修复了未定义的 CTN 变量
- 修复了未定义的 row 变量
- 修复了 convert 方法中的参数顺序问题
- 优化了 _guess_source 方法
- 优化了 _get_valid_codename 方法
- 优化了 country_convert 函数

## 联系方式

如有问题或建议，请提交 Issue 到 GitHub 仓库。
