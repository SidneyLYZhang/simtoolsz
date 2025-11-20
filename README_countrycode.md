# 国家代码转换相关说明

这里尽可能多的收集了各种国际组织、国际标准的代码，以及各种国家组织的成员信息，并尽可能包含了加入时间等信息。

详细代码代表的含义，可以从程序中使用函数读取。这里对目前已包含的信息进行将要说明：

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
48. [SAARC](https://www.saarc-sec.org/) 南亚区域合作联盟。

