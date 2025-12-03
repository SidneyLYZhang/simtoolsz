# Country Code Conversion Library - CountryCode

## Introduction

CountryCode is a powerful country code conversion library that supports conversion between various international organization and international standard country codes. This library collects codes from various international organizations and standards, as well as member information of various country organizations, and includes joining time information as much as possible.

## Installation

```bash
pip install simtoolsz
```

## Core Features

- Supports conversion between multiple country code formats
- Automatically identifies input code format
- Supports batch conversion
- Supports regular expression matching
- Supports multiple data type inputs (string, list, tuple, set, dict, DataFrame, Series)

## Quick Start

### Basic Usage

```python
from simtoolsz.countrycode import country_convert

# Single country code conversion
country_convert("CHN", src="ISO3", to="name_zh")  # Output: "中国"
country_convert("CN", src="ISO2", to="ISO3")  # Output: "CHN"

# Batch conversion
country_convert(["CHN", "USA", "JPN"], src="ISO3", to="name_zh")  # Output: ["中国", "美国", "日本"]

# Auto-detect source format
country_convert("CN", src="auto", to="name_zh")  # Output: "中国"
```

### Using CountryCode Class

```python
from simtoolsz.countrycode import CountryCode

# Initialize converter
converter = CountryCode()

# Convert country code
converter.convert("CHN", source="ISO3", target="name_zh")  # Output: "中国"

# Convert iterable objects
converter.covert_series(["CHN", "USA", "JPN"], source="ISO3", target="name_zh")
```

## API Documentation

### Core Classes

#### CountryCode

Country code converter class that provides conversion functions between various country code formats.

##### Initialization

```python
CountryCode(additional_data: dict|pl.DataFrame|None = None)
```

- `additional_data`: Additional data used to extend country code information

##### Properties

- `all_valid_class`: Get all valid categories
- `core_valid_class`: Get core valid categories

##### Methods

###### search_info

```python
@staticmethod
CountryCode.search_info(colname: str) -> str
```

Search for information description of the specified column name.

- `colname`: Column name
- Returns: Information description of the column name

###### convert

```python
def convert(self, code: int|str|Iterable[str|int], 
            source: str = "auto", target: str = "name_zh",
            not_found: str|None = None, 
            use_regex: bool = False) -> str|list[str]
```

Convert country code to specified format.

- `code`: Input country code (string, integer or iterable object)
- `source`: Source format, default is "auto", i.e., auto-detection
- `target`: Target format, default is "name_zh", i.e., convert to Chinese common name
- `not_found`: Return value when not found
- `use_regex`: Whether to use regular expression matching
- Returns: Converted country code (string or list)

###### covert_series

```python
def covert_series(self, series: Any, 
                  source: str = "auto", target: str = "name_zh",
                  not_found: str|None = None, 
                  use_regex: bool = False,
                  out_type: str = "series") -> pl.Series | pl.DataFrame | list[Any]
```

Convert country codes in iterable objects.

- `series`: Input iterable object (string or integer)
- `source`: Source format, default is "auto", i.e., auto-detection
- `target`: Target format, default is "name_zh", i.e., convert to Chinese common name
- `not_found`: Return value when not found
- `use_regex`: Whether to use regular expression matching
- `out_type`: Output type, default is "series", i.e., return Series; optional "dataframe", return DataFrame; optional "list", return list
- Returns: Converted country codes (Series, DataFrame or List)

###### get_

```python
def get_(self, ctype_: str, extra: list[str]|None = None) -> pl.DataFrame
```

Get core information of specified country codes.

- `ctype_`: Country code type
- `extra`: Additional column name list
- Returns: DataFrame containing core information of specified country codes

### Core Functions

#### country_convert

```python
def country_convert(txt: str|Iterable[str|int], 
                    src: str = "ISO3", to: str = "name_zh",
                    not_found: str|None = None,
                    use_regex: bool = False,
                    additional_data: dict|pl.DataFrame|None = None) -> str|list[str]
```

Shortcut function to convert various country codes to specified type.

- `txt`: Input country code
- `src`: Source format, default is "ISO3", optional auto mode "auto"
- `to`: Target format, default is "name_zh"
- `not_found`: Return value when not found
- `use_regex`: Whether to use regular expression matching
- `additional_data`: Additional data used to extend country code information
- Returns: Converted country code

#### is_data_container

```python
def is_data_container(data: Any) -> bool
```

Check if it is a data container.

- `data`: Data to check
- Returns: True if it is a data container, otherwise False

## Supported Code Types

1.  ISO2 (ISO 3166-1 alpha-2) - International Organization for Standardization (ISO) country code - alphabetic code (2 digits), including the use of UK/EL to refer to the United Kingdom/Greece (but must always be converted to GB/GR)
2.  ISO3 (ISO 3166-1 alpha-3) International Organization for Standardization (ISO) country code - alphabetic code (3 digits)
3.  ISO - numeric (ISO 3166-1 numeric) International Organization for Standardization (ISO) country code - numeric code
4.  UN numeric code (M.49 - follows to a large extend ISO-numeric) United Nations regional code (M.49)
5.  A standard or short name National standard or short name
6.  The "official" name National official name
7.  Continent 6-continent classification
8.  [Continent_7 classification](https://ourworldindata.org/world-region-map-definitions) 7-continent classification (distinguishing North and South America)
9.  UN region United Nations region
10. [EXIOBASE 1](http://exiobase.eu/) Classification in the best environmental economic accounting data for supply chain analysis 2000
11. [EXIOBASE 2](http://exiobase.eu/) Classification in the best environmental economic accounting data for supply chain analysis 2007
12. [EXIOBASE 3](https://zenodo.org/doi/10.5281/zenodo.3583070) Classification in the best environmental economic accounting data for supply chain analysis 1995-2020
13. [WIOD](http://www.wiod.org/home) World Input-Output Database classification
14. [Eora](http://www.worldmrio.com/) Global supply chain data
15. [OECD](http://www.oecd.org/about/membersandpartners/list-oecd-member-countries.htm) Organization for Economic Co-operation and Development members
16. [MESSAGE](http://www.iiasa.ac.at/web/home/research/researchPrograms/Energy/MESSAGE-model-regions.en.html) 11-region classification
17. [IMAGE](https://models.pbl.nl/image/index.php/Welcome_to_IMAGE_3.0_Documentation) IMAGE model code (Paris Climate Agreement)
18. [REMIND](https://www.pik-potsdam.de/en/institute/departments/transformation-pathways/models/remind) REMIND model code
19. [UN](http://www.un.org/en/members/) United Nations
20. [EU](https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:EU_enlargements) European Union member states (including EU12, EU15, EU25, EU27, EU27_2007, EU28)
21. [CoE (Council of Europe)](https://www.coe.int/en/web/portal/members-states) members
22. [EEA](https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:European_Economic_Area_(EEA)) European Economic Area members
23. [Schengen](https://en.wikipedia.org/wiki/Schengen_Area) Schengen Area
24. [Cecilia](https://www.ecologic.eu/sites/default/files/publication/2024/2715-Drummond-2014-Sectoral-Scenarios-for-a-Low-Carbon-Europe.pdf) 2050 European Low-Carbon Vision Classification
25. [APEC](https://en.wikipedia.org/wiki/Asia-Pacific_Economic_Cooperation) Asia-Pacific Economic Cooperation.
26. [BRIC](https://en.wikipedia.org/wiki/BRIC) BRIC countries
27. [BASIC](https://en.wikipedia.org/wiki/BASIC_countries) BASIC countries (G4 developing countries).
28. [CIS](https://en.wikipedia.org/wiki/Commonwealth_of_Independent_States) Commonwealth of Independent States (based on 2019, excl. Turkmenistan)
29. [G7](https://en.wikipedia.org/wiki/Group_of_Seven) G7 country list.
30. [G20](https://en.wikipedia.org/wiki/G20) G20 country list.
31. [FAOcode](http://www.fao.org/faostat/en/#definitions) (Food and Agriculture Organization of the United Nations Statistical Database country/region numeric code
32. [GBDcode](http://ghdx.healthdata.org/) Global Burden of Disease data, country code (numeric)
33. [IEA](https://www.iea.org/countries) World Energy Balance online data code (2021)
34. [DACcode](https://www.oecd.org/dac/financing-sustainable-development/development-finance-standards/dacandcrscodelists.htm)
    International Development Assistance Committee numeric code
35. [ccTLD](https://en.wikipedia.org/wiki/Country_code_top-level_domain) - Country code top-level domain
36. [GWcode](https://www.tandfonline.com/doi/abs/10.1080/03050629908434958) - Gledisch & Ward numeric code (Gledisch & Ward,1999; [metadata](https://www.andybeger.com/states/articles/statelists.html))
37. CC41 - MRIOs common classification (country list available in all public MRIOs; MRIO input-output table)
38. [IOC](https://en.wikipedia.org/wiki/List_of_IOC_country_codes) International Olympic Committee country or region code list
39. [BACI](https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37) - BACI: Product-level international trade database (bilateral trade data)
40. [UNIDO](https://stat.unido.org/portal/dataset/getDataset/COUNTRY_PROFILE) - United Nations Industrial Development Organization (UNIDO) database code
41. [EXIOBASE hybrid 3](https://zenodo.org/records/10148587) classification
42. [EXIOBASE hybrid 3 consequential](https://zenodo.org/records/13926939) classification
43. [GEOnumeric](https://ec.europa.eu/eurostat/comext/newxtweb/openNom.do) GEO geographic code (also used in Prodcom statistics) (GEO code is a list of numeric codes used by Eurostat to identify geographic regions (such as countries, regions); Prodcom is EU industrial product production statistics)
44. [FIFA](https://en.wikipedia.org/wiki/List_of_FIFA_country_codes) International Federation of Association Football country/region code list. 
45. [BRICS](https://infobrics.org/en/) BRICS countries organization.
46. [ASEAN](https://asean.org/) Association of Southeast Asian Nations.
47. [SCO](https://chn.sectsco.org/) Shanghai Cooperation Organization.
48. [OPEC](https://www.opec.org/) Organization of the Petroleum Exporting Countries.
49. [RCEP](https://en.wikipedia.org/wiki/Regional_Comprehensive_Economic_Partnership) Regional Comprehensive Economic Partnership (RCEP).
50. [ISO-4217 Currency Code](https://www.iso.org/iso-4217-currency-codes.html) International Organization for Standardization (ISO) currency code (4217) based on corresponding country, including currency name.

## Advanced Usage

### Using Regular Expression Matching

```python
from simtoolsz.countrycode import country_convert

# Using regular expression matching
country_convert("China", src="regex", to="ISO3", use_regex=True)  # Output: "CHN"
```

### Handling Not Found Cases

```python
from simtoolsz.countrycode import country_convert

# Set return value when not found
country_convert("XYZ", src="ISO3", to="name_zh", not_found="Unknown Country")  # Output: "Unknown Country"
```

## Notes

1. When using auto-detection source format (`src="auto"`), it will judge according to the length and format of the input code
2. Supported input data types include: string, list, tuple, set, dict, DataFrame, Series
3. When batch converting, the type of return value corresponds to the type of input value
4. When using regular expression matching, multiple results may be returned, in which case a warning will be output and the first result will be returned

## Contribution

Welcome to submit Issues and Pull Requests to help improve this library.

## License

This project uses the MulanPSL-2.0 license.

## Changelog

### 0.2.12

- Fixed None value handling when compiling regular expressions
- Fixed __init__ method parameter issue
- Fixed is_data_container function handling of Series
- Fixed undefined CTN variable
- Fixed undefined row variable
- Fixed parameter order issue in convert method
- Optimized _guess_source method
- Optimized _get_valid_codename method
- Optimized country_convert function

## Contact

If you have any questions or suggestions, please submit an Issue to the GitHub repository.
