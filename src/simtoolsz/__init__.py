"""
simtoolsz - 实用工具集

提供数据处理、邮件操作、时间转换、国家代码转换等常用功能模块。

模块说明:
    - db: 数据库操作工具，支持将压缩包中的数据导入DuckDB
    - mail: 邮件发送与接收工具
    - utils: 通用工具函数
    - datetime: 时间日期处理与转换
    - reader: 数据文件读取工具
    - countrycode: 国家代码转换器
"""

import importlib.metadata

import simtoolsz.db as db
import simtoolsz.mail as mail
import simtoolsz.utils as utils
import simtoolsz.datetime as datetime
import simtoolsz.reader as reader
import simtoolsz.countrycode as countrycode
import simtoolsz.math as math


try:
    __version__ = importlib.metadata.version("simtoolsz")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.2.15"

__author__ = "Sidney Zhang <zly@lyzhang.me>"

__all__ = [
    "__version__",
    "mail",
    "utils",
    "datetime",
    "db",
    "reader",
    "countrycode",
    "math",
]
