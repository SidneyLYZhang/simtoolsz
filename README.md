# simtoolsz

<div>
<img alt="PyPI - License" src="https://img.shields.io/pypi/l/simtoolsz">
<img alt="PyPI - Version" src="https://img.shields.io/pypi/v/simtoolsz">
<img alt="Python - Version" src="https://img.shields.io/pypi/pyversions/simtoolsz">
</div>

[English](README_EN.md) | 中文

一个简单、方便的工具集合，均是个人工作中的常用功能。对之前[pytoolsz](https://github.com/SidneyLYZhang/pytoolsz)工具包的精简重构，保留最实用的功能模块。

## 功能特性

### 🕐 时间处理 (`simtoolsz.datetime`)
- **时间格式转换**: 支持多种时间格式间的相互转换（中文、英文、ISO8601、秒、分钟、小时等）
- **智能格式识别**: 自动识别输入的时间格式类型
- **枚举支持**: 提供 `DurationFormat` 枚举类，标准化时间格式处理
- **时间计算**: 支持时间跨度的计算和转换

### 📧 邮件处理 (`simtoolsz.mail`)
- **邮件发送**: 支持HTML/纯文本邮件，附件、抄送、密送
- **邮件接收**: IMAP协议邮件读取，支持主题搜索
- **编码支持**: UTF-7编码解码，处理国际化邮件
- **内嵌图片**: 支持HTML邮件中的内嵌图片

### 💾 数据处理 (`simtoolsz.db`)
- **压缩包数据读取**: 直接从ZIP压缩包读取CSV、Excel、Parquet、JSON数据到DuckDB
- **特殊格式支持**: 支持TSV、Avro、Arrow等特殊格式文件的数据库转换
- **批量处理**: 支持多文件批量导入数据库
- **灵活配置**: 可自定义表名映射和导入参数

### 📖 数据读取 (`simtoolsz.reader`)
- **多格式读取**: 统一接口读取CSV、TSV、Excel、Parquet、JSON、IPC、Avro等格式
- **Polars集成**: 基于Polars的高性能数据读取
- **智能选择**: 根据文件扩展名自动选择合适的读取器
- **懒加载支持**: 支持大数据集的懒加载模式

### 🛠️ 工具函数 (`simtoolsz.utils`)
- **日期获取**: `today()` 函数，支持时区、格式化、标准datetime对象返回
- **列表操作**: `take_from_list()` 智能列表元素查找
- **文件夹操作**: `checkFolders()` 批量文件夹检查和创建
- **文件查找**: `lastFile()` 基于时间或大小的文件查找

## 安装

```bash
pip install simtoolsz
```

### 核心依赖

- Python >= 3.11
- pendulum >= 3.1.0
- duckdb >= 1.4.0
- polars >= 1.0.0

## 快速开始

### 时间格式转换
```python
from simtoolsz.datetime import TimeConversion

# 中文时间到秒
tc = TimeConversion("1天2小时30分钟45秒", "chinese")
seconds = tc.convert("seconds")
print(f"1天2小时30分钟45秒 = {seconds}秒")

# 秒到中文时间
tc = TimeConversion(90061, "seconds")
chinese = tc.convert("chinese")
print(f"90061秒 = {chinese}")
```

### 发送邮件
```python
from simtoolsz.mail import send_email

# 发送纯文本邮件
result = send_email(
    email_account="your@qq.com",
    password="your_password",
    subject="测试邮件",
    content="这是一封测试邮件",
    recipients="friend@example.com"
)

# 发送HTML邮件带附件
result = send_email(
    email_account="your@gmail.com",
    password="app_password",
    subject="项目报告",
    content="<h1>本月工作报告</h1><p>详见附件</p>",
    recipients=["boss@company.com", "同事<colleague@company.com>"],
    attachments=["report.pdf", "data.xlsx"],
    html_mode=True
)
```

### 数据读取
```python
from simtoolsz.reader import getreader
import polars as pl

# 读取CSV文件
reader = getreader("data.csv")
df = reader()

# 读取TSV文件
reader = getreader("data.tsv")
df = reader()

# 懒加载大数据集
reader = getreader("large_data.csv", lazy=True)
lazy_df = reader()
```

### 压缩包数据导入数据库
```python
from simtoolsz.db import zip2db

# 从ZIP文件读取数据到DuckDB
con = zip2db(
    zip_file="data.zip",
    db_file="output.db",
    table={"users.csv": "用户表", "orders.xlsx": "订单表"}
)

# 查询数据
tables = con.execute("SHOW TABLES").fetchall()
print(f"导入的表: {tables}")
```

### 工具函数
```python
from simtoolsz.utils import today, take_from_list

# 获取当前日期时间
current = today(addtime=True)
formatted = today(fmt="YYYY年MM月DD日 HH:mm:ss")

# 列表查找
result = take_from_list("hello", ["he", "world"])  # 返回 "he"
result = take_from_list([2, 3], [1, 2, 3, 4])    # 返回 2
```
