# simtoolsz

<div>
<img alt="PyPI - License" src="https://img.shields.io/pypi/l/simtoolsz">
<img alt="PyPI - Version" src="https://img.shields.io/pypi/v/simtoolsz">
<img alt="Python - Version" src="https://img.shields.io/pypi/pyversions/simtoolsz">
</div>

English | [中文](README.md)

A simple and convenient toolkit containing useful functions, classes, and methods. A streamlined refactoring of the previous [pytoolsz](https://github.com/SidneyLYZhang/pytoolsz) toolkit, keeping only the most practical functional modules.

## Features

### 🕐 Time Processing (`simtoolsz.datetime`)
- **Time Format Conversion**: Supports mutual conversion between multiple time formats (Chinese, English, ISO8601, seconds, minutes, hours, etc.)
- **Smart Format Recognition**: Automatically identifies the type of input time format
- **Enum Support**: Provides `DurationFormat` enum class for standardized time format processing
- **Time Calculation**: Supports calculation and conversion of time spans

### 📧 Email Processing (`simtoolsz.mail`)
- **Email Sending**: Supports HTML/plain text emails, attachments, CC, BCC
- **Email Receiving**: IMAP protocol email reading, supports subject search
- **Encoding Support**: UTF-7 encoding and decoding for handling internationalized emails
- **Embedded Images**: Supports embedded images in HTML emails

### 💾 Data Processing (`simtoolsz.db`)
- **Compressed Data Reading**: Directly reads CSV, Excel, Parquet, JSON data from ZIP archives to DuckDB
- **Special Format Support**: Supports database conversion of special format files like TSV, Avro, Arrow
- **Batch Processing**: Supports batch import of multiple files to database
- **Flexible Configuration**: Customizable table name mapping and import parameters

### 📖 Data Reading (`simtoolsz.reader`)
- **Multi-format Reading**: Unified interface for reading CSV, TSV, Excel, Parquet, JSON, IPC, Avro and other formats
- **Polars Integration**: High-performance data reading based on Polars
- **Smart Selection**: Automatically selects appropriate reader based on file extension
- **Lazy Loading Support**: Supports lazy loading mode for large datasets

### 🛠️ Utility Functions (`simtoolsz.utils`)
- **Date Acquisition**: `today()` function, supports timezone, formatting, standard datetime object return
- **List Operations**: `take_from_list()` intelligent list element lookup
- **Folder Operations**: `checkFolders()` batch folder checking and creation
- **File Lookup**: `lastFile()` file lookup based on time or size

## Installation

```bash
pip install simtoolsz
```

### Core Dependencies

- Python >= 3.11
- pendulum >= 3.1.0
- duckdb >= 1.4.0
- polars >= 1.0.0

## Quick Start

### Time Format Conversion
```python
from simtoolsz.datetime import TimeConversion

# Chinese time to seconds
tc = TimeConversion("1天2小时30分钟45秒", "chinese")
seconds = tc.convert("seconds")
print(f"1天2小时30分钟45秒 = {seconds}秒")

# Seconds to Chinese time
tc = TimeConversion(90061, "seconds")
chinese = tc.convert("chinese")
print(f"90061秒 = {chinese}")
```

### Send Email
```python
from simtoolsz.mail import send_email

# Send plain text email
result = send_email(
    email_account="your@qq.com",
    password="your_password",
    subject="Test Email",
    content="This is a test email",
    recipients="friend@example.com"
)

# Send HTML email with attachments
result = send_email(
    email_account="your@gmail.com",
    password="app_password",
    subject="Project Report",
    content="<h1>This Month's Work Report</h1><p>See attachment for details</p>",
    recipients=["boss@company.com", "colleague<colleague@company.com>"],
    attachments=["report.pdf", "data.xlsx"],
    html_mode=True
)
```

### Data Reading
```python
from simtoolsz.reader import getreader
import polars as pl

# Read CSV file using getreader
reader = getreader("data.csv")
df = reader("data.csv")

# Read TSV file
df = load_tsv("data.tsv")

# Lazy loading for large datasets
lazy_df = load_data("large_data.csv", lazy=True)

# Load compressed dataset
df = load_data("large_data_archive.tar.gz/data.csv")
```

### Compressed Data Import to Database
```python
from simtoolsz.db import zip2db

# Read data from ZIP file to DuckDB
con = zip2db(
    zip_file="data.zip",
    db_file="output.db",
    table={"users.csv": "users_table", "orders.xlsx": "orders_table"}
)

# Query data
tables = con.execute("SHOW TABLES").fetchall()
print(f"Imported tables: {tables}")
```

### Utility Functions
```python
from simtoolsz.utils import today, take_from_list

# Get current date and time
current = today(addtime=True)
formatted = today(fmt="YYYY-MM-DD HH:mm:ss")

# List lookup
result = take_from_list("hello", ["he", "world"])  # Returns "he"
result = take_from_list([2, 3], [1, 2, 3, 4])    # Returns 2
```
