# 时间格式转换功能文档

## 概述

`simtoolsz.datetime`模块提供了强大的时间格式转换功能，支持多种时间格式之间的相互转换，包括中文、英文、冒号格式、ISO8601等。

## 主要类

### TimeConversion 类

这是主要的转换类，用于在不同时间格式间进行转换。

#### 构造函数
```python
TimeConversion(value, format_type="seconds")
```

**参数说明:**
- `value`: 时间值，可以是数字、字符串或Duration对象
- `format_type`: 输入格式，可以是字符串或DurationFormat枚举

#### 支持的时间格式

| 格式名称 | 示例 | 描述 |
|---------|------|------|
| `seconds` | `3600` | 秒数 |
| `minutes` | `60` | 分钟数 |
| `hours` | `1` | 小时数 |
| `milliseconds` | `3600000` | 毫秒数 |
| `chinese` | `1小时30分钟` | 中文格式 |
| `english` | `1 hour 30 minutes` | 英文格式 |
| `colon` | `01:30:00` | 冒号格式 |
| `iso8601` | `PT1H30M` | ISO8601格式（符合国际标准） |
| `duration` | Duration对象 | polars Duration对象 |

#### 主要方法

##### convert(target_format)
将当前时间值转换为目标格式。

```python
# 示例：中文时间转秒
tc = TimeConversion("2小时30分钟", "chinese")
seconds = tc.convert("seconds")  # 返回：9000.0

# 示例：秒转中文时间
tc = TimeConversion(9000, "seconds")
chinese = tc.convert("chinese")  # 返回："2小时30分钟"
```

##### set_format(format_type)
设置输入格式。

```python
tc = TimeConversion(3600)
tc.set_format("hours")
result = tc.convert("seconds")  # 返回：3600.0
```

##### get_format()
获取当前输入格式。

## 使用示例

### 基本转换

```python
from simtoolsz.datetime import TimeConversion

# 中文格式转换
tc = TimeConversion("1天2小时30分钟45秒", "chinese")
print(tc.convert("seconds"))  # 95445.0

# 英文格式转换
tc = TimeConversion("2.5 hours 30 minutes", "english")
print(tc.convert("minutes"))  # 180.0

# 冒号格式转换
tc = TimeConversion("01:30:45.5", "colon")
print(tc.convert("seconds"))  # 5445.5

# ISO 8601 格式转换
tc = TimeConversion("P1DT2H30M45S", "iso8601")
print(tc.convert("seconds"))  # 95445.0
tc = TimeConversion(95445, "seconds")
print(tc.convert("iso8601"))  # P1DT2H30M45S
```

### 使用DurationFormat枚举

```python
from simtoolsz.datetime import TimeConversion, DurationFormat

tc = TimeConversion(3600, DurationFormat.SECONDS)

# 使用枚举进行转换
print(tc.convert(DurationFormat.MINUTES))  # 60.0
print(tc.convert(DurationFormat.HOURS))    # 1.0
print(tc.convert(DurationFormat.CHINESE))  # "1小时"
```

### 复杂格式转换

```python
# 多种格式间的相互转换
original = "2小时30分钟"
tc = TimeConversion(original, "chinese")

print(f"原始: {original}")
print(f"秒: {tc.convert('seconds')}")    # 9000.0
print(f"分钟: {tc.convert('minutes')}")  # 150.0
print(f"小时: {tc.convert('hours')}")    # 2.5
print(f"英文: {tc.convert('english')}")  # "2 hours 30 minutes"
print(f"冒号: {tc.convert('colon')}")   # "02:30:00"
print(f"ISO 8601: {tc.convert('iso8601')}")  # "PT2H30M"
```

## 支持的格式细节

### 中文格式
- 支持单位：天、小时、时、分钟、分、秒钟、秒、毫秒
- 支持小数："2.5小时30.5分钟"
- 支持组合："1天2小时30分钟45秒"

### 英文格式
- 支持单位：days, day, hours, hour, minutes, minute, seconds, second, milliseconds, millisecond
- 支持复数形式："2 hours" 和 "1 hour"
- 支持组合："2 hours 30 minutes"

### 冒号格式
- 支持格式："MM:SS" 和 "HH:MM:SS"
- 支持小数秒："01:30:45.5"

### ISO 8601 格式
- 符合 ISO 8601 标准
- 格式：`P[n]DT[n]H[n]M[n]S`
- 支持单位：天(D)、小时(H)、分钟(M)、秒(S)
- 支持小数："PT1.5H"、"PT30.5M"
- 支持组合："P1DT2H30M45S"（1天2小时30分钟45秒）
- 零值部分自动省略："PT3661.5S" → "PT1H1M1.5S"

## 错误处理

所有转换都有完善的错误处理机制，当输入格式无效时会抛出清晰的错误信息：

```python
try:
    tc = TimeConversion("invalid format", "chinese")
    result = tc.convert("seconds")
except ValueError as e:
    print(f"转换错误: {e}")
```

## 测试

运行测试验证功能：

```bash
# 运行简化测试
python tests/test_simple.py

# 运行完整测试
python tests/test_conversion.py
```

## 示例文件

查看 `examples/conversion_examples.py` 获取完整的使用示例。