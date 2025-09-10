#!/usr/bin/env python3
"""测试which_format函数的示例文件"""

import pendulum as plm
from simtoolsz.datetime import DurationFormat

def test_which_format():
    """测试which_format函数的各种场景"""
    
    print("=== 测试 DurationFormat.which_format ===\n")
    
    # 测试未指定cast的情况
    print("1. 未指定cast的自动判断：")
    
    # 测试int类型
    result = DurationFormat.which_format(123)
    print(f"  int 123 -> {result}")
    assert result == DurationFormat.SECONDS
    
    # 测试float类型
    result = DurationFormat.which_format(12.5)
    print(f"  float 12.5 -> {result}")
    assert result == DurationFormat.MINUTES
    
    # 测试Duration类型
    duration = plm.duration(days=1, hours=2)
    result = DurationFormat.which_format(duration)
    print(f"  Duration {duration} -> {result}")
    assert result == DurationFormat.DURATION
    
    # 测试字符串格式
    test_cases = [
        ("123", DurationFormat.SECONDS),  # 纯数字
        ("1天", DurationFormat.CHINESE),
        ("2小时", DurationFormat.CHINESE),
        ("30分钟", DurationFormat.CHINESE),
        ("1 day", DurationFormat.ENGLISH),
        ("2.5 hours", DurationFormat.ENGLISH),
        ("30 minutes", DurationFormat.ENGLISH),
        ("01:30:45", DurationFormat.COLON),
        ("2:15", DurationFormat.COLON),
        ("P1DT2H3M4S", DurationFormat.ISO8601),
        ("invalid format", None),
    ]
    
    for value, expected in test_cases:
        result = DurationFormat.which_format(value)
        print(f"  str '{value}' -> {result}")
        assert result == expected, f"Expected {expected}, got {result}"
    
    print("\n2. 指定cast的测试：")
    
    # 测试Number类型不能作为人类可读格式
    result = DurationFormat.which_format(123, cast="chinese")
    print(f"  int 123 cast to chinese -> {result}")
    assert result is None
    
    # 测试Duration类型只能为duration
    result = DurationFormat.which_format(duration, cast="seconds")
    print(f"  Duration cast to seconds -> {result}")
    assert result is None
    
    result = DurationFormat.which_format(duration, cast="duration")
    print(f"  Duration cast to duration -> {result}")
    assert result == DurationFormat.DURATION
    
    # 测试字符串可以转换为指定时间单位
    result = DurationFormat.which_format("123", cast="minutes")
    print(f"  str '123' cast to minutes -> {result}")
    assert result == DurationFormat.MINUTES
    
    # 测试字符串不能转换为人类可读格式
    result = DurationFormat.which_format("123", cast="english")
    print(f"  str '123' cast to english -> {result}")
    assert result == DurationFormat.ENGLISH
    
    # 测试无效cast
    result = DurationFormat.which_format("123", cast="invalid")
    print(f"  str '123' cast to invalid -> {result}")
    assert result is None
    
    # 测试字符串不能转换为时间单位的情况
    result = DurationFormat.which_format("abc", cast="seconds")
    print(f"  str 'abc' cast to seconds -> {result}")
    assert result is None
    
    print("\n✅ 所有测试通过！")

if __name__ == "__main__":
    test_which_format()