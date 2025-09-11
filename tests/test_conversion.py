#!/usr/bin/env python3
"""测试ConversionType和TimeConversion类的功能"""

import pendulum as plm
from simtoolsz.datetime import DurationFormat, TimeConversion, ConversionType

def test_conversion_type():
    """测试ConversionType类的各种转换功能"""
    print("=== 测试 ConversionType ===\n")
    
    # 测试不同输入格式的转换
    test_cases = [
        # (source_format, input_value, target_format, expected_result_type)
        (DurationFormat.SECONDS, 3600, DurationFormat.MINUTES, 60.0),
        (DurationFormat.MINUTES, 60, DurationFormat.SECONDS, 3600.0),
        (DurationFormat.HOURS, 1, DurationFormat.MILLISECONDS, 3600000.0),
        (DurationFormat.MILLISECONDS, 1000, DurationFormat.SECONDS, 1.0),
        (DurationFormat.CHINESE, "1小时30分", DurationFormat.SECONDS, 5400.0),
        (DurationFormat.ENGLISH, "2 hours 30 minutes", DurationFormat.MINUTES, 150.0),
        (DurationFormat.COLON, "01:30:45", DurationFormat.SECONDS, 5445.0),
        (DurationFormat.ISO8601, "PT1H30M", DurationFormat.MINUTES, 90.0),
    ]
    
    for source_format, input_value, target_format, expected in test_cases:
        converter = ConversionType(source_format)
        result = converter.fit(target_format)(input_value)
        print(f"  {source_format.value} -> {target_format.value}: {input_value} -> {result}")
        assert abs(result - expected) < 0.001, f"Expected {expected}, got {result}"
    
    print("\n✅ ConversionType 测试通过！")

def test_time_conversion():
    """测试TimeConversion类的功能"""
    print("\n=== 测试 TimeConversion ===\n")
    
    # 测试字符串输入
    tc = TimeConversion("1天2小时30分钟", "chinese")
    result = tc.convert("seconds")
    print(f"  中文时间 -> 秒: 1天2小时30分钟 -> {result}")
    expected = 24*3600 + 2*3600 + 30*60  # 94500秒
    assert abs(result - expected) < 0.001
    
    # 测试英文输入
    tc = TimeConversion("2.5 hours", "english")
    result = tc.convert("minutes")
    print(f"  英文时间 -> 分钟: 2.5 hours -> {result}")
    assert abs(result - 150.0) < 0.001
    
    # 测试冒号格式
    tc = TimeConversion("01:30:45", "colon")
    result = tc.convert("seconds")
    print(f"  冒号格式 -> 秒: 01:30:45 -> {result}")
    assert abs(result - 5445.0) < 0.001
    
    # 测试数字输入
    tc = TimeConversion(3600, "seconds")
    result = tc.convert("chinese")
    print(f"  秒 -> 中文: 3600秒 -> {result}")
    assert "1小时" in result
    
    # 测试ISO8601
    tc = TimeConversion("PT1H30M", "iso8601")
    result = tc.convert("english")
    print(f"  ISO8601 -> 英文: PT1H30M -> {result}")
    assert "1 hour" in result and "30 minutes" in result
    
    # 测试Duration对象
    duration = plm.duration(hours=1, minutes=30)
    tc = TimeConversion(duration)
    result = tc.convert("minutes")
    print(f"  Duration -> 分钟: {duration} -> {result}")
    assert abs(result - 90.0) < 0.001
    
    # 测试格式转换
    tc = TimeConversion(2)  # 默认seconds
    print(f"  当前格式: {tc.get_format()}")
    tc.set_format("hours")
    result = tc.convert("seconds")
    print(f"  小时 -> 秒: 2 hours -> {result}")
    assert abs(result - 7200.0) < 0.001
    
    print("\n✅ TimeConversion 测试通过！")

def test_edge_cases():
    """测试边界情况"""
    print("\n=== 测试边界情况 ===\n")
    
    # 测试零值
    tc = TimeConversion(0, "seconds")
    result = tc.convert("chinese")
    print(f"  零值转换: 0秒 -> {result}")
    assert result == "0秒钟"
    
    # 测试小数
    tc = TimeConversion(1.5, "hours")
    result = tc.convert("minutes")
    print(f"  小数转换: 1.5小时 -> {result}")
    assert abs(result - 90.0) < 0.001
    
    # 测试复杂中文格式
    tc = TimeConversion("1天2小时30分钟45秒钟500毫秒", "chinese")
    result = tc.convert("seconds")
    print(f"  复杂中文: 1天2小时30分钟45秒钟500毫秒 -> {result}")
    expected = 24*3600 + 2*3600 + 30*60 + 45 + 0.5  # 94245.5秒
    assert abs(result - expected) < 0.001
    
    # 测试复杂英文格式
    tc = TimeConversion("1 day 2 hours 30 minutes 45 seconds", "english")
    result = tc.convert("seconds")
    print(f"  复杂英文: 1 day 2 hours 30 minutes 45 seconds -> {result}")
    expected = 24*3600 + 2*3600 + 30*60 + 45  # 94245秒
    assert abs(result - expected) < 0.001
    
    print("\n✅ 边界情况测试通过！")

if __name__ == "__main__":
    test_conversion_type()
    test_time_conversion()
    test_edge_cases()
    print("\n🎉 所有测试通过！")