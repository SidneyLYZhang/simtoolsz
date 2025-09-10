#!/usr/bin/env python3
"""简化版测试文件"""

import pendulum as plm
from simtoolsz.datetime import DurationFormat, TimeConversion, ConversionType

def test_basic_conversions():
    """测试基本转换功能"""
    print("=== 基本转换测试 ===\n")
    
    # 测试数字转换
    converter = ConversionType(DurationFormat.SECONDS)
    result = converter.fit(DurationFormat.MINUTES)(3600)
    print(f"3600秒 = {result}分钟")
    assert abs(result - 60.0) < 0.001
    
    # 测试中文转换
    converter = ConversionType(DurationFormat.CHINESE)
    result = converter.fit(DurationFormat.SECONDS)("1小时")
    print(f"1小时 = {result}秒")
    assert abs(result - 3600.0) < 0.001
    
    # 测试英文转换
    converter = ConversionType(DurationFormat.ENGLISH)
    result = converter.fit(DurationFormat.MINUTES)("2 hours")
    print(f"2 hours = {result}分钟")
    assert abs(result - 120.0) < 0.001
    
    # 测试冒号格式
    converter = ConversionType(DurationFormat.COLON)
    result = converter.fit(DurationFormat.SECONDS)("01:30")
    print(f"01:30 = {result}秒")
    assert abs(result - 90.0) < 0.001
    
    print("\n✅ 基本转换测试通过！")

def test_time_conversion_class():
    """测试TimeConversion类"""
    print("\n=== TimeConversion类测试 ===\n")
    
    # 测试中文到秒的转换
    tc = TimeConversion("2小时30分钟", "chinese")
    result = tc.convert("seconds")
    print(f"2小时30分钟 = {result}秒")
    assert abs(result - 9000.0) < 0.001
    
    # 测试秒到中文的转换
    tc = TimeConversion(7200, "seconds")
    result = tc.convert("chinese")
    print(f"7200秒 = {result}")
    assert "2小时" in result
    
    # 测试Duration对象
    duration = plm.duration(minutes=30)
    tc = TimeConversion(duration)
    result = tc.convert("seconds")
    print(f"30分钟 = {result}秒")
    assert abs(result - 1800.0) < 0.001
    
    print("\n✅ TimeConversion类测试通过！")

if __name__ == "__main__":
    test_basic_conversions()
    test_time_conversion_class()
    print("\n🎉 所有测试通过！")