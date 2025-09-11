#!/usr/bin/env python3
"""ConversionType和TimeConversion类的使用示例"""

from simtoolsz.datetime import TimeConversion, DurationFormat

def main():
    """展示时间格式转换的各种用法"""
    
    print("=== 时间格式转换示例 ===\n")
    
    # 示例1: 中文时间到秒
    print("1. 中文时间到秒")
    tc = TimeConversion("1天2小时30分钟45秒", "chinese")
    seconds = tc.convert("seconds")
    print(f"   1天2小时30分钟45秒 = {seconds}秒")
    
    # 示例2: 秒到中文时间
    print("\n2. 秒到中文时间")
    tc = TimeConversion(90061, "seconds")
    chinese = tc.convert("chinese")
    print(f"   90061秒 = {chinese}")
    
    # 示例3: 英文时间到分钟
    print("\n3. 英文时间到分钟")
    tc = TimeConversion("2.5 hours 30 minutes", "english")
    minutes = tc.convert("minutes")
    print(f"   2.5 hours 30 minutes = {minutes}分钟")
    
    # 示例4: 冒号时间到秒
    print("\n4. 冒号时间到秒")
    tc = TimeConversion("01:30:45.5", "colon")
    seconds = tc.convert("seconds")
    print(f"   01:30:45.5 = {seconds}秒")
    
    # 示例5: 毫秒到小时
    print("\n5. 毫秒到小时")
    tc = TimeConversion(3600000, "milliseconds")
    hours = tc.convert("hours")
    print(f"   3600000毫秒 = {hours}小时")
    
    # 示例6: 多种格式间的相互转换
    print("\n6. 多种格式间的相互转换")
    original = "2小时30分钟"
    
    # 中文 -> 各种格式
    tc = TimeConversion(original, "chinese")
    
    print(f"   原始: {original}")
    print(f"   秒: {tc.convert('seconds')}")
    print(f"   分钟: {tc.convert('minutes')}")
    print(f"   小时: {tc.convert('hours')}")
    print(f"   英文: {tc.convert('english')}")
    print(f"   冒号: {tc.convert('colon')}")
    
    # 示例7: 使用DurationFormat枚举
    print("\n7. 使用DurationFormat枚举")
    tc = TimeConversion(3600, DurationFormat.SECONDS)
    
    for fmt in [DurationFormat.MINUTES, DurationFormat.HOURS, 
                DurationFormat.CHINESE, DurationFormat.ENGLISH]:
        result = tc.convert(fmt)
        print(f"   3600秒 -> {fmt.value}: {result}")
    
    # 示例8: 动态格式切换
    print("\n8. 动态格式切换")
    tc = TimeConversion(7200)  # 默认是seconds
    print(f"   默认格式: {tc.get_format()}")
    print(f"   7200秒 = {tc.convert('minutes')}分钟")
    
    # 创建一个新的TimeConversion实例，输入是2小时
    tc2 = TimeConversion(2, "hours")
    result = tc2.convert("seconds")
    print(f"   2小时 = {result}秒")
    
    # 示例9: ISO 8601 格式转换
    print("\n9. ISO 8601 格式转换")
    
    # 秒到ISO 8601
    tc = TimeConversion(90061, "seconds")
    iso_format = tc.convert("iso8601")
    print(f"   90061秒 = {iso_format}")
    
    # ISO 8601到秒
    tc = TimeConversion("P1DT1H1M1S", "iso8601")
    seconds = tc.convert("seconds")
    print(f"   P1DT1H1M1S = {seconds}秒")
    
    # 复杂ISO 8601格式
    tc = TimeConversion("PT2H30M15.5S", "iso8601")
    print(f"   PT2H30M15.5S = {tc.convert('minutes')}分钟")
    
    # 示例10: 所有格式间的相互转换
    print("\n10. 所有格式间的相互转换")
    original = 3661.5  # 1小时1分钟1.5秒
    tc = TimeConversion(original, "seconds")
    
    print(f"   原始: {original}秒")
    print(f"   中文: {tc.convert('chinese')}")
    print(f"   英文: {tc.convert('english')}")
    print(f"   冒号: {tc.convert('colon')}")
    print(f"   ISO 8601: {tc.convert('iso8601')}")
    print(f"   毫秒: {tc.convert('milliseconds')}")
    print(f"   分钟: {tc.convert('minutes')}")
    print(f"   小时: {tc.convert('hours')}")
    print(f"   天: {tc.convert('hours') / 24}")

if __name__ == "__main__":
    main()