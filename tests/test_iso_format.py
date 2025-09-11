#!/usr/bin/env python3
"""
测试ISO 8601格式转换功能
"""
import pendulum as plm
from src.simtoolsz.datetime import ConversionType, DurationFormat

def test_iso_format():
    """测试各种ISO 8601格式转换"""
    converter = ConversionType(DurationFormat.SECONDS)
    
    # 测试用例
    test_cases = [
        # (总秒数, 期望的ISO格式)
        (0, "PT0S"),
        (30, "PT30S"),
        (60, "PT1M"),
        (3600, "PT1H"),
        (3661, "PT1H1M1S"),
        (86400, "P1D"),
        (90061, "P1DT1H1M1S"),
        (172800, "P2D"),
        (3661.5, "PT1H1M1.5S"),
        (3661.25, "PT1H1M1.25S"),
        (90000, "P1DT1H"),
        (3660, "PT1H1M"),
        (65, "PT1M5S"),
        (1.5, "PT1.5S"),
        (0.5, "PT0.5S"),
        (3600.5, "PT1H0.5S"),
    ]
    
    print("测试ISO 8601格式转换:")
    print("-" * 50)
    
    for total_seconds, expected in test_cases:
        duration = plm.duration(seconds=total_seconds)
        result = converter._duration_to_iso(duration)
        status = "✓" if result == expected else "✗"
        print(f"{status} {total_seconds:>8}s -> {result:<20} (期望: {expected})")
    
    print("-" * 50)

if __name__ == "__main__":
    test_iso_format()