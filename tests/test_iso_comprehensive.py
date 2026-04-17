#!/usr/bin/env python3
"""
ISO 8601 格式全面测试
"""

import pendulum as plm
from simtoolsz.datetime import TimeConversion


def test_iso_roundtrip():
    """测试ISO 8601格式的往返转换"""
    print("=== ISO 8601 往返转换测试 ===\n")

    test_cases = [
        0,  # 零值
        30,  # 30秒
        60,  # 1分钟
        3600,  # 1小时
        3661,  # 1小时1分钟1秒
        86400,  # 1天
        90061,  # 1天1小时1分钟1秒
        172800,  # 2天
        604800,  # 1周
        3661.5,  # 带小数秒
        3661.25,  # 更多小数位
        1.5,  # 1.5秒
        0.5,  # 0.5秒
        3600.5,  # 1小时0.5秒
    ]

    for total_seconds in test_cases:
        # 创建Duration
        duration = plm.duration(seconds=total_seconds)

        # 转换为ISO格式
        tc = TimeConversion(duration)
        iso_format = tc.convert("iso8601")

        # 从ISO格式解析回来
        tc_parsed = TimeConversion(iso_format, "iso8601")
        parsed_seconds = tc_parsed.convert("seconds")

        # 验证
        status = "✓" if abs(parsed_seconds - total_seconds) < 0.001 else "✗"
        print(
            f"{status} {total_seconds:>9.1f}s -> {iso_format:<20} -> {parsed_seconds:>9.1f}s"
        )

    print("\n✅ 往返转换测试完成！\n")


def test_iso_examples():
    """测试ISO 8601标准示例"""
    print("=== ISO 8601 标准示例测试 ===\n")

    examples = [
        ("P1Y", "1年"),
        ("P1M", "1月"),
        ("P1D", "1天"),
        ("PT1H", "1小时"),
        ("PT1M", "1分钟"),
        ("PT1S", "1秒"),
        ("P1DT1H1M1S", "1天1小时1分钟1秒"),
        ("P3Y6M4DT12H30M5S", "3年6月4天12小时30分钟5秒"),
        ("P23DT23H", "23天23小时"),
        ("P4Y", "4年"),
        ("PT0S", "0秒"),
    ]

    print("注意：由于pendulum.Duration的限制，年/月单位会被近似处理\n")

    for iso_str, description in examples:
        try:
            tc = TimeConversion(iso_str, "iso8601")
            seconds = tc.convert("seconds")

            # 转换回ISO格式
            back_to_iso = tc.convert("iso8601")

            print(f"✓ {iso_str:<20} ({description})")
            print(f"   -> {seconds:>12.1f} 秒")
            print(f"   -> {back_to_iso}")
            print()
        except Exception as e:
            print(f"✗ {iso_str:<20} ({description})")
            print(f"   错误: {e}")
            print()


def test_edge_cases():
    """测试边界情况"""
    print("=== 边界情况测试 ===\n")

    edge_cases = [
        # 各种组合
        "PT0.5S",
        "PT0.25H",
        "P1DT0.5H",
        "PT30M30S",
        "P1DT12H",
        "PT1H30M45.123S",
    ]

    for iso_str in edge_cases:
        try:
            tc = TimeConversion(iso_str, "iso8601")
            seconds = tc.convert("seconds")

            # 再转换回ISO格式
            back_to_iso = tc.convert("iso8601")

            print(f"✓ {iso_str:<25} -> {seconds:>12.3f}s -> {back_to_iso}")
        except Exception as e:
            print(f"✗ {iso_str:<25} -> 错误: {e}")

    print("\n✅ 边界情况测试完成！\n")


if __name__ == "__main__":
    test_iso_roundtrip()
    test_iso_examples()
    test_edge_cases()
    print("🎉 所有ISO 8601测试完成！")
