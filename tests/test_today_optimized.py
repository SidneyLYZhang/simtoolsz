#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试优化后的 today 函数
"""

from datetime import datetime
import pendulum as plm
from simtoolsz.utils import today


def test_all_return_types():
    """测试所有返回类型"""
    print("=== 测试不同的返回类型 ===")

    # 测试 pendulum DateTime 对象
    pd_dt = today()
    print(f"pendulum DateTime: {type(pd_dt)} - {pd_dt}")
    assert isinstance(pd_dt, plm.DateTime), "应该返回 pendulum DateTime 对象"

    # 测试标准 datetime 对象
    std_dt = today(return_std=True)
    print(f"标准 datetime: {type(std_dt)} - {std_dt}")
    assert isinstance(std_dt, datetime), "应该返回标准 datetime 对象"

    # 测试字符串返回
    str_result = today(fmt="YYYY-MM-DD")
    print(f"字符串: {type(str_result)} - {str_result}")
    assert isinstance(str_result, str), "应该返回字符串"

    print("✓ 所有返回类型测试通过\n")


def test_timezone_handling():
    """测试时区处理"""
    print("=== 测试时区处理 ===")

    # 测试不同时区
    local_dt = today(addtime=True)
    utc_dt = today(tz="UTC", addtime=True)
    shanghai_dt = today(tz="Asia/Shanghai", addtime=True)

    print(f"本地时区: {local_dt}")
    print(f"UTC时区: {utc_dt}")
    print(f"上海时区: {shanghai_dt}")

    # 验证时区差异
    assert local_dt.timezone_name != utc_dt.timezone_name, "时区应该不同"
    print("✓ 时区处理测试通过\n")


def test_format_options():
    """测试格式化选项"""
    print("=== 测试格式化选项 ===")

    # 测试日期格式化
    date_fmt = today(fmt="YYYY-MM-DD")
    print(f"日期格式: {date_fmt}")
    assert len(date_fmt) == 10, "日期格式应该是 10 个字符"

    # 测试日期时间格式化
    datetime_fmt = today(addtime=True, fmt="YYYY-MM-DD HH:mm:ss")
    print(f"日期时间格式: {datetime_fmt}")
    assert len(datetime_fmt) == 19, "日期时间格式应该是 19 个字符"

    # 测试中文格式
    chinese_fmt = today(fmt="YYYY年MM月DD日")
    print(f"中文格式: {chinese_fmt}")
    assert "年" in chinese_fmt and "月" in chinese_fmt and "日" in chinese_fmt, (
        "应该包含中文字符"
    )

    print("✓ 格式化选项测试通过\n")


def test_parameter_combinations():
    """测试参数组合"""
    print("=== 测试参数组合 ===")

    # 测试 return_std 和 fmt 的组合
    # 当 fmt 有时，return_std 应该被忽略
    result1 = today(fmt="YYYY-MM-DD", return_std=True)
    assert isinstance(result1, str), "当 fmt 有时应该返回字符串"

    # 测试 addtime 和 return_std 的组合
    result2 = today(addtime=True, return_std=True)
    assert isinstance(result2, datetime), "应该返回标准 datetime 对象"

    # 测试所有参数组合
    result3 = today(tz="UTC", addtime=True, fmt="YYYY-MM-DD HH:mm:ss", return_std=True)
    assert isinstance(result3, str), "当 fmt 有时应该返回字符串"

    print("✓ 参数组合测试通过\n")


def test_edge_cases():
    """测试边界情况"""
    print("=== 测试边界情况 ===")

    # 测试不同时区名称
    try:
        result = today(tz="Invalid/Timezone", fmt="YYYY-MM-DD")
        print(f"无效时区测试结果: {result}")
    except Exception as e:
        print(f"无效时区处理: {e}")

    # 测试不同的格式化字符串
    custom_formats = [
        "DD-MM-YYYY",
        "MM/DD/YYYY",
        "YYYY.MM.DD",
        "dddd, MMMM D, YYYY",
        "MMM D, YYYY",
    ]

    for fmt in custom_formats:
        result = today(fmt=fmt)
        print(f"格式 {fmt}: {result}")

    print("✓ 边界情况测试通过\n")


def main():
    """运行所有测试"""
    print("开始测试优化后的 today 函数...\n")

    try:
        test_all_return_types()
        test_timezone_handling()
        test_format_options()
        test_parameter_combinations()
        test_edge_cases()

        print("🎉 所有测试通过！函数优化成功！")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

    return True


if __name__ == "__main__":
    main()
