#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
今天函数使用示例

这个示例展示了 simtoolsz.utils.today 函数的各种用法
"""

import pendulum as plm
from datetime import datetime
from simtoolsz.utils import today


def main():
    print("=== today() 函数使用示例 ===\n")
    
    # 1. 基本用法 - 获取当前日期（返回 DateTime 对象）
    print("1. 获取当前日期对象:")
    current_date = today()
    print(f"   today() = {current_date}")
    print(f"   类型: {type(current_date)}")
    print()
    
    # 2. 获取标准 datetime 对象
    print("2. 获取标准 datetime 对象:")
    std_datetime = today(return_std=True)
    print(f"   today(return_std=True) = {std_datetime}")
    print(f"   类型: {type(std_datetime)}")
    print()
    
    # 3. 获取当前日期时间
    print("3. 获取当前日期时间对象:")
    current_datetime = today(addtime=True)
    print(f"   today(addtime=True) = {current_datetime}")
    print(f"   类型: {type(current_datetime)}")
    print()
    
    # 4. 格式化日期输出
    print("4. 格式化日期输出:")
    formatted_date = today(fmt='YYYY-MM-DD')
    print(f"   today(fmt='YYYY-MM-DD') = '{formatted_date}'")
    print(f"   类型: {type(formatted_date)}")
    print()
    
    # 5. 格式化日期时间输出
    print("5. 格式化日期时间输出:")
    formatted_datetime = today(addtime=True, fmt='YYYY-MM-DD HH:mm:ss')
    print(f"   today(addtime=True, fmt='YYYY-MM-DD HH:mm:ss') = '{formatted_datetime}'")
    print()
    
    # 6. 不同时区的日期
    print("6. 不同时区的日期:")
    local_date = today(fmt='YYYY-MM-DD HH:mm:ss')
    utc_date = today(tz='UTC', fmt='YYYY-MM-DD HH:mm:ss')
    shanghai_date = today(tz='Asia/Shanghai', fmt='YYYY-MM-DD HH:mm:ss')
    
    print(f"   本地时区: {local_date}")
    print(f"   UTC时区:  {utc_date}")
    print(f"   上海时区: {shanghai_date}")
    print()
    
    # 7. 中文格式
    print("7. 中文格式:")
    chinese_date = today(fmt='YYYY年MM月DD日')
    chinese_datetime = today(addtime=True, fmt='YYYY年MM月DD日 HH时mm分ss秒')
    print(f"   中文日期: {chinese_date}")
    print(f"   中文日期时间: {chinese_datetime}")
    print()
    
    # 8. 其他常用格式
    print("8. 其他常用格式:")
    formats = [
        'DD/MM/YYYY',
        'MM-DD-YYYY',
        'YYYY/MM/DD HH:mm',
        'dddd, MMMM D, YYYY',  # 星期几，月份 日，年
        'MMM D, YYYY',         # 简写月份
    ]
    
    for fmt in formats:
        result = today(addtime=True, fmt=fmt)
        print(f"   {fmt:20} = {result}")
    print()
    
    # 9. 与 pendulum 对象的交互
    print("9. 与 pendulum 对象的交互:")
    dt_obj = today()  # 获取 DateTime 对象
    print(f"   原始对象: {dt_obj}")
    print(f"   年份: {dt_obj.year}")
    print(f"   月份: {dt_obj.month}")
    print(f"   日期: {dt_obj.day}")
    print(f"   星期: {dt_obj.day_of_week}")
    print(f"   是否闰年: {dt_obj.is_leap_year()}")
    print()
    
    # 10. 标准 datetime 对象的使用
    print("10. 标准 datetime 对象的使用:")
    std_dt = today(addtime=True, return_std=True)
    print(f"   标准 datetime: {std_dt}")
    print(f"   时间戳: {std_dt.timestamp()}")
    print(f"   ISO格式: {std_dt.isoformat()}")
    print()
    
    # 11. 时区转换
    print("11. 时区转换示例:")
    utc_time = today(tz='UTC', addtime=True)
    print(f"   UTC时间: {utc_time}")
    # 转换到纽约时区
    ny_time = utc_time.in_timezone('America/New_York')
    print(f"   纽约时间: {ny_time}")
    print()
    
    # 12. 组合用法示例
    print("12. 组合用法示例:")
    # 获取UTC时间的标准datetime对象
    utc_std = today(tz='UTC', addtime=True, return_std=True)
    print(f"   UTC标准datetime: {utc_std}")
    
    # 获取格式化的中文日期时间
    chinese_fmt = today(addtime=True, tz='Asia/Shanghai', fmt='YYYY年MM月DD日 HH:mm:ss')
    print(f"   北京时间中文格式: {chinese_fmt}")
    print()


if __name__ == "__main__":
    main()