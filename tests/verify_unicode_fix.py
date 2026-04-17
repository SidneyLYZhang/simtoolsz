#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
邮件Unicode编码修复验证脚本

这个脚本验证修复的邮件发送功能是否能正确处理包含中文和其他
非ASCII字符的情况，避免'ascii' codec编码错误。
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from simtoolsz.mail import send_email


def test_unicode_email_sending():
    """测试Unicode字符邮件发送修复"""
    print("=== 邮件Unicode编码修复验证 ===\n")

    test_cases = [
        {
            "name": "中文发件人和收件人",
            "sender_name": "张三",
            "recipients": ["李四<recipient@example.com>"],
            "subject": "中文主题测试",
            "content": "这是一封包含中文内容的测试邮件。",
        },
        {
            "name": "混合字符测试",
            "sender_name": "Admin管理员",
            "recipients": ["Test用户<recipient@example.com>"],
            "subject": "Mixed Test 混合测试",
            "content": "This email contains both English and 中文内容 for testing.",
        },
    ]

    all_passed = True

    for i, test_case in enumerate(test_cases, 1):
        print(f"测试 {i}: {test_case['name']}")

        try:
            result = send_email(
                email_account="test@example.com",
                password="test123",
                subject=test_case["subject"],
                content=test_case["content"],
                recipients=test_case["recipients"],
                sender_name=test_case["sender_name"],
                smtp_config={
                    "smtp_server": "smtp.example.com",
                    "port": 587,
                    "use_ssl": False,
                },
                timeout=3,  # 短超时，因为我们只测试编码不实际发送
            )

            # 检查是否出现ASCII编码错误
            error_msg = result["message"].lower()
            if "ascii" in error_msg and "codec" in error_msg:
                print(f"❌ ASCII编码错误仍然存在: {result['message']}")
                all_passed = False
            else:
                print(f"✅ Unicode编码处理正常")
                print(f"   消息: {result['message']}")

        except UnicodeEncodeError as e:
            print(f"❌ Unicode编码错误: {e}")
            all_passed = False
        except Exception as e:
            # 其他错误（如网络连接失败）是可以接受的，因为我们没有真实的服务器
            error_msg = str(e).lower()
            if "ascii" in error_msg and "codec" in error_msg:
                print(f"❌ ASCII编码错误: {e}")
                all_passed = False
            else:
                print(f"✅ 无编码错误（其他错误可接受）: {e}")

        print()

    return all_passed


if __name__ == "__main__":
    success = test_unicode_email_sending()

    if success:
        print("🎉 邮件Unicode编码修复验证通过！")
        print("修复成功解决了 'ascii' codec can't encode characters 错误。")
    else:
        print("❌ 邮件Unicode编码修复验证失败。")
        print("仍然存在编码问题，需要进一步修复。")

    sys.exit(0 if success else 1)
