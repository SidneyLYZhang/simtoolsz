#!/usr/bin/env python3
"""Smoke tests for simtoolsz package

冒烟测试 - 验证各模块基本功能是否正常工作
"""

import pytest


class TestPackageImport:
    """测试包的基本导入"""

    def test_import_package(self):
        """测试包可以正常导入"""
        import simtoolsz
        assert simtoolsz is not None

    def test_package_version(self):
        """测试包版本存在"""
        import simtoolsz
        assert hasattr(simtoolsz, '__version__')
        assert simtoolsz.__version__ is not None

    def test_import_all_modules(self):
        """测试所有模块可以正常导入"""
        import simtoolsz
        expected_modules = ['mail', 'utils', 'datetime', 'db', 'reader', 'countrycode']
        for module_name in expected_modules:
            assert hasattr(simtoolsz, module_name), f"Module {module_name} not found"


class TestDatetimeModule:
    """测试datetime模块基本功能"""

    def test_import_datetime_classes(self):
        """测试datetime模块类可以导入"""
        from simtoolsz.datetime import DurationFormat, TimeConversion
        assert DurationFormat is not None
        assert TimeConversion is not None

    def test_duration_format_enum(self):
        """测试DurationFormat枚举"""
        from simtoolsz.datetime import DurationFormat
        assert hasattr(DurationFormat, 'SECONDS')
        assert hasattr(DurationFormat, 'MINUTES')
        assert hasattr(DurationFormat, 'HOURS')
        assert hasattr(DurationFormat, 'CHINESE')
        assert hasattr(DurationFormat, 'ENGLISH')

    def test_time_conversion_basic(self):
        """测试TimeConversion基本转换"""
        from simtoolsz.datetime import TimeConversion
        tc = TimeConversion(3600, "seconds")
        result = tc.convert("minutes")
        assert abs(result - 60.0) < 0.001

    def test_chinese_format_conversion(self):
        """测试中文格式转换"""
        from simtoolsz.datetime import TimeConversion
        tc = TimeConversion("1小时", "chinese")
        result = tc.convert("seconds")
        assert abs(result - 3600.0) < 0.001


class TestCountryCodeModule:
    """测试countrycode模块基本功能"""

    def test_import_countrycode_classes(self):
        """测试countrycode模块类可以导入"""
        from simtoolsz.countrycode import CountryCode, country_convert, is_data_container
        assert CountryCode is not None
        assert country_convert is not None
        assert is_data_container is not None

    def test_countrycode_init(self):
        """测试CountryCode实例化"""
        from simtoolsz.countrycode import CountryCode
        converter = CountryCode()
        assert converter is not None

    def test_countrycode_convert_iso2_to_zh(self):
        """测试ISO2转中文名称"""
        from simtoolsz.countrycode import CountryCode
        converter = CountryCode()
        result = converter.convert("CN", source="ISO2", target="name_zh")
        assert result == "中国"

    def test_countrycode_convert_iso3_to_zh(self):
        """测试ISO3转中文名称"""
        from simtoolsz.countrycode import CountryCode
        converter = CountryCode()
        result = converter.convert("USA", source="ISO3", target="name_zh")
        assert result == "美国"

    def test_countrycode_valid_classes(self):
        """测试有效类别属性"""
        from simtoolsz.countrycode import CountryCode
        converter = CountryCode()
        assert isinstance(converter.all_valid_class, list)
        assert len(converter.all_valid_class) > 0


class TestUtilsModule:
    """测试utils模块基本功能"""

    def test_import_utils_functions(self):
        """测试utils模块函数可以导入"""
        from simtoolsz.utils import today, yesterday, take_from_list
        assert today is not None
        assert yesterday is not None
        assert take_from_list is not None

    def test_today_function(self):
        """测试today函数"""
        from simtoolsz.utils import today
        result = today()
        assert result is not None

    def test_yesterday_function(self):
        """测试yesterday函数"""
        from simtoolsz.utils import yesterday
        result = yesterday()
        assert result is not None

    def test_take_from_list(self):
        """测试take_from_list函数"""
        from simtoolsz.utils import take_from_list
        result = take_from_list("test", ["test", "value"])
        assert result == "test"


class TestReaderModule:
    """测试reader模块基本功能"""

    def test_import_reader_functions(self):
        """测试reader模块函数可以导入"""
        from simtoolsz.reader import read_tsv, scan_tsv, getreader, load_data
        assert read_tsv is not None
        assert scan_tsv is not None
        assert getreader is not None
        assert load_data is not None

    def test_is_archive_file_import(self):
        """测试is_archive_file函数导入"""
        from simtoolsz.reader import is_archive_file
        assert is_archive_file is not None


class TestDBModule:
    """测试db模块基本功能"""

    def test_import_db_module(self):
        """测试db模块可以导入"""
        import simtoolsz.db as db
        assert db is not None


class TestMailModule:
    """测试mail模块基本功能"""

    def test_import_mail_module(self):
        """测试mail模块可以导入"""
        import simtoolsz.mail as mail
        assert mail is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
