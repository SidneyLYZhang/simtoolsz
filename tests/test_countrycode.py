import pytest
import polars as pl
from simtoolsz.countrycode import CountryCode, country_convert, is_data_container

class TestCountryCode:
    """测试CountryCode类的功能"""
    
    def setup_class(self):
        """初始化测试类，创建CountryCode实例"""
        self.converter = CountryCode()
    
    def test_init(self):
        """测试初始化是否成功"""
        assert self.converter is not None
    
    def test_all_valid_class(self):
        """测试all_valid_class属性"""
        valid_classes = self.converter.all_valid_class
        assert isinstance(valid_classes, list)
        assert len(valid_classes) > 0
    
    def test_core_valid_class(self):
        """测试core_valid_class属性"""
        core_classes = self.converter.core_valid_class
        assert isinstance(core_classes, list)
        assert len(core_classes) > 0
    
    def test_search_info(self):
        """测试search_info静态方法"""
        result = CountryCode.search_info("ISO2")
        assert isinstance(result, str)
    
    def test_guess_source(self):
        """测试_guess_source静态方法"""
        # 测试单个字符串
        assert CountryCode._guess_source("CN") == "ISO2"
        assert CountryCode._guess_source("CHN") == "ISO3"
        assert CountryCode._guess_source("156") == "ISOnumeric"
        assert CountryCode._guess_source("China") == "regex"
        
        # 测试可迭代对象
        result = CountryCode._guess_source(["CN", "CHN", "156"])
        assert isinstance(result, list)
        assert len(result) == 3
    
    def test_get_valid_codename(self):
        """测试_get_valid_codename方法"""
        assert self.converter._get_valid_codename("ISO2") == "ISO2"
        assert self.converter._get_valid_codename("alpha_2") == "ISO2"
        assert self.converter._get_valid_codename("ISO_2") == "ISO2"
    
    def test_convert(self):
        """测试convert方法"""
        # 测试ISO2到中文名称
        result = self.converter.convert("CN", source="ISO2", target="name_zh")
        assert result == "中国"
        
        # 测试ISO3到中文名称
        result = self.converter.convert("CHN", source="ISO3", target="name_zh")
        assert result == "中国"
        
        # 测试自动识别源格式
        result = self.converter.convert("CN", source="auto", target="name_zh")
        assert result == "中国"
        
        # 测试未找到的情况
        result = self.converter.convert("XYZ", source="ISO2", target="name_zh", not_found="未知")
        assert result == "未知"
    
    def test_covert_series(self):
        """测试covert_series方法"""
        # 测试转换列表
        test_list = ["CN", "US", "JP"]
        result = self.converter.covert_series(test_list, source="ISO2", target="name_zh", out_type="list")
        assert isinstance(result, list)
        assert len(result) == 3
        
        # 测试转换为Series
        result = self.converter.covert_series(test_list, source="ISO2", target="name_zh", out_type="series")
        assert isinstance(result, pl.Series)
        assert len(result) == 3
        
        # 测试转换为DataFrame
        result = self.converter.covert_series(test_list, source="ISO2", target="name_zh", out_type="dataframe")
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3
    
    def test_get_method(self):
        """测试get_方法"""
        result = self.converter.get_("ISO2")
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0

class TestCountryConvertFunction:
    """测试country_convert函数"""
    
    def test_convert_single(self):
        """测试转换单个国家代码"""
        result = country_convert("CHN", src="ISO3", to="name_zh")
        assert result == "中国"
    
    def test_convert_list(self):
        """测试转换列表"""
        result = country_convert(["CHN", "USA", "JPN"], src="ISO3", to="name_zh")
        assert isinstance(result, list)
        assert len(result) == 3
    
    def test_convert_series(self):
        """测试转换polars Series"""
        series = pl.Series(["CHN", "USA", "JPN"])
        result = country_convert(series, src="ISO3", to="name_zh")
        assert isinstance(result, list)
        assert len(result) == 3
    
    def test_convert_dataframe(self):
        """测试转换polars DataFrame"""
        df = pl.DataFrame({
            "ISO3": ["CHN", "USA", "JPN"]
        })
        result = country_convert(df, src="ISO3", to="name_zh")
        assert isinstance(result, list)
        assert len(result) == 3
    
    def test_convert_dict(self):
        """测试转换字典"""
        # 字典值为列表
        test_dict = {
            "ISO3": ["CHN", "USA", "JPN"]
        }
        result = country_convert(test_dict, src="ISO3", to="name_zh")
        assert isinstance(result, list)
        assert len(result) == 3
        
        # 字典值为单个字符串
        test_dict = {
            "ISO3": "CHN"
        }
        result = country_convert(test_dict, src="ISO3", to="name_zh")
        assert isinstance(result, str)
        assert result == "中国"

class TestIsDataContainer:
    """测试is_data_container函数"""
    
    def test_dataframe(self):
        """测试DataFrame"""
        df = pl.DataFrame({"col1": [1, 2, 3]})
        assert is_data_container(df) is True
    
    def test_series(self):
        """测试Series"""
        series = pl.Series([1, 2, 3])
        assert is_data_container(series) is True
    
    def test_list(self):
        """测试列表"""
        test_list = [1, 2, 3]
        assert is_data_container(test_list) is True
    
    def test_tuple(self):
        """测试元组"""
        test_tuple = (1, 2, 3)
        assert is_data_container(test_tuple) is True
    
    def test_set(self):
        """测试集合"""
        test_set = {1, 2, 3}
        assert is_data_container(test_set) is True
    
    def test_dict(self):
        """测试字典"""
        test_dict = {"a": 1, "b": 2}
        assert is_data_container(test_dict) is True
    
    def test_string(self):
        """测试字符串"""
        test_str = "test"
        assert is_data_container(test_str) is False
    
    def test_integer(self):
        """测试整数"""
        test_int = 123
        assert is_data_container(test_int) is False
    
    def test_float(self):
        """测试浮点数"""
        test_float = 123.456
        assert is_data_container(test_float) is False

if __name__ == "__main__":
    pytest.main([__file__])
