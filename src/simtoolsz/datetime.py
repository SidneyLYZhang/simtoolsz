"""
时间日期处理与转换模块

提供时间持续格式的转换功能，支持秒、毫秒、分钟、小时、ISO8601、中文、英文、冒号格式等。

主要功能:
    - DurationFormat: 时间持续格式枚举类
    - TimeConversion: 时间转换类
    - covertChineseShort: 转换为中文短格式
    - getTimeSpan: 获取时间间隔

支持的时间格式:
    - seconds: 秒
    - milliseconds: 毫秒
    - minutes: 分钟
    - hours: 小时
    - iso8601: ISO 8601 持续时间格式 (如 P1DT2H3M4S)
    - chinese: 中文格式 (如 1天2小时3分钟)
    - english: 英文格式 (如 1 day 2 hours 3 minutes)
    - colon: 冒号分隔格式 (如 01:02:03)
"""

import pendulum as plm
from enum import Enum
from simtoolsz.utils import Number, today
from typing import NewType, Self, Optional, Callable
import re
import datetime as dt


__all__ = [
    "TimeConversion",
    "DurationFormat",
    "DURATIONTYPE",
    "covertChineseShort",
    "getTimeSpan",
]

DURATIONTYPE = NewType("DURATIONTYPE", str | Number | plm.Duration)

DATETIMESETS = NewType("DATETIMESETS", plm.DateTime | dt.datetime)


class DurationFormat(Enum):
    """
    时间持续格式枚举类。

    提供各种时间格式化的标准格式，支持多种输入输出格式。

    Attributes:
        SECONDS: 秒
        MILLISECONDS: 毫秒
        MINUTES: 分钟
        HOURS: 小时
        ISO8601: ISO 8601 持续时间格式
        CHINESE: 中文格式
        ENGLISH: 英文格式
        COLON: 冒号分隔格式
        DURATION: pendulum.Duration 对象

    Examples:
        >>> DurationFormat.SECONDS
        <DurationFormat.SECONDS: 'seconds'>
        >>> DurationFormat.get_format('chinese')
        <DurationFormat.CHINESE: 'chinese'>
    """

    SECONDS = "seconds"
    MILLISECONDS = "milliseconds"
    MINUTES = "minutes"
    HOURS = "hours"
    ISO8601 = "iso8601"
    CHINESE = "chinese"
    ENGLISH = "english"
    COLON = "colon-separated"
    DURATION = "duration"

    def __str__(self) -> str:
        """返回格式的字符串表示"""
        return self.value

    def __repr__(self) -> str:
        """返回格式的详细字符串表示"""
        return f"DurationFormat.{self.name}"

    def __eq__(self, other: object) -> bool:
        """比较两个DurationFormat是否相等"""
        if not isinstance(other, DurationFormat):
            return False
        return self.value == other.value

    @classmethod
    def get_format(cls, format: str) -> Self:
        """
        根据字符串获取对应的DurationFormat枚举值。

        Args:
            format: 格式字符串，不区分大小写

        Returns:
            DurationFormat: 对应的枚举值

        Raises:
            KeyError: 如果格式字符串无效

        Examples:
            >>> DurationFormat.get_format('CHINESE')
            <DurationFormat.CHINESE: 'chinese'>
        """
        try:
            return cls.__members__[format.upper()]
        except KeyError:
            raise KeyError(f"无效的格式: {format}。可选值: {[f.value for f in cls]}")

    @classmethod
    def all_formats(cls) -> list[Self]:
        """获取所有可用的格式"""
        return list(cls)

    @classmethod
    def format_names(cls) -> list[str]:
        """获取所有格式的名称列表"""
        return [f.name for f in cls]

    @classmethod
    def format_values(cls) -> list[str]:
        """获取所有格式的值列表"""
        return [f.value for f in cls]

    @classmethod
    def which_format(
        cls, value: DURATIONTYPE, cast: str | None = None
    ) -> Optional[Self]:
        """
        根据输入内容，自动判断属于哪种时间持续格式。

        自动识别规则:
            - pendulum.Duration: DURATION
            - int: SECONDS
            - float: MINUTES
            - 包含中文时间单位: CHINESE
            - 英文时间格式 (如 "1 day"): ENGLISH
            - 冒号分隔格式 (如 "01:30:45"): COLON
            - ISO8601格式 (如 "P1DT2H"): ISO8601
            - 纯数字字符串: SECONDS

        Args:
            value: 输入值，可以是字符串、数字或pendulum.Duration
            cast: 强制指定格式类型，如果提供则按指定类型判断

        Returns:
            DurationFormat: 对应的格式枚举，无法判断时返回None
        """
        if cast is not None:
            try:
                target_format = cls.get_format(cast)
            except KeyError:
                return None

            if isinstance(value, plm.Duration):
                return target_format if target_format == cls.DURATION else None

            if isinstance(value, (int, float)):
                if target_format.is_human_readable:
                    return None
                return target_format

            if isinstance(value, str):
                if target_format.is_human_readable:
                    return target_format

                try:
                    float(value)
                    return target_format if target_format.is_time_unit else None
                except (ValueError, TypeError):
                    return None

            return None

        if isinstance(value, plm.Duration):
            return cls.DURATION

        if isinstance(value, int):
            return cls.SECONDS

        if isinstance(value, float):
            return cls.MINUTES

        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None

            if any(unit in value for unit in ["天", "小时", "分钟", "秒钟", "毫秒"]):
                return cls.CHINESE

            english_pattern = (
                r"^\s*\d+(\.\d+)?\s*(days?|hours?|minutes?|seconds?|milliseconds?)\s*$"
            )
            if re.match(english_pattern, value, re.IGNORECASE):
                return cls.ENGLISH

            colon_pattern = r"^\s*\d{1,2}(:\d{1,2}){1,2}\s*$"
            if re.match(colon_pattern, value):
                return cls.COLON

            iso_pattern = r"^P(\d+D)?(T(\d+H)?(\d+M)?(\d+S)?)?$"
            if re.match(iso_pattern, value.upper()):
                return cls.ISO8601

            try:
                float(value)
                return cls.SECONDS
            except (ValueError, TypeError):
                pass

        return None

    @property
    def is_time_unit(self) -> bool:
        """判断是否为时间单位格式（秒、毫秒、分钟、小时）"""
        return self.value in {
            self.SECONDS.value,
            self.MILLISECONDS.value,
            self.MINUTES.value,
            self.HOURS.value,
        }

    @property
    def is_human_readable(self) -> bool:
        """判断是否为人可读格式（中文、英文、冒号）"""
        return self.value in {self.CHINESE.value, self.ENGLISH.value, self.COLON.value}


class ConversionType(object):
    """
    时间格式转换器内部类。

    负责将各种格式的输入转换为pendulum.Duration，以及将Duration转换为目标格式。
    """

    __all__ = ["fit"]

    def __init__(self, type: DurationFormat) -> None:
        """
        初始化转换器。

        Args:
            type: 输入值的格式类型
        """
        self._type = type

    def _parse_to_duration(self, value: DURATIONTYPE) -> plm.Duration:
        """
        将各种格式的输入转换为pendulum.Duration。

        Args:
            value: 输入值

        Returns:
            plm.Duration: 转换后的Duration对象

        Raises:
            ValueError: 当输入格式无效时
        """
        if isinstance(value, plm.Duration):
            return value

        if isinstance(value, (int, float)):
            if self._type == DurationFormat.SECONDS:
                return plm.duration(seconds=value)
            elif self._type == DurationFormat.MILLISECONDS:
                return plm.duration(milliseconds=value)
            elif self._type == DurationFormat.MINUTES:
                return plm.duration(minutes=value)
            elif self._type == DurationFormat.HOURS:
                return plm.duration(hours=value)
            else:
                return plm.duration(seconds=value)

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("空字符串无法转换")

            if self._type == DurationFormat.ISO8601:
                return self._parse_iso8601_duration(value)

            if self._type == DurationFormat.CHINESE:
                return self._parse_chinese_duration(value)

            if self._type == DurationFormat.ENGLISH:
                return self._parse_english_duration(value)

            if self._type == DurationFormat.COLON:
                return self._parse_colon_duration(value)

            try:
                num_value = float(value)
                return self._parse_to_duration(num_value)
            except ValueError:
                raise ValueError(f"无法解析字符串为数字: {value}")

        raise ValueError(f"不支持的输入类型: {type(value)}")

    def _parse_chinese_duration(self, value: str) -> plm.Duration:
        """
        解析中文时间格式。

        支持的中文单位: 天、小时、时、分钟、分、秒钟、秒、毫秒

        Args:
            value: 中文时间字符串

        Returns:
            plm.Duration: 解析后的Duration对象
        """
        import re

        total_seconds = 0
        matched = False

        match = re.search(r"(\d+(?:\.\d+)?)\s*天", value)
        if match:
            total_seconds += float(match.group(1)) * 86400
            matched = True

        match = re.search(r"(\d+(?:\.\d+)?)\s*小时", value)
        if not match:
            match = re.search(r"(\d+(?:\.\d+)?)\s*时", value)
        if match:
            total_seconds += float(match.group(1)) * 3600
            matched = True

        match = re.search(r"(\d+(?:\.\d+)?)\s*分钟", value)
        if not match:
            match = re.search(r"(\d+(?:\.\d+)?)\s*分", value)
        if match:
            total_seconds += float(match.group(1)) * 60
            matched = True

        match = re.search(r"(\d+(?:\.\d+)?)\s*秒钟", value)
        if not match:
            match = re.search(r"(\d+(?:\.\d+)?)\s*秒", value)
        if match:
            total_seconds += float(match.group(1)) * 1
            matched = True

        match = re.search(r"(\d+(?:\.\d+)?)\s*毫秒", value)
        if match:
            total_seconds += float(match.group(1)) * 0.001
            matched = True

        if not matched:
            try:
                return plm.duration(seconds=float(value))
            except ValueError:
                raise ValueError(f"无效的中文时间格式: {value}")

        return plm.duration(seconds=total_seconds)

    def _parse_english_duration(self, value: str) -> plm.Duration:
        """
        解析英文时间格式。

        支持的英文单位: day(s), hour(s), minute(s), second(s), millisecond(s)

        Args:
            value: 英文时间字符串

        Returns:
            plm.Duration: 解析后的Duration对象
        """
        import re

        pattern = r"(\d+(?:\.\d+)?)\s*([a-zA-Z]+)"
        matches = re.findall(pattern, value)

        if not matches:
            raise ValueError(f"无效的英文时间格式: {value}")

        total_seconds = 0
        unit_mapping = {
            "days": 86400,
            "day": 86400,
            "hours": 3600,
            "hour": 3600,
            "minutes": 60,
            "minute": 60,
            "seconds": 1,
            "second": 1,
            "milliseconds": 0.001,
            "millisecond": 0.001,
        }

        for num_str, unit in matches:
            num = float(num_str)
            unit_lower = unit.lower()
            if unit_lower in unit_mapping:
                total_seconds += num * unit_mapping[unit_lower]
            else:
                raise ValueError(f"未知的英文时间单位: {unit}")

        return plm.duration(seconds=total_seconds)

    def _parse_colon_duration(self, value: str) -> plm.Duration:
        """
        解析冒号时间格式。

        支持的格式:
            - MM:SS (分钟:秒)
            - HH:MM:SS (小时:分钟:秒)

        Args:
            value: 冒号分隔的时间字符串

        Returns:
            plm.Duration: 解析后的Duration对象
        """
        parts = value.split(":")

        if len(parts) == 2:
            minutes, seconds = map(float, parts)
            return plm.duration(minutes=minutes, seconds=seconds)
        elif len(parts) == 3:
            hours, minutes, seconds = map(float, parts)
            return plm.duration(hours=hours, minutes=minutes, seconds=seconds)
        else:
            raise ValueError(f"无效的冒号时间格式: {value}")

    def _parse_iso8601_duration(self, value: str) -> plm.Duration:
        """
        解析ISO 8601持续时间格式。

        支持的格式: P[n]DT[n]H[n]M[n]S
        示例: P1DT2H3M4S (1天2小时3分钟4秒)

        Args:
            value: ISO 8601格式的持续时间字符串

        Returns:
            plm.Duration: 解析后的Duration对象
        """
        import re

        value = value.strip()
        if not value.startswith("P"):
            raise ValueError(f"无效的ISO 8601格式: {value} (必须以P开头)")

        duration_str = value[1:]

        total_seconds = 0

        if "T" in duration_str:
            date_part, time_part = duration_str.split("T", 1)
        else:
            date_part = duration_str
            time_part = ""

        if date_part:
            day_match = re.search(r"(\d+(?:\.\d+)?)D", date_part)
            if day_match:
                total_seconds += float(day_match.group(1)) * 86400

            week_match = re.search(r"(\d+(?:\.\d+)?)W", date_part)
            if week_match:
                total_seconds += float(week_match.group(1)) * 7 * 86400

        if time_part:
            hour_match = re.search(r"(\d+(?:\.\d+)?)H", time_part)
            if hour_match:
                total_seconds += float(hour_match.group(1)) * 3600

            minute_match = re.search(r"(\d+(?:\.\d+)?)M", time_part)
            if minute_match:
                total_seconds += float(minute_match.group(1)) * 60

            second_match = re.search(r"(\d+(?:\.\d+)?)S", time_part)
            if second_match:
                total_seconds += float(second_match.group(1))

        if total_seconds == 0 and value != "PT0S":
            raise ValueError(f"无效的ISO 8601格式: {value} (没有有效的时间单位)")

        return plm.duration(seconds=total_seconds)

    def _duration_to_target(
        self, duration: plm.Duration, target_format: DurationFormat
    ) -> DURATIONTYPE:
        """
        将Duration转换为目标格式。

        Args:
            duration: pendulum Duration对象
            target_format: 目标格式

        Returns:
            DURATIONTYPE: 转换后的值
        """
        if target_format == DurationFormat.SECONDS:
            return duration.total_seconds()
        elif target_format == DurationFormat.MILLISECONDS:
            return duration.total_seconds() * 1000
        elif target_format == DurationFormat.MINUTES:
            return duration.total_seconds() / 60
        elif target_format == DurationFormat.HOURS:
            return duration.total_seconds() / 3600
        elif target_format == DurationFormat.ISO8601:
            return self._duration_to_iso(duration)
        elif target_format == DurationFormat.CHINESE:
            return self._duration_to_chinese(duration)
        elif target_format == DurationFormat.ENGLISH:
            return self._duration_to_english(duration)
        elif target_format == DurationFormat.COLON:
            return self._duration_to_colon(duration)
        elif target_format == DurationFormat.DURATION:
            return duration
        else:
            return duration.total_seconds()

    def _duration_to_iso(self, duration: plm.Duration) -> str:
        """
        将Duration转换为ISO 8601格式。

        Args:
            duration: pendulum Duration对象

        Returns:
            str: ISO 8601格式的字符串
        """
        total_seconds = duration.total_seconds()

        if total_seconds == 0:
            return "PT0S"

        days = duration.days

        remaining_seconds = total_seconds - (days * 86400)
        hours = int(remaining_seconds // 3600)
        remaining_seconds %= 3600
        minutes = int(remaining_seconds // 60)
        seconds = remaining_seconds % 60

        parts = []

        if days != 0:
            parts.append(f"{days}D")

        time_parts = []
        if hours != 0:
            time_parts.append(f"{hours}H")
        if minutes != 0:
            time_parts.append(f"{minutes}M")
        if seconds != 0:
            if seconds.is_integer():
                if int(seconds) != 0:
                    time_parts.append(f"{int(seconds)}S")
            else:
                seconds_str = f"{seconds:.6f}".rstrip("0").rstrip(".")
                time_parts.append(f"{seconds_str}S")

        result = "P"

        if parts and time_parts:
            result += "".join(parts) + "T" + "".join(time_parts)
        elif parts:
            result += "".join(parts)
        elif time_parts:
            result += "T" + "".join(time_parts)

        return result

    def _duration_to_chinese(self, duration: plm.Duration) -> str:
        """
        将Duration转换为中文格式。

        Args:
            duration: pendulum Duration对象

        Returns:
            str: 中文格式的时间字符串
        """
        total_seconds = duration.total_seconds()

        if total_seconds == 0:
            return "0秒钟"

        parts = []
        days = int(total_seconds // 86400)
        hours = int((total_seconds % 86400) // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        milliseconds = int((total_seconds % 1) * 1000)

        if days > 0:
            parts.append(f"{days}天")
        if hours > 0:
            parts.append(f"{hours}小时")
        if minutes > 0:
            parts.append(f"{minutes}分钟")
        if seconds > 0:
            parts.append(f"{seconds}秒钟")
        if milliseconds > 0:
            parts.append(f"{milliseconds}毫秒")

        return "".join(parts) if parts else "0秒钟"

    def _duration_to_english(self, duration: plm.Duration) -> str:
        """
        将Duration转换为英文格式。

        Args:
            duration: pendulum Duration对象

        Returns:
            str: 英文格式的时间字符串
        """
        total_seconds = duration.total_seconds()

        if total_seconds == 0:
            return "0 seconds"

        parts = []
        days = int(total_seconds // 86400)
        hours = int((total_seconds % 86400) // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        milliseconds = int((total_seconds % 1) * 1000)

        if days > 0:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if seconds > 0:
            parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
        if milliseconds > 0:
            parts.append(
                f"{milliseconds} millisecond{'s' if milliseconds != 1 else ''}"
            )

        return " ".join(parts) if parts else "0 seconds"

    def _duration_to_colon(self, duration: plm.Duration) -> str:
        """
        将Duration转换为冒号格式。

        Args:
            duration: pendulum Duration对象

        Returns:
            str: 冒号格式的时间字符串 (HH:MM:SS)
        """
        total_seconds = duration.total_seconds()

        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def fit(self, dest: DurationFormat) -> Callable[[DURATIONTYPE], DURATIONTYPE]:
        """
        返回一个转换函数，将当前类型的值转换为目标格式。

        Args:
            dest: 目标格式

        Returns:
            转换函数: DURATIONTYPE -> DURATIONTYPE

        Examples:
            >>> conv = ConversionType(DurationFormat.CHINESE)
            >>> to_seconds = conv.fit(DurationFormat.SECONDS)
            >>> to_seconds("1小时30分钟")
            5400
        """

        def converter(value: DURATIONTYPE) -> DURATIONTYPE:
            try:
                duration = self._parse_to_duration(value)
                return self._duration_to_target(duration, dest)
            except Exception as e:
                raise ValueError(f"转换失败: {e}")

        return converter


class TimeConversion(object):
    """
    时间转换类。

    支持不同时间格式之间的转换，包括秒、毫秒、分钟、小时、ISO8601、中文、英文和冒号格式。

    Attributes:
        _time: 原始时间值
        _Type: 输入值的格式类型
        _convType: 内部转换器

    Examples:
        >>> tc = TimeConversion("1天")
        >>> tc.convert("seconds")
        86400
        >>> tc.convert(DurationFormat.ENGLISH)
        '1 day'
    """

    def __init__(
        self, time: DURATIONTYPE, inFormat: str | DurationFormat | None = None
    ) -> None:
        """
        初始化时间转换器。

        Args:
            time: 时间值
            inFormat: 输入格式，如果为None则自动识别

        Raises:
            ValueError: 当无法确定时间格式时
        """
        self._time = time
        if isinstance(inFormat, str):
            self._Type = DurationFormat.get_format(inFormat)
        elif isinstance(inFormat, DurationFormat):
            self._Type = inFormat
        else:
            self._Type = DurationFormat.which_format(time)
        if self._Type is None:
            raise ValueError(f"无法确定时间持续格式: {time}")
        self._convType = ConversionType(self._Type)

    def convert(self, format: str | DurationFormat) -> DURATIONTYPE:
        """
        将当前时间值转换为目标格式。

        Args:
            format: 目标格式，可以是字符串或DurationFormat枚举

        Returns:
            DURATIONTYPE: 转换后的时间值

        Examples:
            >>> tc = TimeConversion("1天")
            >>> tc.convert("seconds")
            86400
            >>> tc.convert(DurationFormat.ENGLISH)
            "1 day"
        """
        if isinstance(format, str):
            target_format = DurationFormat.get_format(format)
        elif isinstance(format, DurationFormat):
            target_format = format
        else:
            raise ValueError(f"无效的目标格式: {format}")

        converter = self._convType.fit(target_format)
        return converter(self._time)

    def set_format(self, inFormat: str | DurationFormat) -> None:
        """
        设置新的输入格式。

        Args:
            inFormat: 新的输入格式

        Raises:
            ValueError: 当格式无效时
        """
        if isinstance(inFormat, str):
            new_type = DurationFormat.get_format(inFormat)
        elif isinstance(inFormat, DurationFormat):
            new_type = inFormat
        else:
            raise ValueError(f"无效的格式: {inFormat}")
        if new_type is None:
            raise ValueError(f"无法设定时间持续格式: {inFormat}")

        self._Type = new_type
        self._convType = ConversionType(self._Type)

    def get_format(self) -> DurationFormat:
        """获取当前输入格式"""
        return self._Type

    def __repr__(self) -> str:
        return f"TimeConversion({self._time}, format={self._Type})"

    def __str__(self) -> str:
        return f"TimeConversion({self._time}, format={self._Type})"


def covertChineseShort(time: DURATIONTYPE) -> str:
    """
    将时间转换为中文短格式。

    中文短格式的最小时间单位为秒，会移除"钟"字。

    Args:
        time: 时间值

    Returns:
        str: 中文短格式的时间字符串

    Examples:
        >>> covertChineseShort("1小时30分钟")
        '1小时30分'
        >>> covertChineseShort(3661)
        '1小时1分1秒'
    """
    tc = TimeConversion(time)
    middle = round(tc.convert("seconds"))
    tc = TimeConversion(middle, "seconds")
    res = tc.convert("chinese")
    res = res.replace("钟", "")
    return res


def getTimeSpan(
    baseDate: DATETIMESETS | str | None,
    interval: str = "4 days",
    direction: str = "forward",
    youtube: bool = False,
    fmt: str | None = None,
) -> tuple:
    """
    基于基准时间获取一个时间间隔。

    Args:
        baseDate: 基准时间，可以是None(使用今天)、字符串或datetime对象
        interval: 时间间隔字符串，格式如"4 days", "1 month", "2 years"
        direction: 时间计算方向
            - 'forward': 基准时间作为开始时间，向前计算间隔得到结束时间
            - 'backward': 基准时间作为结束时间，向后计算间隔得到开始时间
        youtube: 是否为youtube模式，为true时结束日期增加一天
        fmt: 日期格式字符串，如果有值则返回格式化后的字符串

    Returns:
        tuple: (开始日期, 结束日期) 或格式化后的字符串元组，始终按时间顺序返回

    Examples:
        >>> getTimeSpan("2025-01-01", "4 days")
        (DateTime(2025, 1, 1), DateTime(2025, 1, 5))
        >>> getTimeSpan("2025-01-01", "4 days", direction="backward")
        (DateTime(2024, 12, 28), DateTime(2025, 1, 1))
        >>> getTimeSpan("2025-01-01", "4 days", fmt="YYYY-MM-DD")
        ('2025-01-01', '2025-01-05')
    """
    if not isinstance(interval, str) or not interval.strip():
        raise ValueError("时间间隔不能为空")

    if direction not in ["forward", "backward"]:
        raise ValueError(f"不支持的时间方向: {direction}，应该是'forward'或'backward'")

    if baseDate is None:
        baseDate = today()
    elif isinstance(baseDate, str):
        try:
            baseDate = plm.parse(baseDate)
        except Exception as e:
            raise ValueError(f"无效的日期字符串: {baseDate} - {e}")
    else:
        try:
            baseDate = plm.instance(baseDate)
        except Exception as e:
            raise ValueError(f"无效的日期对象: {baseDate} - {e}")

    parts = interval.strip().split()
    if len(parts) != 2:
        raise ValueError(f"无效的时间间隔格式: {interval}，应该是'数字 单位'格式")

    value_str, unit = parts

    try:
        value = int(value_str)
    except ValueError:
        raise ValueError(f"时间间隔数值无效: {value_str}")

    unit = unit.lower()
    unit_mapping = {
        "day": "days",
        "days": "days",
        "week": "weeks",
        "weeks": "weeks",
        "month": "months",
        "months": "months",
        "year": "years",
        "years": "years",
    }

    if unit not in unit_mapping:
        raise ValueError(
            f"不支持的时间单位: {unit}，支持: day(s), week(s), month(s), year(s)"
        )

    delta_kwargs = {unit_mapping[unit]: value}

    if direction == "forward":
        start_date = baseDate
        end_date = baseDate.add(**delta_kwargs)
    else:
        start_date = baseDate.subtract(**delta_kwargs)
        end_date = baseDate

    if youtube:
        end_date = end_date.add(days=1)

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    if fmt:
        return (start_date.format(fmt), end_date.format(fmt))

    return (start_date, end_date)
