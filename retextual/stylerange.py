import math
from datetime import datetime
from typing import Union

from rich.text import Text


class StyleRange:
    def __init__(self, styles, function):
        self.styles = styles
        self.function = function

    def apply(self, content: any):
        style_index = self.function(content, len(self.styles))
        style = self.styles[style_index]
        return Text(text=content, style=style)


class SelectorLinear:
    def __init__(self, start_inclusive: Union[int, float], end_exclusive: Union[int, float]):
        self.start = start_inclusive
        self.end = end_exclusive

    def __call__(self, content, num_styles):
        value_num = float(content)
        index = int(num_styles * (value_num - self.start) / (self.end - self.start))
        return max(0, min(num_styles - 1, index))


class SelectorLogarithmic:
    def __init__(self, start_inclusive: Union[int, float], end_exclusive: Union[int, float]):
        self.start = start_inclusive
        self.end = end_exclusive

    def __call__(self, content, num_styles):
        value_num = float(content)
        if value_num <= self.start:
            value_num = self.start + 1
        index = int(num_styles * math.log(value_num - self.start) / math.log(self.end - self.start))
        return max(0, min(num_styles - 1, index))


class SelectorTime:
    def __init__(self, start_inclusive: datetime = None, end_exclusive: datetime = None, date_time_format: str = "%Y-%m-%d %H:%M:%S"):
        self.start = start_inclusive or datetime.now()
        self.end = end_exclusive or datetime.now()
        self.format = date_time_format

    def __call__(self, content, num_styles):
        time = datetime.strptime(content, self.format)
        index = int((time - self.start) / (self.end - self.start) * num_styles)
        return max(0, min(num_styles - 1, index))
