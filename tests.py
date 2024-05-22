#!/usr/bin/env python
from datetime import datetime

from rich.console import Console

from retextual.gradient import GradientPurpleYellow, GradientBlackWhite, GradientRedGreen
from retextual.stylerange import StyleRange, SelectorTime, SelectorLogarithmic, SelectorLinear


def main():
    style_range = StyleRange(GradientRedGreen, SelectorLinear(0, 20))
    console = Console()
    console.print(style_range.apply("-1"))
    console.print(style_range.apply("0"))
    console.print(style_range.apply("1"))
    console.print(style_range.apply("9"))
    console.print(style_range.apply("10"))
    console.print(style_range.apply("19"))
    console.print(style_range.apply("20"))
    console.print(style_range.apply("21"))

    style_range = StyleRange(GradientBlackWhite, SelectorLogarithmic(1, 100))
    console.print(style_range.apply("0"))
    console.print(style_range.apply("1"))
    console.print(style_range.apply("10"))
    console.print(style_range.apply("100"))
    console.print(style_range.apply("101"))

    style_range = StyleRange(GradientPurpleYellow, SelectorTime(datetime(2021, 1, 1), datetime(2021, 12, 31)))
    console.print(style_range.apply("2020-12-31 23:59:59"))
    console.print(style_range.apply("2021-01-01 00:00:00"))
    console.print(style_range.apply("2021-06-15 12:00:00"))
    console.print(style_range.apply("2021-12-31 23:59:59"))
    console.print(style_range.apply("2022-01-01 00:00:00"))


if __name__ == "__main__":
    main()
