"""Excel report rendering with XlsxWriter: native tables, charts and conditional formats."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

import pandas as pd
import xlsxwriter
from xlsxwriter.chart import Chart
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

# Palette: one hue for single-series charts, blue/red with a neutral midpoint for
# anything measured against 50% (win rates) or zero (gold difference).
INK = "#0B0B0B"
INK_SECONDARY = "#52514E"
INK_MUTED = "#898781"
GRID = "#E1E0D9"
AXIS = "#C3C2B7"
SERIES = "#2A78D6"
NEGATIVE = "#E34948"
HEADER_FILL = "#104281"
TILE_FILL = "#F3F2EF"
NEUTRAL = "#F0EFEC"
POSITIVE_TINT = "#AAC9EF"
NEGATIVE_TINT = "#F4B6B6"
BAR_TINT = "#86B6EF"
FONT = "Calibri"

PERCENT_COLUMNS = {
    "pick_rate", "ban_rate", "win_rate", "win_rate_low", "win_rate_high",
    "kill_participation", "damage_share", "occurs_in", "share", "gold_share",
    "dataset_win_rate", "rolling_win_rate",
}  # fmt: skip
DECIMAL_COLUMNS = {
    "kda", "kills", "deaths", "assists", "cs_per_min", "vision_score_per_min", "solo_kills",
}  # fmt: skip
WHOLE_COLUMNS = {"gold_per_min", "damage_per_min", "games", "players", "gold", "champions"}
SIGNED_COLUMNS = {"gold_diff_at_15"}
DATE_COLUMNS = {"date", "game_start_utc"}

LABELS = {
    "kda": "KDA",
    "cs_per_min": "CS/min",
    "gold_per_min": "Gold/min",
    "damage_per_min": "Damage/min",
    "vision_score_per_min": "Vision/min",
    "win_rate_low": "Win rate 95% low",
    "win_rate_high": "Win rate 95% high",
    "gold_diff_at_15": "Gold diff @15",
    "dataset_win_rate": "Dataset win rate",
    "occurs_in": "Occurs in (% of games)",
    "main_position": "Main role",
}


def label(column: str) -> str:
    return LABELS.get(column, column.replace("_", " ").capitalize())


def cell_value(value: Any) -> Any:
    """Convert pandas/numpy scalars to something XlsxWriter writes correctly."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.tz_convert(None).to_pydatetime() if value.tzinfo else value.to_pydatetime()
    if hasattr(value, "item"):  # numpy scalar
        return cell_value(value.item())
    return value


class Report:
    """Wraps an XlsxWriter workbook with the report's formats and building blocks."""

    def __init__(self, path: str) -> None:
        self.book = xlsxwriter.Workbook(
            path, {"default_date_format": "yyyy-mm-dd hh:mm", "remove_timezone": True}
        )
        self.book.set_size(1600, 1000)
        base = {"font_name": FONT, "font_size": 10, "font_color": INK}
        self._base = base
        self.title = self.fmt(font_size=18, bold=True)
        self.subtitle = self.fmt(font_color=INK_SECONDARY)
        self.section = self.fmt(font_size=12, bold=True)
        self.muted = self.fmt(font_color=INK_MUTED, italic=True)
        self.header = self.fmt(
            bold=True, font_color="#FFFFFF", bg_color=HEADER_FILL, text_wrap=True,
            valign="vcenter", border=0,
        )  # fmt: skip
        self.tile_label = self.fmt(
            font_size=9, font_color=INK_SECONDARY, bg_color=TILE_FILL, valign="bottom", indent=1
        )
        self.tile_value = self.fmt(
            font_size=18, bold=True, bg_color=TILE_FILL, valign="top", indent=1, align="left"
        )
        self.wrap = self.fmt(text_wrap=True, valign="top")
        self.bold = self.fmt(bold=True)
        self._number_formats: dict[str, Format] = {}
        self._table_count = 0
        self._widths: dict[tuple[str, int], float] = {}

    def fmt(self, **props: Any) -> Format:
        return self.book.add_format({**self._base, **props})

    def number_format(self, pattern: str) -> Format:
        if pattern not in self._number_formats:
            self._number_formats[pattern] = self.fmt(num_format=pattern)
        return self._number_formats[pattern]

    def column_pattern(self, column: str) -> str | None:
        if column in PERCENT_COLUMNS:
            return "0.0%"
        if column in DECIMAL_COLUMNS:
            return "0.00"
        if column in WHOLE_COLUMNS:
            return "#,##0"
        if column in SIGNED_COLUMNS:
            return "+#,##0;-#,##0;0"
        if column in DATE_COLUMNS:
            return "yyyy-mm-dd hh:mm"
        return None

    def sheet(self, name: str, *, dashboard: bool = True) -> Worksheet:
        ws = self.book.add_worksheet(name)
        if dashboard:
            ws.hide_gridlines(2)
            ws.set_landscape()
            ws.set_paper(9)  # A4
            ws.fit_to_pages(1, 0)
        ws.set_column(0, 0, 2)
        return ws

    def close(self) -> None:
        self.book.close()

    def set_width(self, ws: Worksheet, col: int, width: float) -> None:
        """Widen a column; tables stacked in the same columns never shrink each other."""
        key = (ws.name, col)
        if width > self._widths.get(key, 0):
            self._widths[key] = width
            ws.set_column(col, col, width)

    # ------------------------------------------------------------- blocks

    def heading(self, ws: Worksheet, title: str, subtitle: str | None = None) -> int:
        ws.set_row(1, 30)
        ws.write(1, 1, title, self.title)
        if subtitle:
            ws.write(2, 1, subtitle, self.subtitle)
        return 4

    def tiles(
        self, ws: Worksheet, row: int, items: Sequence[tuple[str, Any, str | None]], width: int = 2
    ) -> int:
        """Stat tiles: label above a large value. Each tile spans ``width`` columns."""
        ws.set_row(row, 18)
        ws.set_row(row + 1, 30)
        col = 1
        for name, value, pattern in items:
            value_format = self.tile_value
            if pattern:
                value_format = self.fmt(
                    font_size=18, bold=True, bg_color=TILE_FILL, valign="top", indent=1,
                    align="left", num_format=pattern,
                )  # fmt: skip
            ws.merge_range(row, col, row, col + width - 1, name, self.tile_label)
            ws.merge_range(row + 1, col, row + 1, col + width - 1, "", value_format)
            ws.write(row + 1, col, cell_value(value), value_format)
            col += width + 1
        return row + 3

    def table(
        self,
        ws: Worksheet,
        frame: pd.DataFrame,
        row: int,
        col: int = 1,
        *,
        title: str | None = None,
        formats: Mapping[str, str] | None = None,
        widths: Mapping[str, float] | None = None,
        win_rate_columns: Iterable[str] = (),
        bar_columns: Iterable[str] = (),
        signed_columns: Iterable[str] = (),
        uniform_width: float | None = None,
        empty_message: str = "Not enough data.",
    ) -> int:
        """Write ``frame`` as a native Excel table; returns the first free row below it."""
        if title:
            ws.write(row, col, title, self.section)
            row += 1
        if frame.empty:
            ws.write(row, col, empty_message, self.muted)
            return row + 2

        formats = dict(formats or {})
        columns = []
        for name in frame.columns:
            pattern = formats.get(name) or self.column_pattern(name)
            spec: dict[str, Any] = {"header": label(name), "header_format": self.header}
            if pattern:
                spec["format"] = self.number_format(pattern)
            columns.append(spec)
        data = frame_rows(frame)
        last_row = row + len(frame)
        last_col = col + len(frame.columns) - 1
        self._table_count += 1
        ws.add_table(
            row, col, last_row, last_col,
            {
                "name": f"T{self._table_count}_{ws.name.replace(' ', '')}",
                "style": "Table Style Light 1",
                "columns": columns,
                "data": data,
            },
        )  # fmt: skip
        ws.set_row(row, 30)
        for i, name in enumerate(frame.columns):
            width = (
                (widths or {}).get(name) or uniform_width or _auto_width(frame[name], label(name))
            )
            self.set_width(ws, col + i, width)
            first, last = row + 1, last_row
            if name in win_rate_columns:
                self.diverging(ws, first, col + i, last, 0.4, 0.5, 0.6)
            if name in signed_columns:
                spread = (
                    float(frame[name].abs().quantile(0.9) or 1) if frame[name].notna().any() else 1
                )
                self.diverging(ws, first, col + i, last, -spread, 0, spread)
            if name in bar_columns:
                ws.conditional_format(
                    first, col + i, last, col + i,
                    {"type": "data_bar", "bar_color": BAR_TINT, "bar_solid": True,
                     "bar_no_border": True, "min_type": "num", "min_value": 0},
                )  # fmt: skip
        return last_row + 2

    def diverging(
        self, ws: Worksheet, first: int, col: int, last: int, low: float, mid: float, high: float
    ) -> None:
        ws.conditional_format(
            first, col, last, col,
            {
                "type": "3_color_scale",
                "min_type": "num", "min_value": low, "min_color": NEGATIVE_TINT,
                "mid_type": "num", "mid_value": mid, "mid_color": NEUTRAL,
                "max_type": "num", "max_value": high, "max_color": POSITIVE_TINT,
            },
        )  # fmt: skip

    # ------------------------------------------------------------- charts

    def chart(
        self,
        kind: str,
        title: str,
        *,
        sheet: str,
        categories: tuple[int, int, int],
        values: tuple[int, int, int],
        number_format: str = "#,##0",
        diverging: bool = False,
        axis_min: float | None = None,
        axis_max: float | None = None,
        size: tuple[int, int] = (560, 300),
        subtype: str | None = None,
        markers: bool = False,
    ) -> Chart:
        """A single-series chart; categories/values are (column, first_row, last_row)."""
        options: dict[str, Any] = {"type": kind}
        if subtype:
            options["subtype"] = subtype
        chart = self.book.add_chart(options)
        cat_col, first, last = categories
        val_col, vfirst, vlast = values
        series: dict[str, Any] = {
            "name": title,
            "categories": [sheet, first, cat_col, last, cat_col],
            "values": [sheet, vfirst, val_col, vlast, val_col],
        }
        if kind == "line":
            series["line"] = {"color": SERIES, "width": 2}
            series["marker"] = (
                {"type": "circle", "size": 7, "fill": {"color": SERIES}, "border": {"none": True}}
                if markers
                else {"type": "none"}
            )
        else:
            series["fill"] = {"color": SERIES}
            series["border"] = {"none": True}
            series["gap"] = 60
            if diverging:
                series["invert_if_negative"] = True
                series["invert_if_negative_color"] = NEGATIVE
        chart.add_series(series)
        font = {"name": FONT, "color": INK_SECONDARY, "size": 9}
        chart.set_title({"name": title, "name_font": {"name": FONT, "size": 11, "bold": True,
                                                      "color": INK}})  # fmt: skip
        chart.set_legend({"none": True})
        category_axis: dict[str, Any] = {
            "num_font": font,
            "line": {"color": AXIS},
            "major_tick_mark": "none",
        }
        value_axis: dict[str, Any] = {
            "num_font": font,
            "num_format": number_format,
            "line": {"none": True},
            "major_tick_mark": "none",
            "major_gridlines": {"visible": True, "line": {"color": GRID, "width": 0.75}},
        }
        if axis_min is not None:
            value_axis["min"] = axis_min
        elif not diverging and kind in ("bar", "column"):
            value_axis["min"] = 0  # bars always start at zero
        if axis_max is not None:
            value_axis["max"] = axis_max
        if kind == "bar":
            category_axis["reverse"] = True  # first row at the top
            category_axis["label_position"] = "low"
            chart.set_y_axis(category_axis)
            chart.set_x_axis(value_axis)
        else:
            if diverging:
                category_axis["label_position"] = "low"
            chart.set_x_axis(category_axis)
            chart.set_y_axis(value_axis)
        chart.set_chartarea({"border": {"none": True}, "fill": {"color": "#FFFFFF"}})
        chart.set_plotarea({"border": {"none": True}})
        chart.set_size({"width": size[0], "height": size[1]})
        chart.show_hidden_data()  # helper columns feeding charts are hidden
        return chart

    def write_column(
        self, ws: Worksheet, row: int, col: int, header: str, values: Iterable[Any]
    ) -> tuple[int, int]:
        """Write a hidden helper column feeding a chart; returns its first and last data row."""
        ws.set_column(col, col, None, None, {"hidden": True})
        ws.write(row, col, header, self.muted)
        last = row
        for i, value in enumerate(values, start=1):
            ws.write(row + i, col, cell_value(value))
            last = row + i
        return row + 1, last


def frame_rows(frame: pd.DataFrame) -> list[list[Any]]:
    """Rows as plain Python values: ``None`` for missing, naive UTC datetimes.

    Converts column by column, which is much faster than per-cell conversion on the
    Data sheet.
    """
    columns: list[list[Any]] = []
    for name in frame.columns:
        series = frame[name]
        is_datetime = isinstance(series.dtype, pd.DatetimeTZDtype)
        if is_datetime:
            series = series.dt.tz_convert(None)
        values = series.astype(object).where(series.notna(), None).tolist()
        if is_datetime or series.dtype == object:
            values = [cell_value(v) for v in values]
        columns.append(values)
    return [list(row) for row in zip(*columns, strict=True)]


def _auto_width(series: pd.Series, header: str) -> float:
    sample = series.dropna().head(200)
    longest = 0
    for value in sample:
        if isinstance(value, float):
            text = f"{value:,.2f}"
        elif isinstance(value, (pd.Timestamp, datetime)):
            text = "2026-01-01 00:00"
        else:
            text = str(value)
        longest = max(longest, len(text))
    return float(min(max(longest + 2, min(len(header), 25) + 2, 8), 42))
