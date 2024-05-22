from datetime import datetime

from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import TabbedContent, TabPane, DataTable, Footer
from textual_plotext import PlotextPlot, Plot

from tools.artifactory.aggregator import ArtifactoryAggregator


class ArtifactoryDisplayApp(App):
    CSS = """
    TabbedContent {
        height: 100%;
    }
    
    ContentSwitcher {
        height: 100%;
    }

    TabPane {
        padding: 0;
    }
    """

    BINDINGS = [
        ("l", "label_toggle", "Labels"),
        ("t", "totals_toggle", "Totals"),
        ("-", "time_granularity_decrease", "Less granular"),
        ("+", "time_granularity_increase", "More granular"),
        ("=", "time_granularity_increase"),
    ]

    def __init__(self, aggregator: ArtifactoryAggregator):
        super().__init__()
        self.aggregator = aggregator
        self.show_labels = True
        self.show_totals = False
        self.time_granularity_steps = [3600, 1800, 900, 300, 60]
        self.time_granularity_index = 2

    def compose(self) -> ComposeResult:
        with TabbedContent(initial="ip_pane"):
            with TabPane("IP Stats", id="ip_pane"):
                yield DataTable(id="ip_table")
            with TabPane("IP Plot", id="ip_pane_plot"):
                yield PlotextPlot(id="ip_plot")
            with TabPane("Path Stats", id="path_pane"):
                yield DataTable(id="path_table")
            with TabPane("Tag Stats", id="tag_pane"):
                yield DataTable(id="tag_table")
            with TabPane("Tag Plot", id="tag_pane_plot"):
                yield PlotextPlot(id="tag_plot")
        yield Footer()

    def format_bytes(self, num):
        if num is None:
            return Text("N/A", justify="right", style="grey15")
        suffix = ""
        color = "grey15"
        if num > 1024:
            num /= 1024
            suffix = "kB"
            color = "grey15"
        if num > 1024:
            num /= 1024
            suffix = "MB"
            color = "grey35"
        if num > 1024:
            num /= 1024
            suffix = "GB"
            color = "grey62"
            if num > 10:
                color = "white"
        if num > 1024:
            num /= 1024
            suffix = "TB"
            color = "red"
        return Text(f"{num:.2f}{suffix}", justify="right", style=color)

    def format_num(self, num):
        if num is None:
            return Text("N/A", justify="right", style="grey15")
        suffix = ""
        color = "grey35"
        if num > 1000:
            num /= 1000
            suffix = "k"
            color = "grey82"
        if num > 1000:
            num /= 1000
            suffix = "M"
            color = "white"
        if num > 1000:
            num /= 1000
            suffix = "B"
            color = "red"
        return Text(f"{num:.2f}{suffix}", justify="right", style=color)

    def make_summary_table(self, name, table: DataTable, data: any, sort_by=1):
        table.add_columns(name, "Total Reqs", "Total Down",
                          "Peak Req/1s", "Peak Req/1m", "Peak Req/1h",
                          "Peak Down/1s", "Peak Down/1m", "Peak Down/1h")
        total_requests = sum(count for _, count, _, _, _, _, _, _, _ in data if count is not None)
        total_downloads = sum(download for _, _, download, _, _, _, _, _, _ in data if download is not None)
        table.add_row("Total", self.format_num(total_requests), self.format_bytes(total_downloads),
                      "", "", "",
                      "", "", "")
        sorted_data = sorted(data, key=lambda row: row[sort_by], reverse=True)
        for count_item, sum_reqs, sum_download, peak_1s, peak_1m, peak_1h, peak_down_1s, peak_down_1m, peak_down_1h in sorted_data:
            table.add_row(count_item, self.format_num(sum_reqs), self.format_bytes(sum_download),
                          self.format_num(peak_1s), self.format_num(peak_1m), self.format_num(peak_1h),
                          self.format_bytes(peak_down_1s), self.format_bytes(peak_down_1m), self.format_bytes(peak_down_1h))

    def on_mount(self) -> None:
        data_ip = self.aggregator.summarize_ip()
        table_ip = self.query_one("#ip_table", DataTable)
        self.make_summary_table("IP", table_ip, data_ip)
        self.refresh_ip_plot()
        table_path = self.query_one("#path_table", DataTable)
        data_path = self.aggregator.summarize_path()
        self.make_summary_table("Path", table_path, data_path, sort_by=2)
        data_tag = self.aggregator.summarize_tag()
        table_tag = self.query_one("#tag_table", DataTable)
        self.make_summary_table("Tag", table_tag, data_tag)
        self.refresh_tag_plot()

    def action_label_toggle(self):
        self.show_labels = not self.show_labels
        self.refresh_ip_plot()
        self.refresh_tag_plot()

    def action_totals_toggle(self):
        self.show_totals = not self.show_totals
        self.refresh_ip_plot()
        self.refresh_tag_plot()

    def action_time_granularity_decrease(self):
        if self.time_granularity_index > 0:
            self.time_granularity_index -= 1
            self.refresh_ip_plot()
            self.refresh_tag_plot()

    def action_time_granularity_increase(self):
        if self.time_granularity_index < len(self.time_granularity_steps) - 1:
            self.time_granularity_index += 1
            self.refresh_ip_plot()
            self.refresh_tag_plot()

    def refresh_plot(self, plot, values, interval):
        plt: Plot = plot.plt
        plt.xlabel("Time")
        plt.ylabel(f"Requests per {interval}s")
        plt.clear_data()
        plt.date_form("H:M:S")
        if not self.show_totals:
            for key, (time_periods, counts) in values:
                label = key if self.show_labels else None
                time_periods = [datetime.fromtimestamp(tp) for tp in time_periods]
                time_periods = plt.datetimes_to_string(time_periods)
                plt.plot(time_periods, counts, marker='braille', label=label)
        else:
            aggregated_counts = {}
            for key, (time_periods, counts) in values:
                for time_period, count in zip(time_periods, counts):
                    if time_period in aggregated_counts:
                        aggregated_counts[time_period] += count
                    else:
                        aggregated_counts[time_period] = count
            time_periods = list(aggregated_counts.keys())
            time_periods = [datetime.fromtimestamp(tp) for tp in time_periods]
            time_periods = plt.datetimes_to_string(time_periods)

            counts = list(aggregated_counts.values())
            label = "Total" if self.show_labels else None
            plt.plot(time_periods, counts, marker='braille', label=label)
        plt.grid(0, 1)
        plot.refresh()

    def refresh_ip_plot(self):
        interval = self.time_granularity_steps[self.time_granularity_index]
        plot = self.query_one("#ip_plot", PlotextPlot)
        values = self.aggregator.timeline_ip(interval).items()
        self.refresh_plot(plot, values, interval)

    def refresh_tag_plot(self):
        interval = self.time_granularity_steps[self.time_granularity_index]
        plot = self.query_one("#tag_plot", PlotextPlot)
        values = self.aggregator.timeline_tag(interval).items()
        self.refresh_plot(plot, values, interval)
