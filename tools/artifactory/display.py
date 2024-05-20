from datetime import datetime

from textual.app import App, ComposeResult
from textual.widgets import TabbedContent, TabPane, DataTable, Footer
from textual_plotext import PlotextPlot, Plot

from tools.artifactory.aggregator import ArtifactoryAggregator


class ArtifactoryDisplayApp(App):
    CSS = """
    TabbedContent {
        height: 100%;
        width: 100%;
    }
    
    TabPane {
        padding: 0;
        height: 20;
    }
    
    DataTable {
        height: 100%;
        width: 100%;
    }
    
    PlotextPlot {
        height: 100%;
        width: 100%;
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
                yield Footer()
            with TabPane("Path Stats", id="path_pane"):
                yield DataTable(id="path_table")
            with TabPane("Tag Stats", id="tag_pane"):
                yield DataTable(id="tag_table")

    def on_mount(self) -> None:
        data_ip, total_ip = self.aggregator.summarize_ip()
        table_ip = self.query_one("#ip_table", DataTable)
        table_ip.add_columns("Count", "ClientAddr_ClientIp")
        for count_item, counts in data_ip:
            table_ip.add_row(str(counts), count_item)

        plt = self.query_one(PlotextPlot).plt
        self.refresh_ip_plot()

        data_path, total_path = self.aggregator.summarize_path()
        table_path = self.query_one("#path_table", DataTable)
        table_path.add_columns("Count", "RequestPath")
        for count_item, counts in data_path:
            table_path.add_row(str(counts), count_item)

        data_tag, total_tag = self.aggregator.summarize_tag()
        table_tag = self.query_one("#tag_table", DataTable)
        table_tag.add_columns("Count", "Tag")
        for count_item, counts in data_tag:
            table_tag.add_row(str(counts), count_item)

    def action_label_toggle(self):
        self.show_labels = not self.show_labels
        self.refresh_ip_plot()

    def action_totals_toggle(self):
        self.show_totals = not self.show_totals
        self.refresh_ip_plot()

    def action_time_granularity_decrease(self):
        if self.time_granularity_index > 0:
            self.time_granularity_index -= 1
            self.refresh_ip_plot()

    def action_time_granularity_increase(self):
        if self.time_granularity_index < len(self.time_granularity_steps) - 1:
            self.time_granularity_index += 1
            self.refresh_ip_plot()

    def refresh_plot(self, plot, values, interval):
        plt: Plot = self.query_one(PlotextPlot).plt
        plt.xlabel("Time")
        plt.ylabel(f"Requests per {interval}s")
        plt.clear_data()
        plt.date_form("d/m/Y H:M:S")
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
        plot = self.query_one(PlotextPlot)
        values = self.aggregator.timeline_ip(interval).items()
        self.refresh_plot(plot, values, interval)
