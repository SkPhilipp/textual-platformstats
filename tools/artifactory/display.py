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
        ("l", "label_toggle", "Toggle labels"),
        ("t", "totals_toggle", "Toggle totals"),
        ("+", "time_granularity_decrease", "Decrease granularity"),
        ("-", "time_granularity_increase", "Increase granularity"),
    ]

    def __init__(self, aggregator: ArtifactoryAggregator):
        super().__init__()
        self.aggregator = aggregator
        self.show_labels = True
        self.show_totals = False
        self.time_granularity_steps = [60, 300, 900, 1800, 3600]
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
            with TabPane("Tag Stags", id="tag_pane"):
                yield DataTable(id="tag_table")

    def on_mount(self) -> None:
        data_ip, total_ip = self.aggregator.summarize_ip()
        table_ip = self.query_one("#ip_table", DataTable)
        table_ip.add_columns("Count", "ClientAddr_ClientIp")
        for count_item, counts in data_ip:
            table_ip.add_row(str(counts), count_item)

        plt = self.query_one(PlotextPlot).plt
        plt.title("Timeline of Requests")
        plt.xlabel("Time")
        plt.ylabel("Requests")
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

    def refresh_ip_plot(self):
        plot = self.query_one(PlotextPlot)
        plt = self.query_one(PlotextPlot).plt
        plt.clear_data()
        time_granularity = self.time_granularity_steps[self.time_granularity_index]
        if not self.show_totals:
            for ip, (time_periods, counts) in self.aggregator.timeline_ip(time_granularity).items():
                label = ip if self.show_labels else None
                plt.scatter(time_periods, counts, label=label, marker="hd")
        else:
            aggregated_counts = {}
            for ip, (time_periods, counts) in self.aggregator.timeline_ip(time_granularity).items():
                for time_period, count in zip(time_periods, counts):
                    if time_period in aggregated_counts:
                        aggregated_counts[time_period] += count
                    else:
                        aggregated_counts[time_period] = count
            time_periods = list(aggregated_counts.keys())
            counts = list(aggregated_counts.values())
            label = "Total" if self.show_labels else None
            plt.scatter(time_periods, counts, label=label, marker="hd")
        plot.refresh()
