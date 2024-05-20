from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import TabbedContent, TabPane, DataTable

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
    """

    def __init__(self, aggregator: ArtifactoryAggregator):
        super().__init__()
        self.aggregator = aggregator

    def compose(self) -> ComposeResult:
        with TabbedContent(initial="ip_pane"):
            with TabPane("IPs", id="ip_pane"):
                with Horizontal():
                    yield DataTable(id="ip_table")
                    yield DataTable(id="ip_table_timeline")
            with TabPane("Paths", id="path_pane"):
                yield DataTable(id="path_table")
            with TabPane("Tags", id="tag_pane"):
                yield DataTable(id="tag_table")

    def on_mount(self) -> None:
        data_ip, total_ip = self.aggregator.summarize_ip()
        table_ip = self.query_one("#ip_table", DataTable)
        table_ip.add_columns("Count", "ClientAddr_ClientIp")
        for count_item, counts in data_ip:
            table_ip.add_row(str(counts), count_item)

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
