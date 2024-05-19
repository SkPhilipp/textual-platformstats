import sqlite3
from typing import List

from rich.console import Console
from rich.table import Table


class BaseAggregatorConfig:
    def __init__(self, filter_self=True):
        self.filter_self = filter_self


class BaseAggregator[T: BaseAggregatorConfig]:
    def __init__(self, config: T):
        self.config = config
        self.columns: List[str] = []
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        self.cursor.execute("""CREATE TABLE data (
            _id INTEGER PRIMARY KEY
        )""")
        self.add_column('_tag', 'TEXT')

    def add_column(self, column_name, column_type):
        self.columns.append(column_name)
        self.cursor.execute(f'ALTER TABLE data ADD COLUMN {column_name} {column_type}')
        self.cursor.execute(f'CREATE INDEX idx_{column_name} ON data ({column_name})')

    def summarize_counts_for_key(self, key, top_n=10):
        if key not in self.columns:
            raise Exception(f"Key '{key}' not found in fields.")
        self.cursor.execute(f'''
            SELECT {key}, COUNT(*) 
            FROM data 
            GROUP BY {key} 
            ORDER BY COUNT(*) DESC 
            LIMIT ?
        ''', (top_n,))
        rows = self.cursor.fetchall()
        total = sum(count for _, count in rows)
        table = Table(title=f"Top {top_n} {key} values (total: {total})")
        table.add_column("Count", justify="right")
        table.add_column(key)
        for count_item, counts in rows:
            table.add_row(str(counts), count_item)
        console = Console()
        console.print(table)
