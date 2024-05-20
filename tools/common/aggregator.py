import sqlite3


class BaseAggregatorConfig:
    def __init__(self, filter_self=True):
        self.filter_self = filter_self


class BaseAggregator[T: BaseAggregatorConfig]:
    def __init__(self, config: T):
        self.config = config
        self.connection = sqlite3.connect('platformstats.db')
        self.cursor = self.connection.cursor()

    def run_sql(self, file_path):
        with open(file_path, 'r') as sql_file:
            sql_script = sql_file.read()
        self.cursor.executescript(sql_script)
        self.connection.commit()
