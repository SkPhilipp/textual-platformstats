import json

from tools.common.aggregator import BaseAggregator, BaseAggregatorConfig
from tools.common.logs import log


class ArtifactoryAggregator(BaseAggregator):
    def __init__(self):
        super().__init__(BaseAggregatorConfig())
        columns = [
            'ClientAddr',
            'ClientAddr_ClientIp',
            'ClientAddr_ClientPort',
            'DownstreamContentSize',
            'DownstreamStatus',
            'Duration',
            'RequestMethod',
            'RequestPath',
            'ServiceAddr',
            'StartUTC',
            'level',
            'msg',
            'request_Uber_Trace_Id',
            'request_User_Agent',
            'time'
        ]
        for column in columns:
            self.add_column(column, 'TEXT')

    def parse_router_request_log(self, log_file):
        with open(log_file, 'r') as file:
            log.info(f'Indexing "{log_file}"')
            for line in file:
                log_entry = json.loads(line)
                log_client_addr_ip = log_entry["ClientAddr"].split(":")[0]
                log_client_addr_port = log_entry["ClientAddr"].split(":")[1]
                if self.config.filter_self and log_client_addr_ip == "127.0.0.1":
                    continue
                self.cursor.execute('''
                    INSERT INTO data (
                        ClientAddr,
                        ClientAddr_ClientIp,
                        ClientAddr_ClientPort,
                        DownstreamContentSize,
                        DownstreamStatus,
                        Duration,
                        RequestMethod,
                        RequestPath,
                        ServiceAddr,
                        StartUTC,
                        level,
                        msg,
                        request_Uber_Trace_Id,
                        request_User_Agent,
                        time
                    ) VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?
                    )
                ''', (
                    log_entry["ClientAddr"],
                    log_client_addr_ip,
                    log_client_addr_port,
                    log_entry["DownstreamContentSize"],
                    log_entry["DownstreamStatus"],
                    log_entry["Duration"],
                    log_entry["RequestMethod"],
                    log_entry["RequestPath"],
                    log_entry.get("ServiceAddr", None),
                    log_entry["StartUTC"],
                    log_entry["level"],
                    log_entry["msg"],
                    log_entry.get("request_Uber-Trace-Id", None),
                    log_entry.get("request_User-Agent", None),
                    log_entry["time"]
                ))

    def summarize(self):
        self.summarize_counts_for_key("ClientAddr_ClientIp")
        self.summarize_counts_for_key("RequestPath")
