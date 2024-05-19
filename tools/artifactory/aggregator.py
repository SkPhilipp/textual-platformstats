import json

from tools.common.aggregator import BaseAggregator, BaseAggregatorConfig


class Aggregator(BaseAggregator):
    def __init__(self):
        super().__init__(BaseAggregatorConfig())
        self.data = {
            "ClientAddr": [],
            "ClientAddr:ClientIp": [],
            "ClientAddr:ClientPort": [],
            "DownstreamContentSize": [],
            "DownstreamStatus": [],
            "Duration": [],
            "RequestMethod": [],
            "RequestPath": [],
            "ServiceAddr": [],
            "StartUTC": [],
            "level": [],
            "msg": [],
            "request_Uber-Trace-Id": [],
            "request_User-Agent": [],
            "time": [],
        }

    def parse_router_request_log(self, log_file):
        with open(log_file, 'r') as file:
            for line in file:
                log_entry = json.loads(line)
                log_client_addr_ip = log_entry["ClientAddr"].split(":")[0]
                log_client_addr_port = log_entry["ClientAddr"].split(":")[1]
                if self.config.filter_self and log_client_addr_ip == "127.0.0.1":
                    continue
                self.data["ClientAddr"].append(log_entry["ClientAddr"])
                self.data["ClientAddr:ClientIp"].append(log_client_addr_ip)
                self.data["ClientAddr:ClientPort"].append(log_client_addr_port)
                self.data["DownstreamContentSize"].append(log_entry["DownstreamContentSize"])
                self.data["DownstreamStatus"].append(log_entry["DownstreamStatus"])
                self.data["Duration"].append(log_entry["Duration"])
                self.data["RequestMethod"].append(log_entry["RequestMethod"])
                self.data["RequestPath"].append(log_entry["RequestPath"])
                self.data["ServiceAddr"].append(log_entry.get("ServiceAddr", None))
                self.data["StartUTC"].append(log_entry["StartUTC"])
                self.data["level"].append(log_entry["level"])
                self.data["msg"].append(log_entry["msg"])
                self.data["request_Uber-Trace-Id"].append(log_entry.get("request_Uber-Trace-Id", None))
                self.data["request_User-Agent"].append(log_entry.get("request_User-Agent", None))
                self.data["time"].append(log_entry["time"])

    def summarize(self):
        self.summarize_counts_for_key("ClientAddr:ClientIp")
        self.summarize_counts_for_key("RequestPath")
