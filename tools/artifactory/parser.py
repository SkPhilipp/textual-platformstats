import json


class Aggregator:
    def __init__(self, filter_self=True):
        self.filter_self = filter_self
        self.router_request_data = {
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
            "time": []
        }

    def parse_router_request_log(self, log_file):
        with open(log_file, 'r') as file:
            for line in file:
                log_entry = json.loads(line)
                log_client_addr_ip = log_entry["ClientAddr"].split(":")[0]
                log_client_addr_port = log_entry["ClientAddr"].split(":")[1]
                if self.filter_self and log_client_addr_ip == "127.0.0.1":
                    continue
                self.router_request_data["ClientAddr"].append(log_entry["ClientAddr"])
                self.router_request_data["ClientAddr:ClientIp"].append(log_client_addr_ip)
                self.router_request_data["ClientAddr:ClientPort"].append(log_client_addr_port)
                self.router_request_data["DownstreamContentSize"].append(log_entry["DownstreamContentSize"])
                self.router_request_data["DownstreamStatus"].append(log_entry["DownstreamStatus"])
                self.router_request_data["Duration"].append(log_entry["Duration"])
                self.router_request_data["RequestMethod"].append(log_entry["RequestMethod"])
                self.router_request_data["RequestPath"].append(log_entry["RequestPath"])
                self.router_request_data["ServiceAddr"].append(log_entry.get("ServiceAddr", None))
                self.router_request_data["StartUTC"].append(log_entry["StartUTC"])
                self.router_request_data["level"].append(log_entry["level"])
                self.router_request_data["msg"].append(log_entry["msg"])
                self.router_request_data["request_Uber-Trace-Id"].append(log_entry.get("request_Uber-Trace-Id", None))
                self.router_request_data["request_User-Agent"].append(log_entry.get("request_User-Agent", None))
                self.router_request_data["time"].append(log_entry["time"])

    def summarize_router_request_data(self):
        counts_client_ip = {}
        for client_ip in self.router_request_data["ClientAddr:ClientIp"]:
            counts_client_ip[client_ip] = counts_client_ip.get(client_ip, 0) + 1

        print("Unique Client IPs: (top 20)")
        for client_ip, count in sorted(counts_client_ip.items(), key=lambda item: item[1], reverse=True)[:20]:
            print(f"{client_ip}: {count}")

        counts_request_path = {}
        for request_path in self.router_request_data["RequestPath"]:
            counts_request_path[request_path] = counts_request_path.get(request_path, 0) + 1
        print("Unique Request Paths: (top 20)")
        for request_path, count in sorted(counts_request_path.items(), key=lambda item: item[1], reverse=True)[:20]:
            print(f"{request_path}: {count}")
