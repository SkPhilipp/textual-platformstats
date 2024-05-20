import json
from typing import Tuple, List

from tools.common.aggregator import BaseAggregator, BaseAggregatorConfig
from tools.common.logs import log


class ArtifactoryAggregator(BaseAggregator):
    def __init__(self):
        super().__init__(BaseAggregatorConfig())
        self.run_sql('db_artifactory.sql')

    def parse_router_request_log(self, log_file):
        with open(log_file, 'r') as file:
            log.info(f'Indexing "{log_file}"')
            for line in file:
                log_entry = json.loads(line)
                log_client_addr_ip = log_entry["ClientAddr"].split(":")[0]
                log_client_addr_port = log_entry["ClientAddr"].split(":")[1]
                if self.config.filter_self and log_client_addr_ip == "127.0.0.1":
                    continue
                # if ClientAddr and time are unique, then we can insert the log entry
                if self.cursor.execute('''
                    SELECT COUNT(*)
                    FROM data_artifactory
                    WHERE ClientAddr = ? AND time = ?
                ''', (log_entry["ClientAddr"], log_entry["time"])).fetchone()[0] > 0:
                    continue

                self.cursor.execute('''
                    INSERT INTO data_artifactory (
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
        self.connection.commit()

    def summarize_ip(self) -> List[Tuple]:
        self.cursor.execute('''
            SELECT DISTINCT ClientAddr_ClientIp
            FROM data_artifactory
        ''')
        all_ips = [row[0] for row in self.cursor.fetchall()]
        results = []
        for ip in all_ips:
            self.cursor.execute('''
                SELECT COUNT(*), SUM(DownstreamContentSize), 0, 0, 0, 0, 0, 0
                FROM data_artifactory
                WHERE ClientAddr_ClientIp = ?
            ''', (ip,))
            row = self.cursor.fetchone()
            results.append((ip, *row))
            print((ip, *row))
        return results

    def summarize_path(self) -> Tuple[list, int]:
        top_n = 10
        self.cursor.execute(f'''
            SELECT RequestPath, COUNT(*)
            FROM data_artifactory
            GROUP BY RequestPath
            ORDER BY COUNT(*) DESC
            LIMIT ?
        ''', (top_n,))
        rows = self.cursor.fetchall()
        total = sum(count for _, count in rows)
        return rows, total

    def summarize_tag(self) -> Tuple[list, int]:
        top_n = 10
        self.cursor.execute(f'''
            SELECT _tag, COUNT(*)
            FROM data_artifactory
            GROUP BY _tag
            ORDER BY COUNT(*) DESC
            LIMIT ?
        ''', (top_n,))
        rows = self.cursor.fetchall()
        total = sum(count for _, count in rows)
        return rows, total

    def timeline_ip(self, interval: int) -> dict:
        # Get all unique time periods
        self.cursor.execute(f'''
            SELECT DISTINCT strftime('%s', time) / ? * ?
            FROM data_artifactory
        ''', (interval, interval))
        all_time_periods = [row[0] for row in self.cursor.fetchall()]

        # Get all unique IPs
        self.cursor.execute('''
            SELECT DISTINCT ClientAddr_ClientIp
            FROM data_artifactory
        ''')
        all_ips = [row[0] for row in self.cursor.fetchall()]

        # Get the data from the database
        self.cursor.execute(f'''
            SELECT strftime('%s', time) / ? * ?, ClientAddr_ClientIp, COUNT(*)
            FROM data_artifactory
            GROUP BY strftime('%s', time) / ?, ClientAddr_ClientIp
            ORDER BY strftime('%s', time) / ?
        ''', (interval, interval, interval, interval))
        data_ip_timeline = self.cursor.fetchall()
        data_dict = {(time_period, ip): count for time_period, ip, count in data_ip_timeline}
        ip_data = {ip: ([], []) for ip in all_ips}
        for time_period in all_time_periods:
            for ip in all_ips:
                count = data_dict.get((time_period, ip), 0)
                ip_data[ip][0].append(time_period)
                ip_data[ip][1].append(count)

        return ip_data
