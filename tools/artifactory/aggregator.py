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

    def summarize(self, column) -> List[Tuple]:
        self.cursor.execute(f'''
            SELECT DISTINCT {column}
            FROM data_artifactory
        ''')
        all_entries = [row[0] for row in self.cursor.fetchall()]
        results = []
        for entry in all_entries:
            self.cursor.execute(f'''
                SELECT COUNT(*), SUM(DownstreamContentSize)
                FROM data_artifactory
                WHERE {column} = ?
            ''', (entry,))
            total_requests, total_downloads = self.cursor.fetchone()
            self.cursor.execute(f'''
                SELECT MAX(requests), MAX(downloads)
                FROM (
                    SELECT COUNT(*) as requests, SUM(DownstreamContentSize) as downloads
                    FROM data_artifactory
                    WHERE {column} = ?
                    GROUP BY strftime('%s', time)
                )
            ''', (entry,))
            peak_req_per_sec, peak_down_per_sec = self.cursor.fetchone()
            self.cursor.execute(f'''
                SELECT MAX(requests), MAX(downloads)
                FROM (
                    SELECT COUNT(*) as requests, SUM(DownstreamContentSize) as downloads
                    FROM data_artifactory
                    WHERE {column} = ?
                    GROUP BY strftime('%M', time)
                )
            ''', (entry,))
            peak_req_per_min, peak_down_per_min = self.cursor.fetchone()
            self.cursor.execute(f'''
                SELECT MAX(requests), MAX(downloads)
                FROM (
                    SELECT COUNT(*) as requests, SUM(DownstreamContentSize) as downloads
                    FROM data_artifactory
                    WHERE {column} = ?
                    GROUP BY strftime('%H', time)
                )
            ''', (entry,))
            peak_req_per_hour, peak_down_per_hour = self.cursor.fetchone()
            results.append((entry, total_requests, total_downloads, peak_req_per_sec, peak_req_per_min, peak_req_per_hour, peak_down_per_sec, peak_down_per_min,
                            peak_down_per_hour))
        return results

    def summarize_ip(self) -> List[Tuple]:
        return self.summarize('ClientAddr_ClientIp')

    def summarize_path(self) -> List[Tuple]:
        return self.summarize('RequestPath')

    def summarize_tag(self) -> List[Tuple]:
        return self.summarize('_tag')

    def timeline_ip(self, interval: int) -> dict:
        self.cursor.execute(f'''
            SELECT DISTINCT strftime('%s', time) / ? * ?
            FROM data_artifactory
        ''', (interval, interval))
        all_time_periods = [row[0] for row in self.cursor.fetchall()]

        self.cursor.execute('''
            SELECT DISTINCT ClientAddr_ClientIp
            FROM data_artifactory
        ''')
        all_ips = [row[0] for row in self.cursor.fetchall()]

        self.cursor.execute(f'''
            SELECT strftime('%s', time) / ? * ?, ClientAddr_ClientIp, COUNT(*)
            FROM data_artifactory
            GROUP BY strftime('%s', time) / ?, ClientAddr_ClientIp
            ORDER BY strftime('%s', time) / ?
        ''', (interval, interval, interval, interval))
        data_timeline = self.cursor.fetchall()
        data_dict = {(time_period, ip): count for time_period, ip, count in data_timeline}
        ip_data = {ip: ([], []) for ip in all_ips}
        for time_period in all_time_periods:
            for ip in all_ips:
                count = data_dict.get((time_period, ip), 0)
                ip_data[ip][0].append(time_period)
                ip_data[ip][1].append(count)

        return ip_data

    def timeline_tag(self, interval: int) -> dict:
        self.cursor.execute(f'''
            SELECT DISTINCT strftime('%s', time) / ? * ?
            FROM data_artifactory
        ''', (interval, interval))
        all_time_periods = [row[0] for row in self.cursor.fetchall()]
        self.cursor.execute('''
            SELECT DISTINCT _tag
            FROM data_artifactory
        ''')
        all_tags = [row[0] for row in self.cursor.fetchall()]
        self.cursor.execute(f'''
            SELECT strftime('%s', time) / ? * ?, _tag, COUNT(*)
            FROM data_artifactory
            GROUP BY strftime('%s', time) / ?, _tag
            ORDER BY strftime('%s', time) / ?
        ''', (interval, interval, interval, interval))
        data_timeline = self.cursor.fetchall()
        data_dict = {(time_period, ip): count for time_period, ip, count in data_timeline}
        tag_data = {tag: ([], []) for tag in all_tags}
        for time_period in all_time_periods:
            for ip in all_tags:
                count = data_dict.get((time_period, ip), 0)
                tag_data[ip][0].append(time_period)
                tag_data[ip][1].append(count)
        return tag_data
