#!/usr/bin/env python
import argparse

from tools.artifactory.aggregator import Aggregator
from tools.artifactory.unpack import unpack


def main():
    parser = argparse.ArgumentParser(description='Get platform insights.')
    parser.add_argument('application', help='The application to analyze.')
    parser.add_argument('zipfile', help='The path to a zip file.')

    args = parser.parse_args()

    if args.application == 'artifactory':
        log_files = unpack(args.zipfile)
        print(f'Found {len(log_files)} log files.')
        router_request_log_files = [file for file in log_files if 'router-request.log' in file]
        print(f'Found {len(router_request_log_files)} "router-request.log" files.')
        aggregator = Aggregator()
        print('Loading log files...')
        for log_file in router_request_log_files:
            aggregator.parse_router_request_log(log_file)
        aggregator.summarize()
        return

    print(f'Unknown application: {args.application}')


if __name__ == "__main__":
    main()
