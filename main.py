#!/usr/bin/env python
import argparse

from tools.artifactory.aggregator import ArtifactoryAggregator
from tools.artifactory.display import ArtifactoryDisplayApp
from tools.artifactory.unpack import unpack
from tools.common.logs import log


def main():
    parser = argparse.ArgumentParser(description='Get platform insights.')
    parser.add_argument('application', help='The application to analyze.')
    parser.add_argument('zipfile', help='The path to a zip file.')

    args = parser.parse_args()

    if args.application == 'artifactory':
        log_files = unpack(args.zipfile)
        aggregator = ArtifactoryAggregator()
        for log_file in log_files:
            aggregator.parse_router_request_log(log_file)
        app = ArtifactoryDisplayApp(aggregator)
        app.run()
        return

    log.error(f'Unknown application: {args.application}')


if __name__ == "__main__":
    main()
