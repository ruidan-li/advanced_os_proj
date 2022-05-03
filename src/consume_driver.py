#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from consumer import KafkaConsumer

def start_consume(config):
    timeout = int(config.pop('timeout'))
    # Create Producer instance
    consumer = KafkaConsumer(config, topic=[config.pop('topic')])

    consumer.start()
    consumer.basic_consume(timeout)



if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['producer'])    # for topic
    config.update(config_parser['consumer'])
    print(config)
    start_consume(config)
