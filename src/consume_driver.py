#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from consumer import KafkaConsumer
from multiprocessing import Process
# import os

def start_consume(config, sample_interval):
    # print(os.getpid())
    timeout = int(config.pop('timeout'))
    # Create Producer instance
    consumer = KafkaConsumer(config, topic=[config.pop('topic')], sample_interval=sample_interval)

    consumer.start()
    consumer.basic_consume(timeout)



if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('num_proc', type=int)
    parser.add_argument('sample_interval', type=int)
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['producer'])    # for topic
    config.update(config_parser['consumer'])
    config.pop('partition.num')
    print(config)
    proc = []
    for i in range(args.num_proc):
        proc.append(Process(target=start_consume, args=(config, args.sample_interval)))
        proc[-1].start()
    
    for p in proc:
        p.join()
