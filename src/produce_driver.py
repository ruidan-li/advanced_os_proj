#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from producer import KafkaProducer

def start_produce(config):
    # Create Producer instance
    producer = KafkaProducer(config, topic=config.pop('topic'))
    
    msg_key = ['a', 'b', 'c', 'd', 'e']
    msg_val = ['v_a', 'v_b', 'v_c', 'v_d', 'v_e']

    for i in range(len(msg_key)):
        producer.perform_produce(msg_key[i], msg_val[i])

    # Block until the messages are sent.
    producer.poll(10)
    producer.flush()


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
    config.update(config_parser['producer'])
    print(config)
    start_produce(config)
