#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from producer import KafkaProducer
from multiprocessing import Process
import arrow
import json

def start_produce(config, max_ct, start_index, step_size):
    # Create Producer instance
    producer = KafkaProducer(config, topic=config.pop('topic'))
    
    msg_key = [str(x) for x in range(start_index, max_ct, step_size)]
    msg_val = [json.dumps({'value': f'v_{x}', 'timestamp': arrow.now().timestamp()}) for x in msg_key]

    for i in range(len(msg_key)):
        producer.perform_produce(msg_key[i], msg_val[i])

    # Block until the messages are sent.
    producer.poll(10)
    producer.flush()


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('num_proc', type=int)
    parser.add_argument('msg_count', type=int)
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['producer'])
    print(config)
    proc = []
    for i in range(args.num_proc):
        proc.append(Process(target=start_produce, args=(config, args.msg_count, i, args.num_proc)))
        proc[-1].start()
    
    for p in proc:
        p.join()
