#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from time import sleep
from producer import KafkaProducer
from multiprocessing import Process
import arrow
import json

def start_produce(config, max_ct, start_index, step_size, silent_mode):
    # Create Producer instance
    topic = config.pop('topic')
    producer = KafkaProducer(config, topic=topic)
    
    msg_key = [str(x) for x in range(start_index, max_ct, step_size)]

    if not silent_mode:
        for i in range(len(msg_key)):
            do_sleep(i)
            msg_val = json.dumps({'value': f'v_{msg_key[i]}', 'timestamp': arrow.now().timestamp()}) 
            producer.perform_produce(msg_key[i], msg_val)
    else:
        for i in range(len(msg_key)):
            do_sleep(i)
            msg_val = json.dumps({'value': f'v_{msg_key[i]}', 'timestamp': arrow.now().timestamp()}) 
            producer.perform_produce(msg_key[i], msg_val)

    # Block until the messages are sent.
    producer.poll(10)
    producer.flush()

def do_sleep(i):
    pass
    # sleep(0.000008)

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('-s', help='silent mode', action='store_true')
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

    silent_mode = True if args.s else False
    proc = []
    for i in range(args.num_proc):
        proc.append(Process(target=start_produce, args=(config, args.msg_count, i, args.num_proc, silent_mode)))
        proc[-1].start()
    
    for p in proc:
        p.join()
