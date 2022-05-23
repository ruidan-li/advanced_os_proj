#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from time import sleep
from producer import KafkaProducer
from multiprocessing import Process
import arrow
import json
import  datetime

def read_sleep_time():
    with open("../sleeptime.txt") as f:
        return float(f.readline().strip())


def update_and_sleep(last_time, sleep_time, read_interval):
    if (arrow.now() - last_time) >= read_interval:
        sleep_time = read_sleep_time()
        # print("producer sleep time:", sleep_time)
        sleep(sleep_time)
        return arrow.now(), sleep_time
    else:
        sleep(sleep_time)
        return last_time, sleep_time


def start_produce(config, max_ct, start_index, step_size, silent_mode):
    # Create Producer instance
    topic = config.pop('topic')
    number_of_partitions = config.pop('partition.num')
    producer = KafkaProducer(config, topic=topic, num_pa=number_of_partitions)
    last_time = arrow.now()
    sleep_time = read_sleep_time()
    read_sleep_time_interval = datetime.timedelta(seconds=5)

    # msg_key = [str(x) for x in range(start_index, max_ct, step_size)]
    # msg_key = [str(x) for x in range(max_ct)]
    producer.flush()
    print("num of keys of this producer:", max_ct)
    if not silent_mode:
        for i in range(max_ct):
            last_time, sleep_time = update_and_sleep(last_time, sleep_time, read_sleep_time_interval)

            msg_key = str(i)
            msg_val = json.dumps({'value': f'v_{msg_key}', 'timestamp': arrow.now().timestamp()}) 
            producer.perform_produce(msg_key, msg_val)
    else:
        for i in range(max_ct):
            last_time, sleep_time = update_and_sleep(last_time, sleep_time, read_sleep_time_interval)

            msg_key = str(i)
            msg_val = json.dumps({'value': f'v_{msg_key}', 'timestamp': arrow.now().timestamp()}) 
            producer.perform_produce(msg_key, msg_val)

    # Block until the messages are sent.
    producer.poll(10)
    producer.flush()
    
    print(producer.get_stats())

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
