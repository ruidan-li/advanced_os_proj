from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from time import sleep
from producer import KafkaProducer
from multiprocessing import Process
import arrow
import json
from sys import argv
from confluent_kafka.admin import AdminClient, NewTopic

parser = ArgumentParser()
parser.add_argument('-s', help='silent mode', action='store_true')
parser.add_argument('config_file', type=FileType('r'))
parser.add_argument('rf', type=int)
parser.add_argument('pa', type=int)
parser.add_argument('co', type=int)
parser.add_argument('po', type=int)
parser.add_argument('vr', type=int)
args = parser.parse_args()

# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])
config.update(config_parser['producer'])
print(args.rf)


tp = f"topic-rf{args.rf}-pa{args.pa}-co{args.co}-po{args.po}-vr{args.vr}"
a = AdminClient({'bootstrap.servers': config["bootstrap.servers"]})

topic_metadata = a.list_topics()
if topic_metadata.topics.get(tp) is not None:
    print("Topic with name {} exists, will delete this topic".format(tp))
    fs = a.delete_topics([tp], operation_timeout=30)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic with name {} is deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))

    print("Will wait for 3 seconds before creating topic")
    sleep(3.0)

new_topics = [NewTopic(topic, num_partitions=args.pa, replication_factor=args.rf) for topic in [tp]]
# Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

# Call create_topics to asynchronously create topics. A dict
# of <topic,future> is returned.
fs = a.create_topics(new_topics)

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
