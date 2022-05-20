import sys
from confluent_kafka.admin import AdminClient, NewTopic
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

def parse_config(filename):
    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(filename)
    config = dict(config_parser['default'])
    config.update(config_parser['producer'])
    return config

def setup_admin(config):
    return AdminClient({'bootstrap.servers': config["bootstrap.servers"]})

def run_task(admin, topics = []):
    fs = admin.delete_topics(topics, operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))

def main():
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('-n', '--names', help='topic names', nargs='+')
    parser.add_argument('-a', '--all', help='delete all topics', action='store_true')
    args = parser.parse_args()

    config = parse_config(args.config_file)
    admin = setup_admin(config)

    if args.all:
        print('Deleting all topics')
        run_task(admin, list(admin.list_topics().topics.keys()))
    else:
        if args.name is None:
            parser.print_help()
            sys.exit(1)
        run_task(admin, args.names)

if __name__ == '__main__':
    main()
