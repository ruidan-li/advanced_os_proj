from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING
import os
import arrow
import json

# conf = {'bootstrap.servers': "host1:9092,host2:9092",
#         'group.id': "foo",
#         'auto.offset.reset': 'smallest'}

class KafkaConsumer(Consumer):
    def __init__(self, config, topic):
        super(KafkaConsumer, self).__init__(config)
        self._topic = topic
        self._running = False
        self.reset = False
    
    def reset(self, reset=True):
        self.reset = True

    def start(self):
        self._running = True

    def stop(self):
        self._running = False

    def basic_consume(self, timeout, wait=10):
        wait_count = 0
        try:
            self.subscribe(self._topic)

            while self._running:
                msg = self.poll(timeout=timeout)
                if msg is None:
                    wait_count += 1
                    if wait_count > wait:
                        self.stop()
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    current_partition = self.assignment()
                    self.msg_process(msg, current_partition)
                    self.calc_lag(current_partition)
        finally:
            # Close down consumer to commit final offsets.
            self.close()

    def msg_process(self, msg, curr_p):
        msg_value = msg.value().decode('utf-8')
        print("*** {pid} *** topic {topic} (partitions: {partition}): key = {key:12} value = {value:12} ({delay} sec used)".format(
                    pid=os.getpid(),
                    topic=msg.topic(),
                    partition=[p.partition for p in curr_p],
                    key=msg.key().decode('utf-8'),
                    value=msg_value,
                    delay=self.calc_trip_time(msg_value)))

    def reset_offset(consumer, partitions):
        # Set up a callback to handle the '--reset' flag.
        if consumer.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)


    def calc_lag(self, curr_p):
        current_offset = self.position(curr_p)
        for p in current_offset:
            if p.offset > 0:
                latest_offset = self.get_watermark_offsets(p)
                if not latest_offset:
                    print('Unable to get the latest offset')
                else:
                    # lag == -1 means it's fully caught up and waiting new messages
                    latest_offset = latest_offset[1] - 1
                    print(f'LAG @ partition {p.partition}: {latest_offset} - {p.offset} = {latest_offset - p.offset}')
            else:
                print(f'Not actively reading from {p.partition}')

    @staticmethod
    def calc_trip_time(msg):
        return arrow.now().timestamp() - json.loads(msg)['timestamp']