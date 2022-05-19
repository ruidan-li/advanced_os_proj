from time import sleep
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING
import os
import arrow
import json
import uuid
import datetime

# conf = {'bootstrap.servers': "host1:9092,host2:9092",
#         'group.id': "foo",
#         'auto.offset.reset': 'smallest'}

class KafkaConsumer(Consumer):
    def __init__(self, config, topic, sample_interval=5000):
        super(KafkaConsumer, self).__init__(config)
        self._topic = topic
        self._running = False
        self.reset = False
        self.lag_metrics = ['send_time,timestamp,partition,latest_offset,current_position,throughput,latency']
        self.metric_file = f'../logs/kafka_run_{uuid.uuid4()}.out'

        self.sampling_time = datetime.timedelta(milliseconds=50)
        self.last_time = arrow.now()
        self.processed = 0   # for throughput calculation
        self.latencies = []       # for latency-i calculation
    
    def reset(self, reset=True):
        self.reset = True

    def start(self):
        self._running = True

    def stop(self):
        self._running = False

    def basic_consume(self, timeout, wait=5):
        wait_count = 0
        try:
            self.subscribe(self._topic)
            self.last_time = arrow.now()
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
                    # current_partition = self.assignment()
                    send_time = json.loads(msg.value().decode('utf-8'))['timestamp']
                    recv_time = arrow.now().timestamp()

                    # self.msg_process(msg, current_partition, send_time)
                    self.update_stats_per_msg(send_time, recv_time)
                    if (arrow.now() - self.last_time) >= self.sampling_time:
                        self.update_stats_per_sample(msg, send_time, recv_time)

        finally:
            # Close down consumer to commit final offsets.
            self.close()
            # Dump metrics to file
            print(f'Writing metric to {self.metric_file}')
            with open(self.metric_file, 'w') as fp:
                fp.write('\n'.join(self.lag_metrics))

    def msg_process(self, msg, curr_p, send_time):
        print("*** {pid} *** topic {topic} (partitions: {partition}): key = {key:12} value = {value:12} ({delay} sec used)".format(
                    pid=os.getpid(),
                    topic=msg.topic(),
                    partition=[p.partition for p in curr_p],
                    key=msg.key().decode('utf-8'),
                    value=send_time,
                    delay=self.calc_trip_time(send_time)))

    def reset_offset(consumer, partitions):
        # Set up a callback to handle the '--reset' flag.
        if consumer.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    def calc_lag(self, curr_p, send_time):
        current_offset = self.position(curr_p)
        for p in current_offset:
            if p.offset > 0:
                latest_offset = self.get_watermark_offsets(p, cached=True)
                if not latest_offset:
                    print(f'Unable to get the latest offset from partition {p.partition}')
                    self.lag_metrics.append(f'{send_time},{arrow.now().timestamp()},{p.partition},-1,{p.offset}')
                else:
                    # lag == -1 means it's fully caught up and waiting new messages
                    latest_offset = latest_offset[1] - 1
                    self.lag_metrics.append(f'{send_time},{arrow.now().timestamp()},{p.partition},{latest_offset},{p.offset}')
            else:
                print(f'Not actively reading from {p.partition}')
                self.lag_metrics.append(f'{send_time},{arrow.now().timestamp()},{p.partition},-1,-1')

    def update_stats_per_msg(self, send_time, recv_time):
        self.processed += 1
        self.latencies.append(recv_time-send_time)

    def update_stats_per_sample(self, msg, send_time, recv_time):
        time_interval = arrow.now() - self.last_time
        throughput = self.processed / time_interval.total_seconds()
        latencies = sum(self.latencies) / len(self.latencies)

        partitions = self.position(self.assignment())
        for p in partitions:
            if p.partition == msg.partition():
                lastest = self.get_watermark_offsets(p, cached=True)[1]
                log_msg = f'{send_time},{recv_time},{p.partition},{lastest},{p.offset},{throughput},{latencies}'
                self.lag_metrics.append(log_msg)
                print(f"*** {os.getpid()} ***", log_msg)
                break
        else:
            raise Exception("no partition match error")

        self.last_time = arrow.now()
        self.processed = 0
        self.latencies = []

    @staticmethod
    def calc_trip_time(send_time):
        return arrow.now().timestamp() - send_time