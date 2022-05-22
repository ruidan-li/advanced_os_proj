from time import sleep
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING
import os
import arrow
import json
import uuid
import datetime
from collections import defaultdict
from numpy import sort

# conf = {'bootstrap.servers': "host1:9092,host2:9092",
#         'group.id': "foo",
#         'auto.offset.reset': 'smallest'}

class KafkaConsumer(Consumer):
    def __init__(self, config, topic, sample_interval=5000):
        super(KafkaConsumer, self).__init__(config)
        self._topic = topic
        self._running = False
        self.reset = False

        self.sampling_ival = 5000  # msg-based sampling
        self.sampling_cntr = 0    # msg-based sampling
        self.time_diff = defaultdict(lambda:[])       # each element belongs to a partition
        self.indx_diff = defaultdict(lambda:[])       # each element belongs to a partition

        self.sampling_time = datetime.timedelta(milliseconds=1000) # time-based sampling
        self.last_time = arrow.now()                             # time-based sampling
        self.processed = defaultdict(lambda:0)     # each element belongs to a partition
        self.latencies = defaultdict(lambda:[])    # each element belongs to a partition

        self.cntr_log = []
        self.time_log = []
        self.cntr_log_file = f'../logs_cntr/kafka_run_{uuid.uuid4()}.out'
        self.time_log_file = f'../logs_time/kafka_run_{uuid.uuid4()}.out'
    
    def reset(self, reset=True):
        self.reset = True

    def start(self):
        self._running = True

    def stop(self):
        self._running = False

    def basic_consume(self, timeout, wait=5):
        wait_count = 0
        total_count = 0
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
                    # reset the wait count
                    wait_count = 0
                    total_count += 1
                    self.handle_msg(msg)
                    if self.sampling_cntr == self.sampling_ival:
                        self.sampling_cntr_interrupt()
                        self.sampling_cntr = 0
                    if (arrow.now() - self.last_time) >= self.sampling_time:
                        self.sampling_time_interrupt()
                        self.last_time = arrow.now()

        finally:
            # Close down consumer to commit final offsets.
            self.close()
            # Dump metrics to file
            print(f'Writing metric to {self.cntr_log_file}, {self.time_log_file}. total msg count {total_count}')
            with open(self.cntr_log_file, 'w') as fp:
                fp.write('\n'.join(self.cntr_log))
            with open(self.time_log_file, 'w') as fp:
                fp.write('\n'.join(self.time_log))

    def handle_msg(self, msg):
        self.sampling_cntr += 1
        send_time = json.loads(msg.value().decode('utf-8'))['timestamp']
        recv_time = arrow.now().timestamp()
        delta = round(recv_time - send_time, 3)

        # time_diff and indx_diff
        self.time_diff[msg.partition()].append(delta) # time_diff
        partitions = self.position(self.assignment())
        for p in partitions:
            if p.partition == msg.partition():
                lastest = self.get_watermark_offsets(p, cached=True)[1]
                current = p.offset
                self.indx_diff[msg.partition()].append(lastest - current) # indx_diff
                break
        else:
            raise Exception("no partition match error")

        # processed and latencies
        self.processed[msg.partition()] += 1           # processed
        self.latencies[msg.partition()].append(delta)  # latencies
        sleep(0.00003)

    def sampling_cntr_interrupt(self):
        # time_diff =  {"all": [avg, 50, 90, 99], "1": [avg, 50, 90, 99], "2": [avg, 50, 90, 99]}
        # idex_diff =  {"all": [avg, 50, 90, 99], "1": [avg, 50, 90, 99], "2": [avg, 50, 90, 99]}

        time_diff_line = {}
        time_diff_all = []
        for partition, time_diff in self.time_diff.items():
            time_diff = sorted(time_diff)
            time_diff_all.extend(time_diff)
            time_diff_line[partition] =  self.calculate_4(time_diff)
        time_diff_all = sorted(time_diff_all)
        time_diff_line["all"] = self.calculate_4(time_diff_all)

        idex_diff_line = {}
        idex_diff_all = []
        for partition, indx_diff in self.indx_diff.items():
            indx_diff = sorted(indx_diff)
            idex_diff_all.extend(indx_diff)
            idex_diff_line[partition] = self.calculate_4(indx_diff, 1)
        idex_diff_all = sorted(idex_diff_all)
        idex_diff_line["all"] = self.calculate_4(idex_diff_all, 1)

        log_line = json.dumps({"time_diff": time_diff_line, "idex_diff": idex_diff_line})
        self.cntr_log.append(log_line)
        # print(f"*** {os.getpid()} *** ", log_line)

        self.time_diff = defaultdict(lambda:[])   
        self.indx_diff = defaultdict(lambda:[])


    def sampling_time_interrupt(self):
        processed_line = {}
        processed_all = 0
        time_interval = (arrow.now() - self.last_time).total_seconds()
        for partition, processed in self.processed.items():
            processed_all += processed
            processed_line[partition] = round(processed / time_interval, 3)
        processed_line["all"] =  round(processed_all / time_interval, 3)

        latencies_line = {}
        latencies_all = []
        for partition, latencies in self.latencies.items():
            latencies = sorted(latencies)
            latencies_all.extend(latencies)
            latencies_line[partition] = self.calculate_4(latencies)
        latencies_all = sorted(latencies_all)
        latencies_line["all"] = self.calculate_4(latencies_all)

        log_line = json.dumps({"processed": processed_line, "latencies": latencies_line})
        self.time_log.append(log_line)
        print(f"*** {os.getpid()} *** ", processed_line["all"], latencies_line["all"])

        self.processed = defaultdict(lambda:0)
        self.latencies = defaultdict(lambda:[])


    def calculate_4(self, lst, rd=3):
        res = [self.calculate_average(lst, rd)]
        res.append(self.calculate_percent(lst, 0.5))
        res.append(self.calculate_percent(lst, 0.9))
        res.append(self.calculate_percent(lst, 0.99))
        return res

    @staticmethod
    def calculate_average(lst, rd=3):
        try:
            return round(sum(lst) / len(lst), 3)
        except ZeroDivisionError:
            return 0


    @staticmethod
    def calculate_percent(lst, p): # lst: sorted, # p = 0.5, 0.9, 0.99...
        try:
            return lst[int(p*float(len(lst)))]
        except IndexError:
            return 0 

    def msg_process(self, msg, curr_p, send_time):
        print("*** {pid} *** topic {topic} (partitions: {partition}): key = {key:12} value = {value:12} ({delay} sec used)".format(
                    pid=os.getpid(),
                    topic=msg.topic(),
                    partition=[p.partition for p in curr_p],
                    key=msg.key().decode('utf-8') if msg.key() else '',
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