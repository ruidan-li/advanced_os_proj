import datetime
import json
import os
import time
import uuid
from collections import defaultdict
from time import sleep

import arrow
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException
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

        self.sampling_ival = sample_interval  # msg-based sampling
        self.sampling_cntr = 0  # msg-based sampling
        self.time_diff = defaultdict(lambda: [])  # each element belongs to a partition
        self.indx_diff = defaultdict(lambda: [])  # each element belongs to a partition

        self.sampling_time = datetime.timedelta(milliseconds=500)  # time-based sampling
        self.sampling_time_cntr = 0
        self.last_time = arrow.now()  # time-based sampling
        self.processed = defaultdict(lambda: 0)  # each element belongs to a partition
        self.latencies = defaultdict(lambda: [])  # each element belongs to a partition

        self.pid = os.getpid()
        self.current_time = arrow.now().timestamp()

        self.cntr_log = []
        self.time_log = []
        self.metadata = {
            "pid": self.pid,
            "msg_based_sample": self.sampling_ival,
            "sampling_time_sec": self.sampling_time.seconds,
            "sleep": False,
            "sleep_delay_sec": 0,
            "sleep_duration_sec": 0,
            "sleep_start_ts": None,
            "sleep_end_ts": None,
        }

        self.cntr_log_file = f"../logs_cntr/kafka_run_{self.pid}.out"
        self.time_log_file = f"../logs_time/kafka_run_{self.pid}.out"
        self.consumer_metadata_out_file = f"../logs/kakfa_run_{self.pid}.out"

        # for tracking if new partitions kick in
        self.partition_hist = []

    def reset_counter(self):
        self.sampling_time_cntr = 0
        self.sampling_time_cntr = 0
        self.time_diff = defaultdict(lambda: [])  # each element belongs to a partition
        self.indx_diff = defaultdict(lambda: [])
        self.processed = defaultdict(lambda: 0)  # each element belongs to a partition
        self.latencies = defaultdict(lambda: [])

    def reset(self, reset=True):
        self.reset = True

    def start(self):
        self._running = True

    def stop(self):
        self._running = False

    def basic_consume(
        self, timeout, wait=30, put_to_sleep=False, sleep_delay_sec=0, sleep_duration=0
    ):
        if put_to_sleep:
            start = time.time()
        wait_count = 0
        total_count = 0
        self.metadata["sleep"] = put_to_sleep
        self.metadata["sleep_delay_sec"] = sleep_delay_sec
        self.metadata["sleep_duration_sec"] = sleep_duration
        try:
            self.subscribe(self._topic)
            self.last_time = arrow.now()
            while self._running:
                if put_to_sleep and (time.time() - start) >= sleep_delay_sec:
                    print(f"*** {os.getpid()} *** Sleep for {sleep_duration} sec")
                    self.metadata["sleep_start_ts"] = arrow.now().timestamp()
                    time.sleep(sleep_duration)
                    put_to_sleep = False  # only sleep once
                    self.metadata["sleep_end_ts"] = arrow.now().timestamp()
                    self.reset_counter()  # resetting all the counters
                    print(f"*** {os.getpid()} *** wake up at {arrow.utcnow()}")
                msg = self.poll(timeout=timeout)
                if msg is None:
                    wait_count += 1
                    if wait_count > wait:
                        print(f"*** {os.getpid()} *** stop at {arrow.utcnow()}")
                        self.stop()
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(
                            "%% %s [%d] reached end at offset %d\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                    elif msg.error().code() == KafkaError._MAX_POLL_EXCEEDED:
                        print(
                            "%% %s [%d] max poll exceeded triggered\n"
                            % (msg.topic(), msg.partition())
                        )
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # reset the wait count
                    wait_count = 0
                    total_count += 1
                    new_partition = self.handle_msg(msg, total_count)
                    if new_partition:
                        print(
                            f"New partition detected after processing {total_count} msg."
                        )
                    self.current_time = arrow.now().timestamp()
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
            print(
                f"Writing metric to {self.cntr_log_file}, {self.time_log_file}. total msg count {total_count}"
            )
            with open(self.cntr_log_file, "w") as fp:
                fp.write("\n".join(self.cntr_log))
            with open(self.time_log_file, "w") as fp:
                fp.write("\n".join(self.time_log))
            with open(self.consumer_metadata_out_file, "w") as fp:
                json.dump(self.metadata, fp)

    def handle_msg(self, msg, curr_ct):
        self.sampling_cntr += 1
        send_time = json.loads(msg.value().decode("utf-8"))["timestamp"]
        recv_time = arrow.now().timestamp()
        delta = round(recv_time - send_time, 3)

        # time_diff and indx_diff
        self.time_diff[msg.partition()].append(delta)  # time_diff
        partitions = self.position(self.assignment())
        for p in partitions:
            if p.partition == msg.partition():
                lastest = self.get_watermark_offsets(p, cached=True)[1]
                current = p.offset
                self.indx_diff[msg.partition()].append(lastest - current)  # indx_diff
                break
        else:
            raise Exception("no partition match error")

        # processed and latencies
        self.processed[msg.partition()] += 1  # processed
        self.latencies[msg.partition()].append(delta)  # latencies
        time.sleep(0.00003)
        # check if the msg belongs to a new partition
        if msg.partition() in self.partition_hist:
            return False
        elif curr_ct > 1000:  # only start tracking after the consumption is stablized
            self.partition_hist.append(msg.partition())
            return True
        else:
            return False

    def sampling_cntr_interrupt(self):
        # time_diff =  {"all": [avg, 50, 90, 99], "1": [avg, 50, 90, 99], "2": [avg, 50, 90, 99]}
        # idex_diff =  {"all": [avg, 50, 90, 99], "1": [avg, 50, 90, 99], "2": [avg, 50, 90, 99]}
        time_diff_line = {}
        time_diff_all = []
        for partition, time_diff in self.time_diff.items():
            time_diff = sorted(time_diff)
            time_diff_all.extend(time_diff)
            time_diff_line[partition] = self.calculate_4(time_diff)
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

        log_line = json.dumps(
            {
                "ts": self.current_time,
                "pid": self.pid,
                "time_diff": time_diff_line,
                "idex_diff": idex_diff_line,
            }
        )
        self.cntr_log.append(log_line)
        # print(f"*** {os.getpid()} *** ", log_line)

        self.time_diff = defaultdict(lambda: [])
        self.indx_diff = defaultdict(lambda: [])

    def sampling_time_interrupt(self):
        processed_line = {}
        processed_all = 0
        time_interval = (arrow.now() - self.last_time).total_seconds()
        for partition, processed in self.processed.items():
            processed_all += processed
            processed_line[partition] = round(processed / time_interval, 3)
        processed_line["all"] = round(processed_all / time_interval, 3)

        latencies_line = {}
        latencies_all = []
        for partition, latencies in self.latencies.items():
            latencies = sorted(latencies)
            latencies_all.extend(latencies)
            latencies_line[partition] = self.calculate_4(latencies)
        latencies_all = sorted(latencies_all)
        latencies_line["all"] = self.calculate_4(latencies_all)

        log_line = json.dumps(
            {
                "ts": self.current_time,
                "pid": self.pid,
                "processed": processed_line,
                "latencies": latencies_line,
            }
        )
        self.time_log.append(log_line)
        print(f"*** {os.getpid()} *** ", processed_line["all"], latencies_line["all"])

        self.sampling_time_cntr += 1
        self.processed = defaultdict(lambda: 0)
        self.latencies = defaultdict(lambda: [])

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
    def calculate_percent(lst, p):  # lst: sorted, # p = 0.5, 0.9, 0.99...
        try:
            return lst[int(p * float(len(lst)))]
        except IndexError:
            return 0

    def msg_process(self, msg, curr_p, send_time):
        print(
            "*** {pid} *** topic {topic} (partitions: {partition}): key = {key:12} value = {value:12} ({delay} sec used)".format(
                pid=os.getpid(),
                topic=msg.topic(),
                partition=[p.partition for p in curr_p],
                key=msg.key().decode("utf-8") if msg.key() else "",
                value=send_time,
                delay=self.calc_trip_time(send_time),
            )
        )

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
                    print(
                        f"Unable to get the latest offset from partition {p.partition}"
                    )
                    self.lag_metrics.append(
                        f"{send_time},{arrow.now().timestamp()},{p.partition},-1,{p.offset}"
                    )
                else:
                    # lag == -1 means it's fully caught up and waiting new messages
                    latest_offset = latest_offset[1] - 1
                    self.lag_metrics.append(
                        f"{send_time},{arrow.now().timestamp()},{p.partition},{latest_offset},{p.offset}"
                    )
            else:
                print(f"Not actively reading from {p.partition}")
                self.lag_metrics.append(
                    f"{send_time},{arrow.now().timestamp()},{p.partition},-1,-1"
                )

    def update_stats_per_msg(self, send_time, recv_time):
        self.processed += 1
        self.latencies.append(recv_time - send_time)

    def update_stats_per_sample(self, msg, send_time, recv_time):
        time_interval = arrow.now() - self.last_time
        throughput = self.processed / time_interval.total_seconds()
        latencies = sum(self.latencies) / len(self.latencies)

        partitions = self.position(self.assignment())
        for p in partitions:
            if p.partition == msg.partition():
                lastest = self.get_watermark_offsets(p, cached=True)[1]
                log_msg = f"{send_time},{recv_time},{p.partition},{lastest},{p.offset},{throughput},{latencies}"
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
