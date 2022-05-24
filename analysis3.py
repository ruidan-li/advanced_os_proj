import json
import os
import sys
from audioop import mul
from os import listdir
from os.path import isfile, join
from pprint import pprint

from numpy import result_type

# output will be like this
#
# result = {
#     "consumers": {
#         "pid_1": {
#             "avg": [
#                 {"timestamp": "...", "value": "..."},
#                 ...
#             ],
#             "p90": [
#                 {"timestamp": "...", "value": "..."},
#                 ...
#             ],
#         }
#     },
#     "partitions": {
#         "partition_1": {
#             "avg": [
#                 {"timestamp": "...", "value": "...", "pid": ""},
#                 ...
#             ],
#             "p90": [
#                 {"timestamp": "...", "value": "...", "pid": ""},
#                 ...
#             ],
#         }
#     }
# }


def process_one_file(fname):
    with open(fname) as fh:
        lst = []
        # create a list of dictionaries
        for line in fh.readlines():
            lst.append(json.loads(line))
        return lst


def extract_avg(extracted_list, list_idx):
    result_list = []
    for i in range(len(extracted_list[0])):  # tick
        tick_sum = 0
        for c in range(len(extracted_list)):  # client
            tick_sum += extracted_list[c][i][list_idx]
        result_list.append(round(tick_sum / len(extracted_list), 3))
    return result_list


def get_4_stats(line):  # avg, p50, p90, p99
    avg = line[0]
    p50 = line[1]
    p90 = line[2]
    p99 = line[3]
    return avg, p50, p90, p99


def process_raw_data_cntr(raw_data):
    results = {
        "consumers": {},
        "partitions": {},
    }

    for pid in raw_data:
        results["consumers"][pid] = {
            "idx_diff": {
                "avg": [],
                "p50": [],
                "p90": [],
                "p99": [],
            },
            "time_diff": {
                "avg": [],
                "p50": [],
                "p90": [],
                "p99": [],
            },
        }
        for line in raw_data[pid]:  # line should contain ts, pid, time_diff, idex_diff
            # if pid != str(line["pid"]):  # sanity check
            #     continue
            ts = line["ts"]

            # ----------------------------
            # single consumer
            avg, p50, p90, p99 = get_4_stats(line["time_diff"]["all"])
            results["consumers"][pid]["time_diff"]["avg"].append(
                {"timestamp": ts, "value": avg}
            )
            results["consumers"][pid]["time_diff"]["p50"].append(
                {"timestamp": ts, "value": p50}
            )
            results["consumers"][pid]["time_diff"]["p90"].append(
                {"timestamp": ts, "value": p90}
            )
            results["consumers"][pid]["time_diff"]["p99"].append(
                {"timestamp": ts, "value": p99}
            )

            avg, p50, p90, p99 = get_4_stats(line["idex_diff"]["all"])
            results["consumers"][pid]["idx_diff"]["avg"].append(
                {"timestamp": ts, "value": avg}
            )
            results["consumers"][pid]["idx_diff"]["p50"].append(
                {"timestamp": ts, "value": p50}
            )
            results["consumers"][pid]["idx_diff"]["p90"].append(
                {"timestamp": ts, "value": p90}
            )
            results["consumers"][pid]["idx_diff"]["p99"].append(
                {"timestamp": ts, "value": p99}
            )

            # this is bad practice, but we need to do this fast
            del line["time_diff"]["all"]
            del line["idex_diff"]["all"]
            # end of single consumer
            # ----------------------------

            # ----------------------------
            # partitions
            for partition in line["time_diff"]:
                if (
                    partition not in results["partitions"]
                ):  # check if partition exists inside result
                    results["partitions"][partition] = {
                        "idx_diff": {
                            "avg": [],
                            "p50": [],
                            "p90": [],
                            "p99": [],
                        },
                        "time_diff": {
                            "avg": [],
                            "p50": [],
                            "p90": [],
                            "p99": [],
                        },
                    }
                avg, p50, p90, p99 = get_4_stats(line["time_diff"][partition])
                # need to save pid, because different consumer might consume this partition
                results["partitions"][partition]["time_diff"]["avg"].append(
                    {"timestamp": ts, "value": avg, "pid": line["pid"]}
                )
                results["partitions"][partition]["time_diff"]["p50"].append(
                    {"timestamp": ts, "value": p50, "pid": line["pid"]}
                )
                results["partitions"][partition]["time_diff"]["p90"].append(
                    {"timestamp": ts, "value": p90, "pid": line["pid"]}
                )
                results["partitions"][partition]["time_diff"]["p99"].append(
                    {"timestamp": ts, "value": p99, "pid": line["pid"]}
                )

            for partition in line["idex_diff"]:
                if (
                    partition not in results["partitions"]
                ):  # check if partition exists inside result
                    results["partitions"][partition] = {
                        "idx_diff": {
                            "avg": [],
                            "p50": [],
                            "p90": [],
                            "p99": [],
                        },
                        "time_diff": {
                            "avg": [],
                            "p50": [],
                            "p90": [],
                            "p99": [],
                        },
                    }
                avg, p50, p90, p99 = get_4_stats(line["idex_diff"][partition])
                # need to save pid, because different consumer might consume this partition
                results["partitions"][partition]["idx_diff"]["avg"].append(
                    {"timestamp": ts, "value": avg, "pid": line["pid"]}
                )
                results["partitions"][partition]["idx_diff"]["p50"].append(
                    {"timestamp": ts, "value": p50, "pid": line["pid"]}
                )
                results["partitions"][partition]["idx_diff"]["p90"].append(
                    {"timestamp": ts, "value": p90, "pid": line["pid"]}
                )
                results["partitions"][partition]["idx_diff"]["p99"].append(
                    {"timestamp": ts, "value": p99, "pid": line["pid"]}
                )

            # end of partitions
            # ----------------------------

        # sort by timestamp, make sure that everything is sorted asc
        for diff in results["consumers"][pid]:
            for key in results["consumers"][pid][diff]:
                results["consumers"][pid][diff][key] = sorted(
                    results["consumers"][pid][diff][key], key=lambda v: v["timestamp"]
                )

    for partition in results["partitions"]:
        for diff in results["partitions"][partition]:
            for key in results["partitions"][partition][diff]:
                results["partitions"][partition][diff][key] = sorted(
                    results["partitions"][partition][diff][key],
                    key=lambda v: v["timestamp"],
                )

    return results


# def get_avg_tput(lsts):
#     extracted_list = extract_dict(lsts, "processed")
#     result_list = []
#     for i in range(len(extracted_list[0])):  # tick
#         tick_sum = 0
#         for c in range(len(extracted_list)):  # client
#             tick_sum += extracted_list[c][i]
#         result_list.append(round(tick_sum / len(extracted_list), 3))
#     return result_list


# def get_sum_tput(lsts):
#     extracted_list = extract_dict(lsts, "processed")
#     result_list = []
#     for i in range(len(extracted_list[0])):  # tick
#         tick_sum = 0
#         for c in range(len(extracted_list)):  # client
#             tick_sum += extracted_list[c][i]
#         result_list.append(round(tick_sum, 3))
#     return result_list


def process_raw_data_time(raw_data):
    results = {
        "consumers": {},
        "partitions": {},
    }

    for pid in raw_data:
        results["consumers"][pid] = {
            "latencies": {
                "avg": [],
                "p50": [],
                "p90": [],
                "p99": [],
            },
            "throughput": [],
        }
        for line in raw_data[pid]:  # line should contain ts, pid, time_diff, idex_diff
            # if pid != str(line["pid"]):  # sanity check
            #     continue
            ts = line["ts"]

            # ----------------------------
            # single consumer
            avg, p50, p90, p99 = get_4_stats(line["latencies"]["all"])
            results["consumers"][pid]["latencies"]["avg"].append(
                {"timestamp": ts, "value": avg}
            )
            results["consumers"][pid]["latencies"]["p50"].append(
                {"timestamp": ts, "value": p50}
            )
            results["consumers"][pid]["latencies"]["p90"].append(
                {"timestamp": ts, "value": p90}
            )
            results["consumers"][pid]["latencies"]["p99"].append(
                {"timestamp": ts, "value": p99}
            )

            throughput = line["processed"]["all"]
            results["consumers"][pid]["throughput"].append(
                {"timestamp": ts, "value": throughput}
            )
            # this is bad practice, but we need to do this fast
            del line["latencies"]["all"]
            del line["processed"]["all"]
            # end of single consumer
            # ----------------------------

            # ----------------------------
            # partitions
            for partition in line["latencies"]:
                if (
                    partition not in results["partitions"]
                ):  # check if partition exists inside result
                    results["partitions"][partition] = {
                        "latencies": {
                            "avg": [],
                            "p50": [],
                            "p90": [],
                            "p99": [],
                        },
                        "throughput": [],
                    }
                avg, p50, p90, p99 = get_4_stats(line["latencies"][partition])
                # need to save pid, because different consumer might consume this partition
                results["partitions"][partition]["latencies"]["avg"].append(
                    {"timestamp": ts, "value": avg, "pid": line["pid"]}
                )
                results["partitions"][partition]["latencies"]["p50"].append(
                    {"timestamp": ts, "value": p50, "pid": line["pid"]}
                )
                results["partitions"][partition]["latencies"]["p90"].append(
                    {"timestamp": ts, "value": p90, "pid": line["pid"]}
                )
                results["partitions"][partition]["latencies"]["p99"].append(
                    {"timestamp": ts, "value": p99, "pid": line["pid"]}
                )

            for partition in line["processed"]:
                if (
                    partition not in results["partitions"]
                ):  # check if partition exists inside result
                    results["partitions"][partition] = {
                        "latencies": {
                            "avg": [],
                            "p50": [],
                            "p90": [],
                            "p99": [],
                        },
                        "throughput": [],
                    }
                throughput = line["processed"][partition]
                # need to save pid, because different consumer might consume this partition
                results["partitions"][partition]["throughput"].append(
                    {"timestamp": ts, "value": throughput, "pid": line["pid"]}
                )
            # end of partitions
            # ----------------------------

        # sort consumers by timestamp, make sure that everything is sorted asc
        for diff in results["consumers"][pid]:
            if type(results["consumers"][pid][diff]) is dict:
                for key in results["consumers"][pid][diff]:
                    results["consumers"][pid][diff][key] = sorted(
                        results["consumers"][pid][diff][key],
                        key=lambda v: v["timestamp"],
                    )
            elif type(results["consumers"][pid][diff]) is list:
                results["consumers"][pid][diff] = sorted(
                    results["consumers"][pid][diff], key=lambda v: v["timestamp"]
                )

    # results["consumers"] =
    # for pid in results["consumers"]:

    # sort partitions by timestamp, make sure that everything is sorted asc
    for partition in results["partitions"]:
        for diff in results["partitions"][partition]:
            if type(results["partitions"][partition][diff]) is dict:
                for key in results["partitions"][partition][diff]:
                    results["partitions"][partition][diff][key] = sorted(
                        results["partitions"][partition][diff][key],
                        key=lambda v: v["timestamp"],
                    )
            elif type(results["partitions"][partition][diff]) is list:
                results["partitions"][partition][diff] = sorted(
                    results["partitions"][partition][diff], key=lambda v: v["timestamp"]
                )

    return results


def get_pid(filename):
    names = filename.split("_")
    return names[-1].replace(".out", "")


def extract_from_logs_cntr():
    raw_data = {}
    for (dirname, dirs, files) in os.walk(join(os.getcwd(), "logs_cntr")):
        for filename in files:
            pid = get_pid(filename)
            lst = process_one_file(join(dirname, filename))
            raw_data[pid] = lst
    result = process_raw_data_cntr(raw_data)
    return result


def extract_from_logs_time():
    raw_data = {}
    for (dirname, dirs, files) in os.walk(join(os.getcwd(), "logs_time")):
        for filename in files:
            pid = get_pid(filename)
            lst = process_one_file(join(dirname, filename))
            raw_data[pid] = lst
    result = process_raw_data_time(raw_data)
    return result

def extract_who_consume_the_partition(times):
    result = {}
    for partition in times["partitions"]:
        ts = times["partitions"][partition]["latencies"]["avg"][0]["timestamp"]
        consumers = {}
        for line in times["partitions"][partition]["latencies"]["avg"]:
            if line["pid"] not in consumers:
                consumers[line["pid"]] = 0
            consumers[line["pid"]] += 1
        result[partition] = list(consumers.keys())
        # print(f"partition: {partition}, consumers: {list(consumers.keys())}")
    return result

print("Running the analysis")
msg_based = extract_from_logs_cntr()
time_based = extract_from_logs_time()
who = extract_who_consume_the_partition(time_based)

import pickle

print("Saving the analysis result")
filehandler = open(f"{sys.argv[1]}/res_obj.pickle", "wb")
pickle.dump((msg_based, time_based, who), filehandler)
