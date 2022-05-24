import os
import pickle
import shutil
import sys
from cProfile import label
from os import listdir
from os.path import isfile, join

import arrow
import matplotlib.dates as md
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec

rfs = [3]
pas = [2]
cos = [2]
pos = [2]
vrs = [11]


def fetch_experiment_data():
    experiments = []

    for vr in vrs:
        for rf in rfs:
            for pa in pas:
                for co in cos:
                    for po in pos:
                        param = f"rf{rf}-pa{pa}-co{co}-po{po}-vr{vr}"
                        # param_short = f"{rf};{pa};{co};{po};{vr}"
                        folder = f"topic-{param}"
                        fpath = join(os.getcwd(), f"res/{folder}/res_obj.pickle")
                        fh = open(fpath, "rb")
                        res_obj = pickle.load(fh)
                        experiments.append((param, res_obj))
    print("num of experiments loaded:", len(experiments))

    output_folder = "./fig/" + "_".join(
        [
            "-".join(map(str, rfs)),
            "-".join(map(str, pas)),
            "-".join(map(str, cos)),
            "-".join(map(str, pos)),
            "-".join(map(str, vrs)),
        ]
    )

    print(f"Creating output directory {output_folder} if does not exist")
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder, ignore_errors=True)

    os.makedirs(output_folder)

    return experiments, output_folder


def emit_x_and_y(data):
    timestamps = []
    values = []

    if len(data) == 0:
        return timestamps, values

    # data should be sorted
    # get first timestamp

    ts = data[0]["timestamp"]

    for tick in data:
        timestamps.append(tick["timestamp"] - ts)
        values.append(tick["value"])

    return timestamps, values


############################################################################
## Plots

# idx_based
#######   #######  #######  #######
# avg #   # p50 #  # p90 #  # p99 #
#######   #######  #######  #######

# time_based
#######   #######  #######  #######
# avg #   # p50 #  # p90 #  # p99 #
#######   #######  #######  #######

# latencies
#######   #######  #######  #######
# avg #   # p50 #  # p90 #  # p99 #
#######   #######  #######  #######

# throughput
##################  ##################
# sum throughput #  # avg throughput #
##################  ##################

############################################################################


ROWS = 4
COLS = 4


LABELS = {
    "idx_diff": {
        "unit": None,
        "metrics": {
            "avg": "Average Index Diff",
            "p50": "50th %tile Index Diff",
            "p90": "90th %tile Index Diff",
            "p99": "99th %tile Index Diff",
        },
    },
    "time_diff": {
        "unit": "sec",
        "metrics": {
            "avg": "Average Time Diff",
            "p50": "50th %tile Time Diff",
            "p90": "90th %tile Time Diff",
            "p99": "99th %tile Time Diff",
        },
    },
    "latencies": {
        "unit": "sec",
        "metrics": {
            "avg": "Average Latencies",
            "p50": "50th %tile Latencies",
            "p90": "90th %tile Latencies",
            "p99": "99th %tile Latencies",
        },
    },
    "throughput": {
        "unit": "recv/sec",
        "metrics": {
            "all": "Throughput",
        },
    },
}


def get_label(diff, metric):
    if diff in LABELS:
        if metric in LABELS[diff]["metrics"]:
            return LABELS[diff]["unit"], LABELS[diff]["metrics"][metric]

    return f"{metric} {diff}"


def plot_experiments(experiments, output_folder, metric="avg"):
    for name, exp in experiments:
        fig = plt.figure(constrained_layout=True)
        fig.suptitle(f"Experiment {name} Consumers", fontsize=18)
        fig.set_size_inches(13, 13)
        fig.tight_layout(h_pad=3.5, rect=[0, 0.03, 1, 0.97])

        axs = {}

        msg_based, time_based = exp

        counter = 0

        for consumer_idx, consumer in enumerate(msg_based["consumers"]):
            for diff_idx, diff in enumerate(
                msg_based["consumers"][consumer]
            ):  # idx_diff, time_diff
                for metric_idx, metric in enumerate(
                    msg_based["consumers"][consumer][diff]
                ):  # avg, p50, p90, p99
                    key = f"{diff_idx},{metric_idx}"
                    if key not in axs:
                        axs[key] = plt.subplot2grid(
                            (ROWS, COLS), (diff_idx, metric_idx), colspan=1
                        )
                    unit, label = get_label(diff, metric)
                    axs[key].plot(
                        *emit_x_and_y(msg_based["consumers"][consumer][diff][metric]),
                        label=f"Consumer #{consumer_idx}",
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)", ylabel=f"{label} (in {unit})"
                    )
                    axs[key].legend()

        counter += 2  # 2 rows are already occupied by msg_based
        for consumer_idx, consumer in enumerate(time_based["consumers"]):
            for diff_idx, diff in enumerate(
                time_based["consumers"][consumer]
            ):  # latencies, throughput
                if type(time_based["consumers"][consumer][diff]) is dict:
                    for metric_idx, metric in enumerate(
                        time_based["consumers"][consumer][diff]
                    ):  # avg, p50, p90, p99
                        key = f"{counter + diff_idx},{metric_idx}"
                        if key not in axs:
                            axs[key] = plt.subplot2grid(
                                (ROWS, COLS),
                                (counter + diff_idx, metric_idx),
                                colspan=1,
                            )
                        unit, label = get_label(diff, metric)
                        axs[key].plot(
                            *emit_x_and_y(
                                time_based["consumers"][consumer][diff][metric]
                            ),
                            label=f"Consumer #{consumer_idx}",
                        )
                        axs[key].set_title(f"{label}")
                        axs[key].set(
                            xlabel=f"duration (in seconds)",
                            ylabel=f"{label} (in {unit})",
                        )
                        axs[key].legend()
                else:
                    # throughput
                    key = f"{counter + diff_idx},0"
                    if key not in axs:
                        axs[key] = plt.subplot2grid(
                            (ROWS, COLS),
                            (counter + diff_idx, 0),
                            colspan=COLS,
                        )
                    unit, label = get_label(diff, "all")
                    axs[key].plot(
                        *emit_x_and_y(time_based["consumers"][consumer][diff]),
                        label=f"Consumer #{consumer_idx}",
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)", ylabel=f"{label} (in {unit})"
                    )
                    axs[key].legend()

        plt.savefig(f"{output_folder}/{name}-consumer.png", bbox_inches="tight")

        ##############################################
        ## PARTITION PLOTS
        ##############################################

        fig = plt.figure(constrained_layout=True)
        fig.suptitle(f"Experiment {name} Partitions", fontsize=18)
        fig.set_size_inches(13, 13)
        fig.tight_layout(h_pad=3.5, rect=[0, 0.03, 1, 0.97])

        axs = {}

        counter = 0

        for partition in msg_based["partitions"]:
            for diff_idx, diff in enumerate(
                msg_based["partitions"][partition]
            ):  # idx_diff, time_diff
                for metric_idx, metric in enumerate(
                    msg_based["partitions"][partition][diff]
                ):  # avg, p50, p90, p99
                    key = f"{diff_idx},{metric_idx}"
                    if key not in axs:
                        axs[key] = plt.subplot2grid(
                            (ROWS, COLS), (diff_idx, metric_idx), colspan=1
                        )
                    unit, label = get_label(diff, metric)
                    axs[key].plot(
                        *emit_x_and_y(msg_based["partitions"][partition][diff][metric]),
                        label=f"Partition #{partition}",
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)", ylabel=f"{label} (in {unit})"
                    )
                    axs[key].legend()

        counter += 2  # 2 rows are already occupied by msg_based
        for partition in time_based["partitions"]:
            for diff_idx, diff in enumerate(
                time_based["partitions"][partition]
            ):  # latencies, throughput
                if type(time_based["partitions"][partition][diff]) is dict:
                    for metric_idx, metric in enumerate(
                        time_based["partitions"][partition][diff]
                    ):  # avg, p50, p90, p99
                        key = f"{counter + diff_idx},{metric_idx}"
                        if key not in axs:
                            axs[key] = plt.subplot2grid(
                                (ROWS, COLS),
                                (counter + diff_idx, metric_idx),
                                colspan=1,
                            )
                        unit, label = get_label(diff, metric)
                        axs[key].plot(
                            *emit_x_and_y(
                                time_based["partitions"][partition][diff][metric]
                            ),
                            label=f"partition #{partition}",
                        )
                        axs[key].set_title(f"{label}")
                        axs[key].set(
                            xlabel=f"duration (in seconds)",
                            ylabel=f"{label} (in {unit})",
                        )
                        axs[key].legend()
                else:
                    # throughput
                    key = f"{counter + diff_idx},0"
                    if key not in axs:
                        axs[key] = plt.subplot2grid(
                            (ROWS, COLS),
                            (counter + diff_idx, 0),
                            colspan=COLS,
                        )
                    unit, label = get_label(diff, "all")
                    axs[key].plot(
                        *emit_x_and_y(time_based["partitions"][partition][diff]),
                        label=f"partition #{partition}",
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})",
                    )
                    axs[key].legend()

        plt.savefig(f"{output_folder}/{name}-partition.png", bbox_inches="tight")


experiments, output_folder = fetch_experiment_data()

plot_experiments(experiments, output_folder)
