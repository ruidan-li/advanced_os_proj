import os
import pickle
import shutil
import sys
from cProfile import label
from dataclasses import dataclass
from os import listdir
from os.path import isfile, join

import arrow
import matplotlib.dates as md
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection
from matplotlib.colors import ListedColormap

# from matplotlib.gridspec import GridSpec
from matplotlib.lines import Line2D

rfs = [3]
pas = [4]
cos = [4]
pos = [4]
vrs = [10]


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


############################################################################
## Plots
############################################################################

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

COLORS = [
    # "tab:green",
    "maroon",
    "tab:blue",
    "tab:orange",
    "tab:purple",
    "tab:brown",
    "tab:pink",
    "tab:gray",
    "tab:olive",
    "tab:cyan",
    "deeppink",
    "gold",
]

LINESTYLES = [
    "dotted",
    "solid",
    "dashed",
    "dashdot",
    (0, (1, 10)),
    # (0, (1, 1)),
    (0, (1, 1)),
    (0, (5, 10)),
    # (0, (5, 5)),
    (0, (5, 1)),
    (0, (3, 10, 1, 10)),
    (0, (3, 5, 1, 5)),
    (0, (3, 1, 1, 1)),
    (0, (3, 5, 1, 5, 1, 5)),
    (0, (3, 10, 1, 10, 1, 10)),
    (0, (3, 1, 1, 1, 1, 1))
    # "loosely dotted",
    # "dotted",
    # "densely dotted",
    # "loosely dashed",
    # "dashed",
    # "densely dashed",
    # "loosely dashdotted",
    # "dashdotted",
    # "densely dashdotted",
    # "dashdotdotted",
    # "loosely dashdotdotted",
    # "densely dashdotdotted",
]

CMAP = ListedColormap(COLORS)


def get_label(diff, metric):
    if diff in LABELS:
        if metric in LABELS[diff]["metrics"]:
            return LABELS[diff]["unit"], LABELS[diff]["metrics"][metric]

    return f"{metric} {diff}"


def normalize_to_bound(value, lower=0.0, upper=1.0):
    return lower + (upper - lower) * value


# https://stackoverflow.com/a/25628397
# CC BY-SA 3.0
def get_cmap(n, name="hsv"):
    """Returns a function that maps each index in 0, 1, ..., n-1 to a distinct
    RGB color; the keyword argument name must be a standard mpl colormap name."""
    return plt.cm.get_cmap(name, n)


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


def emit_x_and_y_partitions(partition, data, colors, linestyle="solid"):
    results = []  # [[x_1, y_1, c_1], [x_2, y_2,c_2], ... ]

    if len(data) == 0:
        return results

    # data should be sorted
    # get first timestamp

    ts = data[0]["timestamp"]
    # last_pid = data[0]["pid"]

    # cmap = get_cmap(cnt_consumers)

    timestamps = []
    values = []
    # colors = {}
    segments = []
    line_colors = []
    line_styles = []

    for idx, (tick, tick_next) in enumerate(zip(data, data[1:])):
        # if last_pid != tick["pid"]:
        # flush the data
        # if last_pid not in colors:
        #     colors[last_pid] = COLORS[last_pid % len(colors)]
        # for timestamp1, timestamp2, value1, value2 in zip(
        #     timestamps, timestamps[1:], values, values[1:]
        # ):
        segments.append(
            [
                (tick["timestamp"] - ts, tick["value"]),
                (tick_next["timestamp"] - ts, tick_next["value"]),
            ]
        )
        line_colors.append(colors[str(tick["pid"])])
        # line_styles.append(LINESTYLES[int(tick["pid"]) % len(LINESTYLES)])
        # points = np.array([timestamps, values]).T.reshape(-1, 1, 2)
        # print(points)
        # segment = np.concatenate([points[:-1], points[1:]], axis=1)
        # segments.append(segment)
        # line_colors.append(colors[str(last_pid)])
        # segments.append()
        # results.append((timestamps, values, colors[last_pid]))
        # timestamps = []
        # values = []
        # line_colors = []

        # timestamps.append(tick["timestamp"] - ts)
        # values.append(tick["value"])

    return LineCollection(
        segments,
        colors=line_colors,
        linestyles=linestyle,
        linewidths=2,
    )


def make_legends(partition_consumers, linestyles, colors, **kwargs):
    lines = []
    names = []
    for partition in partition_consumers:  # { partition_id: [consumers] }
        for consumer in partition_consumers[partition]:
            lines.append(
                Line2D(
                    [0, 1],
                    [0, 1],
                    color=colors[str(consumer)],
                    linestyle=linestyles[partition],
                    **kwargs,
                )
            ),
            names.append(f"C#{consumer} P#{partition}")

    return lines, names


def plot_experiments(experiments, output_folder, metric="avg"):
    for name, exp in experiments:
        fig = plt.figure(constrained_layout=True)
        fig.suptitle(f"Experiment {name} Consumers", fontsize=18)
        fig.set_size_inches(13, 13)
        fig.tight_layout(h_pad=3.5, rect=[0, 0.03, 1, 0.97])

        axs = {}

        msg_based, time_based, partition_consumers = exp

        counter = 0

        colors = {}

        for consumer_idx, consumer in enumerate(msg_based["consumers"]):
            if consumer not in colors:
                colors[consumer] = COLORS[consumer_idx % len(COLORS)]

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
                        color=colors[consumer],
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)", ylabel=f"{label} (in {unit})"
                    )
                    axs[key].legend()

        counter += 2  # 2 rows are already occupied by msg_based
        for consumer_idx, consumer in enumerate(time_based["consumers"]):
            if consumer not in colors:
                colors[consumer] = COLORS[consumer_idx % len(COLORS)]

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
                            color=colors[consumer],
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
                        color=colors[consumer],
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})",
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

        linestyles = {}

        counter = 0

        for partition_idx, partition in enumerate(msg_based["partitions"]):
            if partition not in linestyles:
                linestyles[partition] = LINESTYLES[partition_idx % len(LINESTYLES)]
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
                    lc = emit_x_and_y_partitions(
                        partition,
                        msg_based["partitions"][partition][diff][metric],
                        colors,
                        linestyles[partition],
                    )
                    axs[key].add_collection(lc)
                    axs[key].autoscale()
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)", ylabel=f"{label} (in {unit})"
                    )

        counter += 2  # 2 rows are already occupied by msg_based
        for partition_idx, partition in enumerate(time_based["partitions"]):
            if partition not in linestyles:
                linestyles[partition] = LINESTYLES[partition_idx % len(LINESTYLES)]
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
                        lc = emit_x_and_y_partitions(
                            partition,
                            time_based["partitions"][partition][diff][metric],
                            colors,
                            linestyles[partition],
                        )
                        axs[key].add_collection(lc)
                        axs[key].autoscale()
                        axs[key].set_title(f"{label}")
                        axs[key].set(
                            xlabel=f"duration (in seconds)",
                            ylabel=f"{label} (in {unit})",
                        )
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
                    lc = emit_x_and_y_partitions(
                        partition,
                        time_based["partitions"][partition][diff],
                        colors,
                        linestyles[partition],
                    )

                    axs[key].add_collection(lc)
                    axs[key].autoscale()
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})",
                    )

        lines, names = make_legends(
            partition_consumers, colors=colors, linestyles=linestyles
        )

        for key in axs:
            axs[key].legend(lines, names, ncol=3)

        plt.savefig(f"{output_folder}/{name}-partition.png", bbox_inches="tight")


experiments, output_folder = fetch_experiment_data()

plot_experiments(experiments, output_folder)
