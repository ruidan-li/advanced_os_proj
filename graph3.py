import os
import pickle
import shutil
from os.path import join

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection
from matplotlib.colors import ListedColormap
from matplotlib.lines import Line2D
from matplotlib.markers import MarkerStyle

rfs = [3]
pas = [8]
cos = [4]
pos = [8]
vrs = [50]


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
    # "-",
    # "--",
    # "-.",
    # "dotted",
    # "solid",
    # "dashed",
    # "dashdot",
    (0, (1, 10)),
    (0, (1, 1)),
    (0, (5, 10)),
    (0, (3, 10, 1, 10)),
    (0, (3, 5, 1, 5)),
    (0, (3, 1, 1, 1)),
    (0, (3, 5, 1, 5, 1, 5)),
    (0, (3, 10, 1, 10, 1, 10)),
    (0, (3, 1, 1, 1, 1, 1)),
]

MARKERS = MarkerStyle.markers

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


def emit_x_and_y(data, consumer_metadata):
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


def emit_x_and_y_partitions(
    data, partition, consumer_metadata, consumer_plot_metadata, linestyle
):
    # data should be sorted
    # get first timestamp

    ts = data[0]["timestamp"]

    segments = []
    line_colors = []

    for idx, (tick, tick_next) in enumerate(zip(data, data[1:])):
        metadata = consumer_metadata[str(tick["pid"])]
        if metadata["sleep"]:
            if (
                tick["timestamp"] >= metadata["sleep_start_ts"]
                and tick_next["timestamp"] <= metadata["sleep_end_ts"]
            ):
                continue

        segments.append(
            [
                (tick["timestamp"] - ts, tick["value"]),
                (tick_next["timestamp"] - ts, tick_next["value"]),
            ]
        )
        line_colors.append(consumer_plot_metadata[str(tick["pid"])]["color"])

    return LineCollection(
        segments,
        colors=line_colors,
        # linestyle=linestyle,
        linewidths=2,
    )


def make_legends(consumer_partitions, linestyles, consumer_plot_metadata, **kwargs):
    lines = []
    names = []
    for consumer in consumer_partitions:  # { consumer_id: [partitions] }
        consumer_metadata = consumer_plot_metadata[str(consumer)]
        # for partition in consumer_partitions[consumer]:
        lines.append(
            Line2D(
                [0, 1],
                [0, 1],
                color=consumer_metadata["color"],
                # linestyle=linestyles[partition],
                **kwargs,
            )
        ),
        names.append(f"Consumer #{consumer_metadata['idx']}")

    return lines, names


def plot_experiments(experiments, output_folder, metric="avg"):
    for name, exp in experiments:

        ##############################################
        ## CONSUMER PLOTS
        ##############################################

        print("Building consumer plots")
        fig = plt.figure(constrained_layout=True)
        fig.suptitle(f"Experiment {name} Consumers", fontsize=18)
        fig.set_size_inches(13, 13)
        fig.tight_layout(h_pad=3.5, rect=[0, 0.03, 1, 0.97])

        axs = {}

        (
            consumer_metadata,
            msg_based,
            time_based,
            partition_consumers,
            consumer_partitions,
        ) = exp

        counter = 0

        consumer_plot_metadata = {}

        for consumer_idx, consumer in enumerate(msg_based["consumers"]):
            if str(consumer) not in consumer_plot_metadata:
                consumer_plot_metadata[str(consumer)] = {
                    "idx": consumer_idx,
                    "color": COLORS[consumer_idx % len(COLORS)],
                }

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
                        *emit_x_and_y(
                            msg_based["consumers"][consumer][diff][metric],
                            consumer_metadata=consumer_metadata,
                        ),
                        label=f"Consumer #{consumer_idx}",
                        color=consumer_plot_metadata[consumer]["color"],
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})"
                        if unit is not None
                        else f"{label}",
                    )
                    axs[key].legend()

        counter += 2  # 2 rows are already occupied by msg_based
        for consumer_idx, consumer in enumerate(time_based["consumers"]):
            if str(consumer) not in consumer_plot_metadata:
                consumer_plot_metadata[str(consumer)] = {
                    "idx": consumer_idx,
                    "color": COLORS[consumer_idx % len(COLORS)],
                }

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
                                time_based["consumers"][consumer][diff][metric],
                                consumer_metadata=consumer_metadata,
                            ),
                            label=f"Consumer #{consumer_idx}",
                            color=consumer_plot_metadata[consumer]["color"],
                        )
                        axs[key].set_title(f"{label}")
                        axs[key].set(
                            xlabel=f"duration (in seconds)",
                            ylabel=f"{label} (in {unit})"
                            if unit is not None
                            else f"{label}",
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
                        *emit_x_and_y(
                            time_based["consumers"][consumer][diff],
                            consumer_metadata=consumer_metadata,
                        ),
                        label=f"Consumer #{consumer_idx}",
                        color=consumer_plot_metadata[consumer]["color"],
                    )
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})"
                        if unit is not None
                        else f"{label}",
                    )
                    axs[key].legend()

        print("Saving consumer plots")
        plt.savefig(f"{output_folder}/{name}-consumer.png", bbox_inches="tight")

        ##############################################
        ## PARTITION PLOTS
        ##############################################
        print("Building partition plots")
        fig = plt.figure(constrained_layout=True)
        fig.suptitle(f"Experiment {name} Partitions", fontsize=18)
        fig.set_size_inches(20, 20)
        fig.tight_layout(h_pad=3.5, rect=[0, 0.03, 1, 0.5])

        axs = {}

        linestyles = {}
        markers = {}

        counter = 0

        lines, names = make_legends(
            consumer_partitions,
            consumer_plot_metadata=consumer_plot_metadata,
            linestyles=linestyles,
        )

        for partition_idx, partition in enumerate(msg_based["partitions"]):
            if str(partition) not in linestyles:
                linestyles[str(partition)] = LINESTYLES[partition_idx % len(LINESTYLES)]
                markers[str(partition)] = MARKERS[partition_idx % len(MARKERS)]
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
                        msg_based["partitions"][partition][diff][metric],
                        partition=partition,
                        consumer_metadata=consumer_metadata,
                        consumer_plot_metadata=consumer_plot_metadata,
                        linestyle=linestyles[str(partition)],
                    )
                    axs[key].add_collection(lc)
                    axs[key].autoscale()
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})"
                        if unit is not None
                        else f"{label}",
                    )
                    axs[key].legend(lines, names)

        counter += 2  # 2 rows are already occupied by msg_based
        for partition_idx, partition in enumerate(time_based["partitions"]):
            if str(partition) not in linestyles:
                linestyles[str(partition)] = LINESTYLES[partition_idx % len(LINESTYLES)]
                markers[str(partition)] = MARKERS[partition_idx % len(MARKERS)]
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
                            time_based["partitions"][partition][diff][metric],
                            partition=partition,
                            consumer_metadata=consumer_metadata,
                            consumer_plot_metadata=consumer_plot_metadata,
                            linestyle=linestyles[str(partition)],
                        )
                        axs[key].add_collection(lc)
                        axs[key].autoscale()
                        axs[key].set_title(f"{label}")
                        axs[key].set(
                            xlabel=f"duration (in seconds)",
                            ylabel=f"{label} (in {unit})"
                            if unit is not None
                            else f"{label}",
                        )
                        axs[key].legend(lines, names)
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
                        time_based["partitions"][partition][diff],
                        partition=partition,
                        consumer_metadata=consumer_metadata,
                        consumer_plot_metadata=consumer_plot_metadata,
                        linestyle=linestyles[str(partition)],
                    )

                    axs[key].add_collection(lc)
                    axs[key].autoscale()
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})"
                        if unit is not None
                        else f"{label}",
                    )
                    axs[key].legend(lines, names)

        # handles, labels = ax.get_legend_handles_labels()
        # fig.legend(lines, names, loc="lower center", ncol=len(lines))

        # for key in axs:
        # axs[key].legend(lines, names, ncol=3)
        print("Saving partition plots")
        plt.savefig(f"{output_folder}/{name}-partition.png", bbox_inches="tight")


experiments, output_folder = fetch_experiment_data()

plot_experiments(experiments, output_folder)
