import os
import pickle
import shutil
import sys
from os.path import join

import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D

rfs = [3]
pas = [4]
cos = [4]
pos = [4]

# format
# { version_1: [partition_1, partition_2, ...] }
vrs_partitions = {
    48: [1],
    70: [0],
}


def fetch_experiment_data():
    experiments = []

    for vr in vrs_partitions:
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
                        experiments.append((vr, vrs_partitions[vr], param, res_obj))
    print("num of experiments loaded:", len(experiments))

    pathname = "_".join(
        [
            "-".join(map(str, rfs)),
            "-".join(map(str, pas)),
            "-".join(map(str, cos)),
            "-".join(map(str, pos)),
            "-".join(map(str, list(vrs_partitions.keys()))),
        ]
    )

    name = f"RF ({','.join(map(str, rfs))}), Versions ({','.join(map(str, list(vrs_partitions.keys())))})"

    output_folder = "./fig/" + pathname

    print(f"Creating output directory {output_folder} if does not exist")
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder, ignore_errors=True)

    os.makedirs(output_folder)

    return name, experiments, output_folder, pathname


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


def emit_x_and_y_partitions(
    data,
    color,
):
    # data should be sorted
    # get first timestamp

    # ts = data[0]["timestamp"]

    segments = []
    line_colors = []

    for idx, (tick, tick_next) in enumerate(zip(data, data[1:])):
        # metadata = consumer_metadata[str(tick["pid"])]
        # if metadata["sleep"]:
        #     if (
        #         tick["timestamp"] >= metadata["sleep_start_ts"]
        #         and tick_next["timestamp"] <= metadata["sleep_end_ts"]
        #     ):
        #         continue

        segments.append(
            [
                (idx, tick["value"]),
                (idx, tick_next["value"]),
            ]
        )
        line_colors.append(color)

    return LineCollection(
        segments,
        colors=line_colors,
        linewidths=2,
    )


def make_legends(partition_plot_metadata, **kwargs):
    lines = []
    names = []
    for (
        partition
    ) in partition_plot_metadata:  # { partition_with_version_key: { idx, color } }
        # for partition in consumer_partitions[consumer]:
        lines.append(
            Line2D(
                [0, 1],
                [0, 1],
                color=partition_plot_metadata[partition]["color"],
                # linestyle=linestyles[partition],
                **kwargs,
            )
        ),
        names.append(f"{partition}")

    return lines, names


def check_partitions_in_exp(vr, choosen_partitions, partition_consumers, verbose=False):
    for choosen_partition in choosen_partitions:
        p = str(choosen_partition)
        if p not in partition_consumers:
            if verbose:
                print(f"Error, partition {p} not found in version {vr}")
            return False

    return True


def get_partition_key(vr, partition):
    return f"p#{partition}-vr#{vr}"


def plot_experiments(name, experiments, output_folder, pathname):
    consumer_plot_metadata = {}
    partition_plot_metadata = {}

    consumer_counter = 0
    partition_counter = 0

    # choosen partitions container
    partitions = {
        "idx_diff": {
            "avg": {},
            "p50": {},
            "p90": {},
            "p99": {},
        },
        "time_diff": {
            "avg": {},
            "p50": {},
            "p90": {},
            "p99": {},
        },
        "latencies": {
            "avg": {},
            "p50": {},
            "p90": {},
            "p99": {},
        },
        "throughput": {},
    }

    print("Aggregating data from the experiments")

    for vr, choosen_partitions, param, exp in experiments:
        (
            consumer_metadata,
            msg_based,
            time_based,
            partition_consumers,
            consumer_partitions,
        ) = exp

        # sanity checck
        if not check_partitions_in_exp(
            vr,
            choosen_partitions=choosen_partitions,
            partition_consumers=partition_consumers,
            verbose=True,
        ):
            sys.exit(1)

        choosen_partitions = [str(p) for p in choosen_partitions]

        # populating consumer metadata
        for consumer_idx, consumer in enumerate(consumer_partitions):
            consumer_key = f"c#{consumer}-vr#{vr}"
            if consumer_key not in consumer_plot_metadata:
                consumer_counter += 1
                consumer_plot_metadata[consumer_key] = {
                    "idx": consumer_idx + consumer_counter,
                    "color": COLORS[(consumer_idx + consumer_counter) % len(COLORS)],
                }

        # populating partition metadata
        for partition_idx, choosen_partition in enumerate(choosen_partitions):
            partition_key = get_partition_key(vr, choosen_partition)
            if partition_key not in partition_plot_metadata:
                partition_counter += 1
                partition_plot_metadata[partition_key] = {
                    "idx": partition_idx + partition_counter,
                    "color": COLORS[(partition_idx + partition_counter) % len(COLORS)],
                }

        # populate partition containers from msg_based data
        for partition_idx, partition in enumerate(msg_based["partitions"]):
            if str(partition) not in choosen_partitions:
                continue

            for diff_idx, diff in enumerate(
                msg_based["partitions"][partition]
            ):  # idx_diff, time_diff
                for metric_idx, metric in enumerate(
                    msg_based["partitions"][partition][diff]
                ):  # avg, p50, p90, p99
                    key = f"p#{partition}-vr#{vr}"
                    partitions[diff][metric][key] = msg_based["partitions"][partition][
                        diff
                    ][metric]

        # populate partition containers from time_based data
        for partition_idx, partition in enumerate(time_based["partitions"]):
            if partition not in choosen_partitions:
                continue

            for diff_idx, diff in enumerate(
                time_based["partitions"][partition]
            ):  # idx_diff, time_diff
                if type(time_based["partitions"][partition][diff]) is dict:
                    for metric_idx, metric in enumerate(
                        time_based["partitions"][partition][diff]
                    ):  # avg, p50, p90, p99
                        key = get_partition_key(vr, partition)
                        partitions[diff][metric][key] = time_based["partitions"][
                            partition
                        ][diff][metric]
                else:  # throughput
                    key = get_partition_key(vr, partition)
                    partitions[diff][key] = time_based["partitions"][partition][diff]

    print("Building partition plots")
    fig = plt.figure(constrained_layout=True)
    fig.suptitle(f"Experiment [{name}] Partitions Plot", fontsize=18)
    fig.set_size_inches(20, 20)
    fig.tight_layout(h_pad=3.5, rect=[0, 0.03, 1, 0.5])

    axs = {}

    lines, names = make_legends(partition_plot_metadata)

    for diff_idx, diff in enumerate(partitions):
        for metric_or_partition_idx, metric_or_partition in enumerate(partitions[diff]):
            if type(partitions[diff][metric_or_partition]) is dict:
                for partition_idx, partition in enumerate(
                    partitions[diff][metric_or_partition]
                ):
                    key = f"{diff_idx},{metric_or_partition_idx}"
                    if key not in axs:
                        axs[key] = plt.subplot2grid(
                            (ROWS, COLS),
                            (diff_idx, metric_or_partition_idx),
                            colspan=1,
                        )
                    unit, label = get_label(diff, metric_or_partition)
                    lc = emit_x_and_y_partitions(
                        partitions[diff][metric_or_partition][partition],
                        color=partition_plot_metadata[partition]["color"],
                    )
                    axs[key].add_collection(lc)
                    axs[key].autoscale()
                    axs[key].set_title(f"{label}")
                    axs[key].set(
                        # xlabel=f"duration (in seconds)",
                        ylabel=f"{label} (in {unit})"
                        if unit is not None
                        else f"{label}",
                    )
                    axs[key].legend(lines, names)
            else:  # special case for throughput
                key = f"{diff_idx},0"
                if key not in axs:
                    axs[key] = plt.subplot2grid(
                        (ROWS, COLS),
                        (diff_idx, 0),
                        colspan=COLS,
                    )
                unit, label = get_label(diff, "all")
                lc = emit_x_and_y_partitions(
                    partitions[diff][metric_or_partition],
                    color=partition_plot_metadata[metric_or_partition]["color"],
                )
                axs[key].add_collection(lc)
                axs[key].autoscale()
                axs[key].set_title(f"{label}")
                axs[key].set(
                    # xlabel=f"",
                    ylabel=f"{label} (in {unit})"
                    if unit is not None
                    else f"{label}",
                )
                axs[key].legend(lines, names)

    print("Saving partition plots")
    plt.savefig(f"{output_folder}/{pathname}-partition.png", bbox_inches="tight")


name, experiments, output_folder, pathname = fetch_experiment_data()

plot_experiments(name, experiments, output_folder, pathname)
