from cProfile import label
import matplotlib.pyplot as plt
import numpy as np
import sys
from os import listdir
from os.path import isfile, join
import os


rfs = [3]
pas = [4]
cos = [2]
pos = [4]
vr=1001


def fetch_experiment_data(fname):
    experiment_timediff = []

    for rf in rfs:
        for pa in pas:
            for co in cos:
                for po in pos:
                    folder = f"topic-rf{rf}-pa{pa}-co{co}-po{po}-vr{vr}"
                    fpath = join(os.getcwd(), f"res/{folder}/{fname}.txt")
                    with open(fpath) as f:
                        lat = [float(l.strip() ) for l in f.readlines()]
                        print(fpath, len(lat))
                        experiment_timediff.append((folder, lat))

    return experiment_timediff

def plot_experiment_data(experiment_timediff):
    fig = plt.figure()
    for data in experiment_timediff:
        length = len(data[1])
        plt.plot(range(length), data[1], label=data[0])
        plt.ylim([0, 40])
    plt.legend()
    plt.show()

def new_plot_experiment_data(time_diff, indx_diff, tput, lats):
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3)
    plt.show()

experiment_time = fetch_experiment_data("diff_time")
experiment_indx = fetch_experiment_data("diff_indx")
experiment_tput = fetch_experiment_data("tput")
experiment_lats = fetch_experiment_data("lats")

new_plot_experiment_data(experiment_time, experiment_indx, experiment_tput, experiment_lats)

# print(len(experiment_timediff))
# plot_experiment_data(experiment_timediff)

# for (dirname, dirs, files) in os.walk(join(os.getcwd(), "res")):        
#     for filename in files:
#         if "diff_time.txt" in filename:
#             with open(join(dirname, filename)) as f:
#                 lat = [float(l.strip() ) for l in f.readlines()]
#                 print(dirname, len(lat))