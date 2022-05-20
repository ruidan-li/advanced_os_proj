from cProfile import label
import matplotlib.pyplot as plt
import numpy as np
import sys
from os import listdir
from os.path import isfile, join
import os


rfs = [3]
pas = [16]
cos = [8]
pos = [9]
vr=62


def fetch_experiment_data():
    experiment_timediff = []

    for rf in rfs:
        for pa in pas:
            for co in cos:
                for po in pos:
                    folder = f"topic-rf{rf}-pa{pa}-co{co}-po{po}-vr{vr}"
                    fpath = join(os.getcwd(), f"res/{folder}/diff_time.txt")
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


experiment_timediff = fetch_experiment_data()
print(len(experiment_timediff))
plot_experiment_data(experiment_timediff)

# for (dirname, dirs, files) in os.walk(join(os.getcwd(), "res")):        
#     for filename in files:
#         if "diff_time.txt" in filename:
#             with open(join(dirname, filename)) as f:
#                 lat = [float(l.strip() ) for l in f.readlines()]
#                 print(dirname, len(lat))