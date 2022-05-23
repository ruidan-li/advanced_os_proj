from cProfile import label
import matplotlib.pyplot as plt
import numpy as np
import sys
from os import listdir
from os.path import isfile, join
import os
import pickle


rfs = [3]
pas = [4]
cos = [4]
pos = [4]
vrs = [226]

def fetch_experiment_data():
    experiments= []

    for vr in vrs:
        for rf in rfs:
            for pa in pas:
                for co in cos:
                    for po in pos:
                        param = f"rf{rf}-pa{pa}-co{co}-po{po}-vr{vr}"
                        param_short = f"{rf};{pa};{co};{po};{vr}"
                        folder = f"topic-{param}"
                        fpath = join(os.getcwd(), f"res/{folder}/res_obj.pickle")
                        fh = open(fpath, 'rb') 
                        res_obj = pickle.load(fh)
                        experiments.append((param_short, res_obj))
    
    print("num of experiments loaded:", len(experiments))
    return experiments

def emit_x_and_y(data):
    return range(len(data)), data

def plot_experiments(experiments, metric="avg"):
    fig, axs = plt.subplots(4, 4) # time diff, idex diff, lats, tputs,
    fig.set_size_inches(13,13)
    fig.tight_layout(h_pad=3.5)

    for param, res_obj in experiments:
            axs[0,0].plot(*emit_x_and_y(res_obj[0][0][0]), label=f"{param}") # tavg
            axs[0,1].plot(*emit_x_and_y(res_obj[0][1][0]), label=f"{param}") # iavg
            axs[0,2].plot(*emit_x_and_y(res_obj[1][0][0]), label=f"{param}") # lavg
            axs[0,3].plot(*emit_x_and_y(res_obj[1][1][0]), label=f"{param}") # avg_tput

            axs[1,0].plot(*emit_x_and_y(res_obj[0][0][1]), label=f"{param}") # tp50
            axs[1,1].plot(*emit_x_and_y(res_obj[0][1][1]), label=f"{param}") # ip50
            axs[1,2].plot(*emit_x_and_y(res_obj[1][0][1]), label=f"{param}") # lp50
            axs[1,3].plot(*emit_x_and_y(res_obj[1][1][1]), label=f"{param}") # sum_tput

            axs[2,0].plot(*emit_x_and_y(res_obj[0][0][2]), label=f"{param}") # tp90
            axs[2,1].plot(*emit_x_and_y(res_obj[0][1][2]), label=f"{param}") # ip90
            axs[2,2].plot(*emit_x_and_y(res_obj[1][0][2]), label=f"{param}") # lp90

            axs[3,0].plot(*emit_x_and_y(res_obj[0][0][3]), label=f"{param}") # tp99
            axs[3,1].plot(*emit_x_and_y(res_obj[0][1][3]), label=f"{param}") # ip99
            axs[3,2].plot(*emit_x_and_y(res_obj[1][0][3]), label=f"{param}") # lp99


    # line 22, 27 of consumer.py
    sampling_ival = 5000 # msgs
    sampling_time = 1000 # ms

    axs[0,0].set_title(f"average time diff")
    axs[0,0].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average latencies (in sec)")

    axs[0,1].set_title(f"avergage index diff")
    axs[0,1].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average index diff")

    axs[0,2].set_title(f"average latency")
    axs[0,2].set(xlabel=f"# of passed sampling time ({sampling_time} ms)", ylabel="average latencies (in sec)")

    axs[0,3].set_title(f"avergage thoroughput")
    axs[0,3].set(xlabel=f"# of passed sampling time ({sampling_time} ms)", ylabel="avergage of thoroughputs (recv/sec)")


    axs[1,0].set_title(f"50th %tile time diff")
    axs[1,0].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average latencies (in sec)")

    axs[1,1].set_title(f"50th %tile index diff")
    axs[1,1].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average index diff")

    axs[1,2].set_title(f"50th %tile latency")
    axs[1,2].set(xlabel=f"# of passed sampling time ({sampling_time} ms)", ylabel="average latencies (in sec)")

    axs[1,3].set_title(f"sum of thoroughputs")
    axs[1,3].set(xlabel=f"# of passed sampling time ({sampling_time} ms)", ylabel="sum of thoroughputs (recv/sec)")


    axs[2,0].set_title(f"90th %tile time diff")
    axs[2,0].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average latencies (in sec)")

    axs[2,1].set_title(f"90th %tile index diff")
    axs[2,1].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average index diff")

    axs[2,2].set_title(f"90th %tile latency")
    axs[2,2].set(xlabel=f"# of passed sampling time ({sampling_time} ms)", ylabel="average latencies (in sec)")


    axs[3,0].set_title(f"99th %tile time diff")
    axs[3,0].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average latencies (in sec)")

    axs[3,1].set_title(f"99th %tile index diff")
    axs[3,1].set(xlabel=f"# of passed sampling interval ({sampling_ival} msgs)", ylabel="average index diff")

    axs[3,2].set_title(f"99th %tile latency")
    axs[3,2].set(xlabel=f"# of passed sampling time ({sampling_time} ms)", ylabel="average latencies (in sec)")


    for rs in axs:
        for c in rs:
            c.legend()
    # plt.show()
    title = '_'.join(['-'.join(map(str, rfs)),
            '-'.join(map(str, pas)),
            '-'.join(map(str, cos)),
            '-'.join(map(str, pos)),
            '-'.join(map(str, vrs))])
    print('./fig/{title}.png')
    plt.savefig(f'./fig/{title}.png', bbox_inches='tight')

plot_experiments(fetch_experiment_data())