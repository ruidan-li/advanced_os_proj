import sys
from os import listdir
from os.path import isfile, join
import os

"""
    read a client's data
"""
def process_one_file(fh):
    time_diff_list, indx_diff_list = [], []
    for line in fh.readlines()[1:]: # remove header
        line = line.split(",")
        assert len(line) == 5 # send_time,timestamp,partition,latest_offset,current_position
        time_diff = float(line[1]) - float(line[0])
        indx_diff = int(line[3]) - int(line[4]) - 1
        # print(time_diff, indx_diff)
        time_diff_list.append(time_diff)
        indx_diff_list.append(indx_diff)
    return time_diff_list, indx_diff_list

"""
    aggregate all client's data
"""
def convert_multi_list_to_avg_list(multi_list):
    avg_list = []
    min_len = min([len(l) for l in multi_list]) # each consumer (l) may have different number of ticks, pick the min
    multi_list = [l[-min_len:] for l in multi_list] # pick the last min_len ticks from each consumer
    for i in range(min_len): # for tick 1, 2, 3, 4, 5, ...
        sum_of_dalay = 0
        for j in range(len(multi_list)): # for each client j
            sum_of_dalay += multi_list[j][i]
        avg_list.append(sum_of_dalay / len(multi_list))
    assert len(avg_list) == min_len
    return avg_list



time_diff_multi_list, indx_diff_multi_list = [], [] # each entry is a time_diff_list/indx_diff_list
for (dirname, dirs, files) in os.walk(join(os.getcwd(), "logs")):        
    for filename in files:
        # print(filename)
        with open(join(dirname, filename)) as f:
            time_diff_list, indx_diff_list = process_one_file(f)
            # print(len(time_diff_list), len(indx_diff_list))
            time_diff_multi_list.append(time_diff_list)
            indx_diff_multi_list.append(indx_diff_list)


time_diff_avg_list = convert_multi_list_to_avg_list(time_diff_multi_list)
indx_diff_avg_list = convert_multi_list_to_avg_list(indx_diff_multi_list)

with open(f"{sys.argv[1]}/diff_time.txt", "w") as f:
    for i in time_diff_avg_list:
        f.write(f"{i}\n")

with open(f"{sys.argv[1]}/diff_indx.txt", "w") as f:
    for i in indx_diff_avg_list:
        f.write(f"{i}\n")