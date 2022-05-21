import sys
from os import listdir
from os.path import isfile, join
import os

"""
    read a client's data
"""
def process_one_file(fh):
    time_diff_list, indx_diff_list = [], []
    tput_list, lats_list = [], []

    for line in fh.readlines()[1:]: # remove header
        line = line.split(",")
        assert len(line) == 7 # send_time,timestamp,partition,latest_offset,current_position
        time_diff = float(line[1]) - float(line[0])
        indx_diff = int(line[3]) - int(line[4])
        # print(time_diff, indx_diff)
        time_diff_list.append(time_diff)
        indx_diff_list.append(indx_diff)
        tput_list.append(float(line[5]))
        lats_list.append(float(line[6]))

        
    return time_diff_list, indx_diff_list, tput_list, lats_list

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
tput_multi_list, lats_multi_list = [], []
for (dirname, dirs, files) in os.walk(join(os.getcwd(), "logs")):        
    for filename in files:
        print(filename)
        with open(join(dirname, filename)) as f:
            time_diff_list, indx_diff_list, tput_list, lats_list = process_one_file(f)
            # print(len(time_diff_list), len(indx_diff_list))
            time_diff_multi_list.append(time_diff_list)
            indx_diff_multi_list.append(indx_diff_list)
            tput_multi_list.append(tput_list)
            lats_multi_list.append(lats_list)


time_diff_avg_list = convert_multi_list_to_avg_list(time_diff_multi_list)
indx_diff_avg_list = convert_multi_list_to_avg_list(indx_diff_multi_list)
tput_avg_list = convert_multi_list_to_avg_list(tput_multi_list)
lats_avg_list = convert_multi_list_to_avg_list(lats_multi_list)

# diff_time
# diff_indx
# tput
# lats

def write_to_file(fname, lst):
    with open(f"{sys.argv[1]}/{fname}.txt", "w") as f:
        for i in lst:
            f.write(f"{i}\n")

write_to_file("diff_time", time_diff_avg_list)
write_to_file("diff_indx", indx_diff_avg_list)
write_to_file("tput", tput_avg_list)
write_to_file("lats", lats_avg_list)