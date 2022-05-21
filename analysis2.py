import sys
from os import listdir
from os.path import isfile, join
import os
import json

def process_one_file(fname):
    with open(fname) as fh:
        lst = []
        # create a list of dictionaries
        for line in fh.readlines():
            lst.append(json.loads(line))
        return lst

def align_multiple_files(multi_lst):
    min_len = min([len(l) for l in multi_lst]) # each consumer (l) may have different number of ticks, pick the min
    return [l[-min_len:] for l in multi_lst] # pick the last min_len ticks from each consumer

def extract_dict(multi_lst, dict_name):
    extracted_list = []
    for client in multi_lst:
        client_list = []
        for line in client:
            all_list = line[dict_name]["all"]
            client_list.append(all_list)
        extracted_list.append(client_list)
    return extracted_list

def extract_avg(extracted_list, list_idx):
    result_list = []
    for i in range(len(extracted_list[0])): # tick
        tick_sum = 0
        for c in range(len(extracted_list)): # client
            tick_sum += extracted_list[c][i][list_idx]
        result_list.append(round(tick_sum/len(extracted_list), 3))
    return result_list

def get_4_stats(lsts, dict_name): # time_diff, indx_diff, latencies
    extr = extract_dict(lsts, dict_name)
    avg = extract_avg(extr, 0)
    p50 = extract_avg(extr, 1)
    p90 = extract_avg(extr, 2)
    p99 = extract_avg(extr, 3)
    return avg, p50, p90, p99

def get_avg_tput(lsts):
    extracted_list = extract_dict(lsts, "processed")
    result_list = []
    for i in range(len(extracted_list[0])): # tick
        tick_sum = 0
        for c in range(len(extracted_list)): # client
            tick_sum += extracted_list[c][i]
        result_list.append(round(tick_sum/len(extracted_list), 3))
    return result_list

def get_sum_tput(lsts):
    extracted_list = extract_dict(lsts, "processed")
    result_list = []
    for i in range(len(extracted_list[0])): # tick
        tick_sum = 0
        for c in range(len(extracted_list)): # client
            tick_sum += extracted_list[c][i]
        result_list.append(round(tick_sum, 3))
    return result_list


def extract_from_logs_cntr():
    lsts = []
    for (dirname, dirs, files) in os.walk(join(os.getcwd(), "logs_cntr")):        
        for filename in files:
            lst = process_one_file(join(dirname, filename))
            lsts.append(lst)
    lsts = align_multiple_files(lsts)

    return get_4_stats(lsts, "time_diff"), get_4_stats(lsts, "idex_diff")

def extract_from_logs_time():
    lsts = []
    for (dirname, dirs, files) in os.walk(join(os.getcwd(), "logs_time")):        
        for filename in files:
            lst = process_one_file(join(dirname, filename))
            lsts.append(lst)
    lsts = align_multiple_files(lsts)

    return get_4_stats(lsts, "latencies"),  (get_avg_tput(lsts), get_sum_tput(lsts))


from pprint import pprint

# res_obj = (
#   ((tavg, tp50, tp90, tp99), (iavg, ip50, ip90, ip99)), 
#   ((lavg, lp50, lp90, lp99), (avg_tput, sum_tput))
# )
res_obj = extract_from_logs_cntr(), extract_from_logs_time()

import pickle 
filehandler = open(f"{sys.argv[1]}/res_obj.pickle", "wb")
pickle.dump(res_obj, filehandler)