#coding=utf-8
'''
Created on May 17, 2018

@author: Heng.Zhang
'''

import sys
import time
import numpy as np
import logging
import copy

runningPath = sys.path[0]
sys.path.append("%s\\features\\" % runningPath)

RELEASE_RATIO = 1
DISPATCH_RATIO = -1

MAX_CPU = 92
MAX_MEM = 288
MAX_DISK = 1024
MAX_P = 7
MAX_M = 7
MAX_PM = 9
SLICE_CNT = 98 * 15
ISOTIMEFORMAT = "%Y-%m-%d %X"

MACHINE_CNT = 6000
APP_CNT = 9338
INST_CNT = 68219

def getCurrentTime():
    return "[%s]" % (time.strftime(ISOTIMEFORMAT, time.localtime()))

def split_slice(slice):
    return np.array(list(map(float, slice.split('|'))))

def score_of_cpu_percent_slice(slice, running_inst_cnt):
    tmp = np.where(np.less(slice, 0.0001), 0, slice)
    return np.where(np.greater(tmp, 0), \
                    1 + (1 + running_inst_cnt) * (np.exp(np.maximum(0, tmp - 0.5)) - 1), \
                    0).sum()

def print_and_log(msg, print_new_line=True):
    if (print_new_line):
        print(getCurrentTime(), msg)
    else:
        print(getCurrentTime(), "%s\r" % msg, end='')
    logging.info(msg)

# return True for small machine, False for big machine otherwise
def does_prefer_small_machine(app_res):
    return False

    cpu_mean = np.mean(app_res.get_cpu_slice())
    mem_mean = np.mean(app_res.get_mem_slice())
    disk = app_res.get_disk()
    
    if (data_set == 'a'):
        if (cpu_mean < 16 and mem_mean < 64 and disk < 500):
            return True

        return False
    else:
        if (cpu_mean < 16):
            return True

        return False

data_set = 'sf'

ALPHA = 1.0 #启发因子，信息素的重要程度
BETA = 2.0  #期望因子
ROU = 0.5   #信息素残留参数

MAX_SCORE_DIFF = 10

def find_insert_pos(score_list, score, s, e):
    mid = int((e + s)/2)
    if (score < score_list[mid]):
        if (score > score_list[mid - 1]):        
            return mid
        else:
            return find_insert_pos(score_list, score, s, mid - 1)
    else:
        if (score < score_list[mid + 1]):        
            return mid + 1
        else:
            return find_insert_pos(score_list, score, mid + 1, e)

def append_score_by_score_diff(score_list, score):
    appended = False
    if (len(score_list) == 0):
        score_list.append(score)
        return True, score_list
    
    if (score < score_list[0]):
        if (score_list[0] - score >= MAX_SCORE_DIFF):
            score_list.insert(0, score)
            return True, score_list
        else: 
            return False, score_list
        
    if (score > score_list[-1]):
        if (score - score_list[-1] >= MAX_SCORE_DIFF):
            score_list.append(score)
            return True, score_list
        else:
            return False, score_list

    s = 0
    e = len(score_list)
    pos = None
    while (s <= e):
        m = int((s + e)/2)
        if (score <= score_list[m]):
            if (score >= score_list[m - 1]):
                pos = m
                break
            else:
                e = m - 1
        else:
            if (score <= score_list[m + 1]):
                pos = m + 1
                break
            else:
                s = m + 1

    if (pos is not None and score > score_list[pos - 1] and score < score_list[pos] and
        score - score_list[pos - 1] >= MAX_SCORE_DIFF and score - score_list[pos] <= -MAX_SCORE_DIFF):                            
        score_list.insert(pos, score)
        appended = True
        
            
#     for i in range(len(score_list) - 1):
#         if (score > score_list[i] and score < score_list[i + 1] and
#             score - score_list[i] >= MAX_SCORE_DIFF and score - score_list[i + 1] <= -MAX_SCORE_DIFF):                            
#             score_list.insert(i + 1, score)
#             appended = True
#             break                

    return appended, score_list

BASE_SCORE = 1500
g_prefered_machine = {
    'a' :(1, 8000),
    'b' :(1, 8000),
    'c' :(6001, 9000),
    'd' :(6001, 9000),
    'e' :(6001, 8000),
    }

g_max_offline_job_step = {'a' : 10, 'b':9, 'c':10, 'd':8}

g_job_cnt = {'a':5241, 'b':5637, 'c':2840, 'd':2250}

# cpu 最小剩余容量 for dispatching offline jobs
g_min_cpu_left_useage_per = {'a':0.5, 'b':0.5, 'c':0.5, 'd':0.5, 'e':0}

g_extend_idle_machine_cnt = 1