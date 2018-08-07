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
SLICE_CNT = 98
ISOTIMEFORMAT = "%Y-%m-%d %X"

MACHINE_CNT = 6000
APP_CNT = 9338
INST_CNT = 68219
        
def getCurrentTime():
    return "[%s]" % (time.strftime(ISOTIMEFORMAT, time.localtime()))

def split_slice(slice):
    return np.array(list(map(float, slice.split('|'))))

def score_of_cpu_percent_slice(slice):
    tmp = np.where(np.less(slice, 0.001), 0, slice)
    return np.where(np.greater(tmp, 0), \
                    1 + 10 * (np.exp(np.maximum(0, tmp - 0.5)) - 1), \
                    0).sum()

def print_and_log(msg):
    print(getCurrentTime(), msg)
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

data_set = 'b'

ALPHA = 1.0 #启发因子，信息素的重要程度
BETA = 2.0  #期望因子
ROU = 0.5   #信息素残留参数

MAX_SCORE_DIFF = 0.3

def find_insert_pos_no(score_list, score, s, e):
    list_len = e - s
    if (list_len == 2):
        return e
    
    mid = int((e + s)/2)
    if (score_list[mid - 1] < score and score_list[mid] > score):
        return mid

    if (score_list[mid] < score and score_list[mid + 1] > score):
        return mid + 1
    
    if (score < score_list[mid]):
        return find_insert_pos(score_list, score, s, mid)
    
    if (score > score_list[mid]):
        return find_insert_pos(score_list, score, mid + 1, e)
    
    return mid

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

