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

MAX_SCORE_DIFF = 1
def append_score_by_score_diff(score_list, score):
    score_list = sorted(score_list)
    appended = False
    if (len(score_list) == 0):
        score_list.append(score)
        appended = True
    elif (len(score_list) == 1):
        if (abs(score - score_list[0]) >= MAX_SCORE_DIFF):
            score_list.append(score)
            appended = True
    else:
        if ((score_list[0] < score and score_list[0] - score >= MAX_SCORE_DIFF) 
            or 
            (score > score_list[-1] and score - score_list[-1] >= MAX_SCORE_DIFF)):
            score_list.append(score)
            appended = True
        for i in range(len(score_list) - 1):
            if (score > score_list[i] and score < score_list[i + 1] and
                score - score_list[i] >= MAX_SCORE_DIFF and score - score_list[i + 1] <= -MAX_SCORE_DIFF):                            
                score_list.append(score)
                appended = True

    if (appended):
        score_list = sorted(score_list)

    return appended, score_list

