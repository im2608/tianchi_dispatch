'''
Created on May 17, 2018

@author: Heng.Zhang
'''

import sys
import time

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
def getCurrentTime():
    return "[%s]" % (time.strftime(ISOTIMEFORMAT, time.localtime()))

