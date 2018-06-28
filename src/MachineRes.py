'''
Created on Jun 25, 2018

@author: Heng.Zhang
'''
 
from global_param import *
import numpy as np

class MachineRes(object):
    def __init__(self, each_machine):
        self.machine_id = each_machine[0]
        self.cpu = float(each_machine[1]) # cpu 容量
        self.mem = float(each_machine[2]) # mem
        self.disk = float(each_machine[3]) # 剩余 disk
        self.p = float(each_machine[4]) # 剩余 p
        self.m = float(each_machine[5]) # 剩余 m
        self.pm = float(each_machine[6]) # 剩余 pm
        
        self.cpu_useage = 0
        self.cpu_slice = np.array(np.zeros(0))     # cpu 使用量
        self.mem_slice  = np.array(np.zeros(0))     # mem 使用量
        self.cpu_percentage = 0 # cpu 使用率 = cpu 使用量 / cpu 容量
    
        # 剩余可用资源
        self.res_sum = 1 - self.cpu_percentage + self.mem + self.disk + self.p + self.m + self.pm
        return

    def to_string(self):    
        return '%s, cp %.6f, c %.6f, m %d, d %d, p %d, m %d, pm %d' % \
              (self.machine_id, 
              self.mem * MAX_MEM, 
              self.disk * MAX_DISK, 
              self.p * MAX_P, 
              self.m * MAX_M, 
              self.pm * MAX_PM)

    # 得到剩余可用资源
    def get_res_sum(self):
        return self.res_sum
       
    def update_machine_res(self, app_res, ratio):
        self.mem_slice += ratio * app_res.mem_slice
        self.cpu_slice += ratio * app_res.cpu_slice
        self.disk += ratio * app_res.disk
        self.p += ratio * app_res.p
        self.m += ratio * app_res.m
        self.pm += ratio * app_res.pm
        
    # 机器资源是否能够容纳 inst
    def meet_inst_res_require(self, app_res):
        return (np.all(self.cpu_slice >= app_res.cpu_slice) and  
                np.all(self.mem_slice >= app_res.mem_slice) and
                self.disk >= app_res.disk and
                self.p >= app_res.p and
                self.m >= app_res.m  and
                self.pm >= app_res.pm)
        
