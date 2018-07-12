'''
Created on Jun 25, 2018

@author: Heng.Zhang
'''
 
from global_param import *
import numpy as np

class MachineRes(object):
    def __init__(self, each_machine):
        self.machine_id = int(each_machine[0])
        self.cpu = float(each_machine[1]) # cpu 容量
        self.mem = float(each_machine[2]) #  mem
        self.disk = float(each_machine[3]) # 剩余 disk
        self.p = float(each_machine[4]) # 剩余 p
        self.m = float(each_machine[5]) # 剩余 m
        self.pm = float(each_machine[6]) # 剩余 pm

        self.cpu_useage = 0
        self.cpu_slice = np.array(np.zeros(SLICE_CNT) + self.cpu)  # 剩余 cpu
        self.mem_slice  = np.array(np.zeros(SLICE_CNT) + self.mem) # 剩余 mem 
        self.cpu_percentage = 0 # cpu 使用率 = cpu slice max 使用量 / cpu 容量
        
        self.machine_score = 0
    
        # 剩余可用资源
        self.res_sum = 1 - self.cpu_percentage + self.mem + self.disk + self.p + self.m + self.pm
        return

    def to_string(self):    
        return '%s, cp %.6f, d %d, p %d, m %d, pm %d' % \
              (self.machine_id, 
               self.cpu_percentage,
              self.disk, 
              self.p, 
              self.m, 
              self.pm)

    def to_full_string(self):    
        return '%s, cp %.6f, c %s, m %s, d %d, p %d, m %d, pm %d' % \
              (self.machine_id, 
               self.cpu_percentage,
               self.cpu_slice,
               self.mem_slice,
              self.disk, 
              self.p, 
              self.m, 
              self.pm)              

    # 得到剩余可用资源
    def get_res_sum(self):
        return self.res_sum
       
    def update_machine_res(self, app_res, ratio):
        self.mem_slice += ratio * app_res.mem_slice
        self.cpu_slice += ratio * app_res.cpu_slice

        # slice 由于误差可能不会为0， 这里凡是 < 0.001 的slice 都设置成0
        self.cpu_slice = np.where(np.less(self.cpu_slice, 0.001), 0, self.cpu_slice)
        self.mem_slice = np.where(np.less(self.mem_slice, 0.001), 0, self.mem_slice)

        self.disk += ratio * app_res.disk
        self.p += ratio * app_res.p
        self.m += ratio * app_res.m
        self.pm += ratio * app_res.pm
        
        self.cpu_percentage = self.cpu_slice.max() / self.cpu
        
        self.machine_score = score_of_cpu_percent_slice((self.cpu - self.cpu_slice) / self.cpu)
        
    # 机器资源是否能够容纳 inst
    def meet_inst_res_require(self, app_res):
        return (np.all(self.cpu_slice >= app_res.cpu_slice) and  
                np.all(self.mem_slice >= app_res.mem_slice) and
                self.disk >= app_res.disk and
                self.p >= app_res.p and
                self.m >= app_res.m  and
                self.pm >= app_res.pm)
        
    @staticmethod
    def sum_machine_remaining_res(sorted_machine_res):
        cpu_slice = np.array(np.zeros(SLICE_CNT))
        mem_slice = np.array(np.zeros(SLICE_CNT))
        disk = 0
        p = 0
        m = 0
        pm = 0
        
        for machine_id, machine_res in sorted_machine_res:
            cpu_slice += machine_res.running_machine_res.cpu_slice
            mem_slice += machine_res.running_machine_res.mem_slice
            disk += machine_res.running_machine_res.disk
            p += machine_res.running_machine_res.p
            m += machine_res.running_machine_res.m
            pm += machine_res.running_machine_res.pm
            
            return cpu_slice, mem_slice, disk, p, m, pm


        
