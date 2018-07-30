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
        mem = float(each_machine[2]) #  mem
        disk = float(each_machine[3]) # 剩余 disk
        p = float(each_machine[4]) # 剩余 p
        m = float(each_machine[5]) # 剩余 m
        pm = float(each_machine[6]) # 剩余 pm

        cpu_slice = np.array(np.zeros(SLICE_CNT) + self.cpu)  # 剩余 cpu
        mem_slice  = np.array(np.zeros(SLICE_CNT) + mem) # 剩余 mem 
        self.cpu_percentage = 0 # cpu 使用率 = cpu slice max 使用量 / cpu 容量
        
        self.res_vector = np.hstack((cpu_slice, mem_slice, disk, p, m, pm))
        
        self.machine_score = 0
    
        return
    
    def get_cpu_slice(self):
        return self.res_vector[:98]

       
    def update_machine_res(self, app_res, ratio):
#         self.mem_slice += ratio * app_res.mem_slice
#         self.cpu_slice += ratio * app_res.cpu_slice
# 
#         # slice 由于误差可能不会为0， 这里凡是 < 0.001 的slice 都设置成0
#         self.cpu_slice = np.where(np.less(self.cpu_slice, 0.001), 0, self.cpu_slice)
#         self.mem_slice = np.where(np.less(self.mem_slice, 0.001), 0, self.mem_slice)
# 
#         self.disk += ratio * app_res.disk
#         self.p += ratio * app_res.p
#         self.m += ratio * app_res.m
#         self.pm += ratio * app_res.pm
#         
#         self.res_vector = np.hstack((self.cpu_slice, self.mem_slice, self.disk, self.p, self.m, self.pm))
#         
#         self.cpu_percentage = self.cpu_slice.max() / self.cpu
        
        self.res_vector += ratio * app_res.res_vector
        
        self.machine_score = score_of_cpu_percent_slice((self.cpu - self.res_vector[:98]) / self.cpu)
        
    # 机器资源是否能够容纳 inst
    def meet_inst_res_require(self, app_res):
        return np.all(self.res_vector >= app_res.res_vector)
#         return (np.all(self.cpu_slice >= app_res.cpu_slice) and  
#                 np.all(self.mem_slice >= app_res.mem_slice) and
#                 self.disk >= app_res.disk and
#                 self.p >= app_res.p and
#                 self.m >= app_res.m  and
#                 self.pm >= app_res.pm)
        
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


        
