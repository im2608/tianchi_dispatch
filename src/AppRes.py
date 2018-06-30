
from global_param import *
import numpy as np

class AppRes(object):
    def __init__(self, each_app):
        self.app_id = each_app[0]
        self.cpu_slice = np.array(list(map(float, each_app[1].split('|'))))
        self.mem_slice = np.array(list(map(float, each_app[2].split('|'))))
        self.disk = int(each_app[3])
        self.p = int(each_app[4])
        self.m = int(each_app[5])
        self.pm = int(each_app[6])
        
        self.res_var = np.var([self.disk / MAX_DISK, self.p / MAX_P, self.m / MAX_M, self.pm / MAX_PM, 
                               np.var(self.cpu_slice / MAX_CPU), np.var(self.mem_slice / MAX_CPU)])
        return
    
    def to_string(self):
        return '%s, d %d, p %d, m %d, pm %d' % \
            (self.app_id, self.disk, self.p, self.m, self.pm)
            
    def to_full_string(self):
        return '%s, c %s, m %s, d %d, p %d, m %d, pm %d' % \
            (self.app_id, self.cpu_slice, self.mem_slice, self.disk, self.p, self.m, self.pm)
    
    @staticmethod
    def sum_app_res(inst_list, inst_app_dict, app_res_dict):
        cpu_slice = np.array(np.zeros(SLICE_CNT))
        mem_slice = np.array(np.zeros(SLICE_CNT))
        disk = 0
        p = 0
        m = 0
        pm = 0
        
        for each_inst in inst_list:
            app_res = app_res_dict[inst_app_dict[each_inst][0]]
            cpu_slice += app_res.cpu_slice
            mem_slice += app_res.mem_slice
            disk += app_res.disk
            p += app_res.p
            m += app_res.m
            pm += app_res.pm
         
        return cpu_slice, mem_slice, disk, p, m, pm
    
    @staticmethod
    def get_var_mean_of_apps(inst_list, inst_app_dict, app_res_dict):
        sum_var = 0
        for each_inst in inst_list:
            sum_var += app_res_dict[inst_app_dict[each_inst][0]].res_var

        return sum_var / len(inst_list)
