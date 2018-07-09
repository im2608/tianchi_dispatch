
from global_param import *
import numpy as np

class AppRes(object):
    def __init__(self, each_app):
        if (each_app is not None):
            self.app_id = int(each_app[0])
            self.cpu_slice = np.array(list(map(float, each_app[1].split('|'))))
            self.mem_slice = np.array(list(map(float, each_app[2].split('|'))))
            self.disk = int(each_app[3])
            self.p = int(each_app[4])
            self.m = int(each_app[5])
            self.pm = int(each_app[6])
        else:
            self.app_id = 0
            self.cpu_slice = np.array(np.zeros(SLICE_CNT))
            self.mem_slice = np.array(np.zeros(SLICE_CNT))
            self.disk = 0
            self.p = 0
            self.m = 0
            self.pm = 0
        
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
    def sum_app_res_by_inst(inst_list, inst_app_dict, app_res_dict):
        tmp_app_res = AppRes(None)
        
        for each_inst in inst_list:
            app_res = app_res_dict[inst_app_dict[each_inst][0]]
            tmp_app_res.cpu_slice += app_res.cpu_slice
            tmp_app_res.mem_slice += app_res.mem_slice
            tmp_app_res.disk += app_res.disk
            tmp_app_res.p += app_res.p
            tmp_app_res.m += app_res.m
            tmp_app_res.pm += app_res.pm
         
        return tmp_app_res
    
    @staticmethod
    def sum_app_res_by_list(app_res_list):
        tmp_app_res = AppRes(None)
        
        for app_res in app_res_list:
            tmp_app_res.cpu_slice += app_res.cpu_slice
            tmp_app_res.mem_slice += app_res.mem_slice
            tmp_app_res.disk += app_res.disk
            tmp_app_res.p += app_res.p
            tmp_app_res.m += app_res.m
            tmp_app_res.pm += app_res.pm
         
        return tmp_app_res        
    
    @staticmethod
    def get_var_mean_of_apps(inst_list, inst_app_dict, app_res_dict):
        sum_var = 0
        for each_inst in inst_list:
            sum_var += app_res_dict[inst_app_dict[each_inst][0]].res_var

        return sum_var / len(inst_list)
    
    @staticmethod
    # 得到迁出的 app list 在 machine 上的分数
    def get_socre_of_apps(inst_list, inst_app_dict, app_res_dict, machine_cpu):
        cpu_slice_per = np.array(np.zeros(SLICE_CNT))
        for each_inst in inst_list:
            cpu_slice_per += app_res_dict[inst_app_dict[each_inst][0]].cpu_slice
            
        cpu_slice_per /= machine_cpu
        
        return score_of_cpu_percent_slice(cpu_slice_per)
