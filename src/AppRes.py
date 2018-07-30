
from global_param import *
import numpy as np

class AppRes(object):
    def __init__(self, each_app):
        if (each_app is not None):
            self.app_id = int(each_app[0])
            cpu_slice = np.array(list(map(float, each_app[1].split('|'))))
            mem_slice = np.array(list(map(float, each_app[2].split('|'))))
            disk = int(float(each_app[3]))
            p = int(each_app[4])
            m = int(each_app[5])
            pm = int(each_app[6])
        else:
            self.app_id = 0
            cpu_slice = np.array(np.zeros(SLICE_CNT))
            mem_slice = np.array(np.zeros(SLICE_CNT))
            disk = 0
            p = 0
            m = 0
            pm = 0

        self.res_vector = np.hstack((cpu_slice, mem_slice, disk, p, m, pm))

        return
    
    def get_cpu_slice(self):
        return self.res_vector[:98]    
    
    def get_mem_slice(self):
        return self.res_vector[98:196]
    
    def get_disk(self):
        return self.res_vector[196]
    
    @staticmethod
    def sum_app_res_by_inst(inst_list, inst_app_dict, app_res_dict):
        tmp_app_res = AppRes(None)

        for each_inst in inst_list:
            app_res = app_res_dict[inst_app_dict[each_inst]]
            tmp_app_res.res_vector += app_res.res_vector
         
        return tmp_app_res
    
    @staticmethod
    def sum_app_res_by_list(app_res_list):
        tmp_app_res = AppRes(None)
        
        for app_res in app_res_list:
            tmp_app_res.res_vector += app_res.res_vector
         
        return tmp_app_res        
    
    
