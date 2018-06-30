'''
Created on Jun 22, 2018

@author: Heng.Zhang
'''

from global_param import *
import csv
import numpy as np

from MachineRes import *
from AppRes import *
from MachineRunningInfo import *
    
import logging
import threading
    
from multiprocessing import cpu_count

class MachineResMgr(object):
    def __init__(self):
        
        log_file = r'%s\..\log\dispatch.txt' % runningPath
    
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')

        #  记录 machine 的运行信息， 包括 cpu 使用量,  cpu 使用率 = cpu 使用量 / cpu 容量， d, p, m, pm, app list
        self.machine_runing_info_dict = {} 
        machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources_reverse.csv' % runningPath, 'r'))
#         machine_res_csv = csv.reader(open(r'%s\..\input\part_normal_machine.csv' % runningPath, 'r'))
        for each_machine in machine_res_csv:
            machine_id = each_machine[0]
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 
            
        self.app_res_dict = {}
        app_res_csv = csv.reader(open(r'%s\..\input\app_resources.csv' % runningPath, 'r'))
        for each_app in app_res_csv:
            app_id = each_app[0]
            self.app_res_dict[app_id] = AppRes(each_app)

        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
        for each_inst in inst_app_csv:
            inst_id = each_inst[0]
            self.inst_app_dict[inst_id] = (each_inst[1], each_inst[2]) # app id, machine id

        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
        for each_cons in app_cons_csv:
            app_id_a = each_cons[0]
            app_id_b = each_cons[1]
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

#             if (app_id_b not in self.app_constraint_dict):
#                 self.app_constraint_dict[app_id_b] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.output_file = open(r'%s\..\output\submit.csv' % runningPath, 'w')
        
        self.sort_machine_by_cpu_per()
        
        self.machine_lock = threading.Lock()
        self.candidate_lock = threading.Lock()
        
        self.candidate_inst_list = {}
        
        self.machine_idx = 0

        return
 
    # 分发 inst_id 到  machine_id, 将分发好的信息写入文件
    # 当现有的 machin 资源无法分发某个 inst 时，就会将某些 inst 从某个 machine 中迁出， 然后将 inst 分发到该机器
    # 迁出的 inst 需要重新分发到其他机器，此时会递归调用 dispatch_cpp, skipped_machine_id 就是迁出 inst 的机器，
    # 重新分发迁出的 inst 时应该跳过该机器
    def dispatch_inst(self, inst_id, skipped_machine_id):
        app_res = self.app_res_dict[self.inst_app_dict[inst_id][0]]

        if (skipped_machine_id is not None and len(skipped_machine_id) == len(self.machine_runing_info_dict)):
            cpu_slice, mem_slice, disk, p, m, pm = MachineRes.sum_machine_remaining_res(self.sorted_machine_res)
            if (np.all(cpu_slice > app_res.cpu_slice) and np.all(mem_slice > app_res.mem_slice) and 
                disk >= app_res.disk and p >= app_res.p and m >= app_res.m and pm >= app_res.pm):
                logging.info('%s dispatch failed for instance %s, all machine are skipped, but remaining resource is enough '
                             'cpu %s, mem %s, disk %d, p %d, m %d, pm %d' % \
                             (getCurrentTime(), inst_id, cpu_slice, mem_slice, disk, p, m, pm))
            return False

        # 排好序的 sorted_machine_res 中的 machine_res 是个 tuple， 不可修改，根据 machine_id 到  machine_runing_info_dict 
        # 中去更新
        for machine_info_tuple in self.sorted_machine_res:
            machine_id =  machine_info_tuple[0]
            if (skipped_machine_id is not None and machine_id in skipped_machine_id):
                continue

            machine_running_res = self.machine_runing_info_dict[machine_id]
            if (machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)): 
                self.sort_machine_by_cpu_per()
                self.output_file.write('%s,%s\n' % (inst_id, machine_id))
                return True

        # 没有能分配当前 inst 的机器，在每台机器上得到可迁出的 app list， 在最长的 app list 的机器上迁出
        print(getCurrentTime(), 'no machine could dispatch inst %s, finding some migrating insts' % (inst_id))
        logging.info('no machine could dispatch inst %s, finding some migrating insts' % (inst_id))

        migrating_machine_running_res, migrating_insts = self.search_immigrate_list(inst_id, skipped_machine_id)

        # 在 migrating_machine_running_res 上将 migrating_insts 迁出
        logging.info("To migrat %s/%s into %s, immigrating %s " %\
                     (inst_id, app_res.app_id, migrating_machine_running_res.machine_res.machine_id, migrating_insts))

        for each_inst in migrating_insts:
            migrating_machine_running_res.release_app(each_inst, self.app_res_dict[self.inst_app_dict[each_inst][0]])

        # 迁出一些 inst 后，当前机器可以分发 inst
        if (migrating_machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
            self.sort_machine_by_cpu_per()
        else:
            print(getCurrentTime(), 'ERROR, dispatch instance %s failed after migrating %s on machine %s' % \
                  (inst_id, migrating_insts, migrating_machine_running_res.machine_res.machine_id))
            return False
        
        # 递归调用 dispatch_app, 将迁出的 inst 分发到其他的机器上， 但是要跳过迁出的机器
        if (skipped_machine_id is None):
            skipped_machine_id = [migrating_machine_running_res.machine_res.machine_id]

        for each_inst in migrating_insts:
            if (not self.dispatch_inst(each_inst, skipped_machine_id + [migrating_machine_running_res.machine_res.machine_id])):
                return False

        return True

    # 按照 get_cpu_percentage 从低到高排序, 优先分配使用率低的机器
    def sort_machine_by_cpu_per(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_cpu_percentage())

    def search_machine_immigrate_list(self, inst_id, skipped_machine_id):
        while (True):
            machine_idx = self.get_machine_index()
            if (machine_idx == -1):
                return

            if (machine_idx % 100 == 0):
                print(getCurrentTime(), '%d machine handled' % machine_idx)

            machine_id = self.sorted_machine_res[machine_idx][0]        
            machine_running_res = self.machine_runing_info_dict[machine_id]
            app_res = self.app_res_dict[self.inst_app_dict[inst_id][0]]
    
            if ((skipped_machine_id is not None and machine_id in skipped_machine_id) or 
                # 在当前机器能够容纳 inst 时， 才需要查找迁出列表
                not machine_running_res.meet_inst_res_require(app_res)):
                return
    
            # 在每台机器上得到可迁出的 app list
            tmp = machine_running_res.cost_of_immigrate_app(inst_id, self.inst_app_dict, self.app_res_dict, self.app_constraint_dict)
            if (len(tmp) == 0):
                return
    
            self.add_to_candidate_list(machine_id, tmp)
        return

    def add_to_candidate_list(self, machine_id, inst_list):
        self.candidate_lock.acquire()
        self.candidate_inst_list[machine_id] = inst_list
        self.candidate_lock.release()

    def get_machine_index(self):
        self.machine_lock.acquire()
        if (self.machine_idx < 6000):
            tmp = self.machine_idx
        else:
            tmp = -1
        self.machine_idx += 1
        self.machine_lock.release()
        return tmp

    @staticmethod
    def search_immigrate_list_func(machine_res_mgr, inst_id, skipped_machine_id):
        machine_res_mgr.search_machine_immigrate_list(inst_id, skipped_machine_id)
        return
        
    def search_immigrate_list(self, inst_id, skipped_machine_id):
        self.machine_idx = 0
        self.candidate_inst_list.clear()
        thread_cnt = cpu_count() * 8 + 2
        
        thread_list = []
        
        print(getCurrentTime(), 'starting %d threads to search' % thread_cnt)

        for i in range(thread_cnt):
            t =  threading.Thread(target=MachineResMgr.search_immigrate_list_func, args=(self, inst_id, skipped_machine_id, ))
            t.start()
            thread_list.append(t)

        for t in thread_list:
            t.join()
            
        migrating_machine_running_res = None
        migrating_insts = []
        min_var = 1e9
        
        for machine_id, each_list in self.candidate_inst_list.items():
            var_mean_of_list = AppRes.get_var_mean_of_apps(each_list, self.inst_app_dict, self.app_res_dict)
            # 找到迁出资源的方差的均值最小的一个作为最终的迁出 app list, 从 migrating_machine_running_res 迁出
            if (var_mean_of_list < min_var):
                migrating_insts = each_list
                migrating_machine_running_res = self.machine_runing_info_dict[machine_id]
                min_var = var_mean_of_list
        
        return migrating_machine_running_res, migrating_insts
        
        
        
        
        