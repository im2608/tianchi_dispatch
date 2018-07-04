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
import datetime

from multiprocessing import cpu_count, Process, Pipe

class MachineResMgr(object):
    def __init__(self):
        
        log_file = r'%s\..\log\dispatch_score.txt' % runningPath
    
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')

        #  记录 machine 的运行信息， 包括 cpu 使用量,  cpu 使用率 = cpu 使用量 / cpu 容量， d, p, m, pm, app list
        self.machine_runing_info_dict = {} 
#         machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources_reverse.csv' % runningPath, 'r'))
        machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources.csv' % runningPath, 'r'))
#         machine_res_csv = csv.reader(open(r'%s\..\input\part_normal_machine.csv' % runningPath, 'r'))
        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 
            
        self.app_res_dict = {}
        app_res_csv = csv.reader(open(r'%s\..\input\app_resources.csv' % runningPath, 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0])
            self.app_res_dict[app_id] = AppRes(each_app)

        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            self.inst_app_dict[inst_id] = int(each_inst[1]) # app id

        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

#             if (app_id_b not in self.app_constraint_dict):
#                 self.app_constraint_dict[app_id_b] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.output_file = open(r'%s\..\output\submit_%s.csv' % (runningPath, datetime.datetime.now().strftime('%Y%m%d_%H%M%S')), 'w')
        
        self.sort_machine()
        
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
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

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
                self.sort_machine()
                self.output_file.write('%s,%s\n' % (inst_id, machine_id))
                return True            

        # 没有能分配当前 inst 的机器，在每台机器上得到可迁出的 app list， 在最长的 app list 的机器上迁出
        print(getCurrentTime(), 'no machine could dispatch inst %s, finding some migrating insts' % (inst_id))
        logging.info('no machine could dispatch inst %s, finding some migrating insts' % (inst_id))

        migrating_machine_running_res = None
        migrating_insts = []
        max_score = 0
        start_time = time.time()
        for idx, machine_info_tuple in enumerate(self.sorted_machine_res):
            if (idx % 100 == 0):
                end_time = time.time()
                print(getCurrentTime(), '%d machine through..., used %d seconds\r' % (idx, end_time - start_time), end='')
                start_time = time.time()
            
            machine_id =  machine_info_tuple[0]
            machine_running_res = self.machine_runing_info_dict[machine_id]
            if ((skipped_machine_id is not None and machine_id in skipped_machine_id) or 
                # 在当前机器能够容纳 inst 时， 才需要查找迁出列表
                not machine_running_res.meet_inst_res_require(app_res)):
                continue

            # 在每台机器上得到可迁出的 app list, 以及迁出的 app list 在该机器上的得分
            tmp, score = machine_running_res.cost_of_immigrate_app(inst_id, self.inst_app_dict, self.app_res_dict, self.app_constraint_dict)
            if (len(tmp) == 0):
                continue

            # 找到迁出资源的得分最高的作为的迁出 app list, 从 migrating_machine_running_res 迁出
            if (max_score < score):
                migrating_insts = tmp
                migrating_machine_running_res = self.machine_runing_info_dict[machine_id]
                max_score = score

        # 在 migrating_machine_running_res 上将 migrating_insts 迁出
        logging.info("To migrat %s/%s into %s, immigrating %s " %\
                     (inst_id, app_res.app_id, migrating_machine_running_res.machine_res.machine_id, migrating_insts))

        for each_inst in migrating_insts:
            migrating_machine_running_res.release_app(each_inst, self.app_res_dict[self.inst_app_dict[each_inst]])

        # 迁出一些 inst 后，当前机器可以分发 inst
        if (migrating_machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
            self.sort_machine()
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
    
    def sort_machine(self):
        self.sort_machine_by_score()

    # 按照 get_cpu_percentage 从低到高排序, 优先分配使用率低的机器
    def sort_machine_by_cpu_per(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_cpu_percentage())

    # 按照 机器的得分 从低到高排序, 优先分配得分低的机器
    def sort_machine_by_score(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_machine_score())

    def search_machine_immigrate_list(self, inst_id, skipped_machine_id, machine_start, machine_end, childEnd):
        migrating_machine_id = None
        migrating_insts = []
        min_var = 1e9
        
        for machine_idx in range(machine_start, machine_end):
            if (machine_idx % 100 == 0):
                print(getCurrentTime(), '%d machine handled' % machine_idx)

            machine_id = self.sorted_machine_res[machine_idx][0]        
            machine_running_res = self.machine_runing_info_dict[machine_id]
            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
    
            if ((skipped_machine_id is not None and machine_id in skipped_machine_id) or 
                # 在当前机器能够容纳 inst 时， 才需要查找迁出列表
                not machine_running_res.meet_inst_res_require(app_res)):
                return
    
            # 在每台机器上得到可迁出的 app list
            tmp = machine_running_res.cost_of_immigrate_app(inst_id, self.inst_app_dict, self.app_res_dict, self.app_constraint_dict)
            if (len(tmp) == 0):
                return
    
            var_mean_of_list = AppRes.get_var_mean_of_apps(tmp, self.inst_app_dict, self.app_res_dict)
            # 找到迁出资源的方差的均值最小的一个作为最终的迁出 app list, 从 migrating_machine_running_res 迁出
            if (var_mean_of_list < min_var):
                migrating_insts = tmp
                migrating_machine_id = self.machine_runing_info_dict[machine_id]
                min_var = var_mean_of_list  

        childEnd.send({'machine_id':migrating_machine_id, 'inst_list':migrating_insts})        
        childEnd.close()  
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
    def search_immigrate_list_func(machine_res_mgr, inst_id, skipped_machine_id, machine_start, machine_end, childEnd):
        machine_res_mgr.search_machine_immigrate_list(inst_id, skipped_machine_id, machine_start, machine_end, childEnd)
        return
        
    def search_immigrate_list(self, inst_id, skipped_machine_id):
        self.machine_idx = 0
        self.candidate_inst_list.clear()
        subprocess_cnt = cpu_count()      
        subprocess_cnt = 1
        
        subprocess_list = []
        machines_for_each_sub = len(self.sorted_machine_res) / subprocess_cnt 
        
        print(getCurrentTime(), 'starting %d sub process to search' % subprocess_cnt)

        for i in range(subprocess_cnt):
            machine_start = i * machines_for_each_sub
            machine_end = machine_start + machines_for_each_sub
            machine_end = machine_start + 3

            (parentEnd, childEnd) = Pipe()
            p =  Process(target=MachineResMgr.search_immigrate_list_func, \
#                          args=(self, inst_id, skipped_machine_id, machine_start, machine_end, childEnd, ))
                         args=(1, ))
            p.start()
            subprocess_list.append((p, parentEnd))

        for (p, parentEnd) in subprocess_list:
            s = parentEnd.recv()
            parentEnd.close()
            p.join()
            
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
        
        
        
        
        