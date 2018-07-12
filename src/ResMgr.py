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
        print(getCurrentTime(), 'loading machine_resources.csv...')
        self.machine_runing_info_dict = {} 
#         machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources_reverse.csv' % runningPath, 'r'))
        machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources.csv' % runningPath, 'r'))
        
        self.used_machine_dict = {}

        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 

        print(getCurrentTime(), 'loading app_resources.csv...')
        self.app_res_dict = [0 for x in range(APP_CNT + 1)]
        app_res_csv = csv.reader(open(r'%s\..\input\app_resources.csv' % runningPath, 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0])
            self.app_res_dict[app_id] = AppRes(each_app)

        self.migrating_list = []
        print(getCurrentTime(), 'loading instance_deploy.csv...')
        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            app_id = int(each_inst[1])
            self.inst_app_dict[inst_id]  = app_id
            if (len(each_inst[2]) > 0):
                machine_id = int(each_inst[2])

                # 将 machine 从未使用列表中移动到使用列表中
                if (machine_id not in self.used_machine_dict):
                    self.used_machine_dict[machine_id] = self.machine_runing_info_dict.pop(machine_id)

                self.used_machine_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)
                    

        print(getCurrentTime(), 'loading app_interference.csv...')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.sort_machine()
        
        self.init_deploying()
        
        self.machine_lock = threading.Lock()
        self.candidate_lock = threading.Lock()
        
        self.candidate_inst_list = {}
        
        self.machine_idx = 0
        
        return
    
    # 初始化状态，将不满足约束的 inst 迁移出去
    def init_deploying(self):
        print(getCurrentTime(), 'init_deploying...')
        index = 0
        for machine_id, violate_machine_running_res in self.sorted_machine_res:
            if (index  % 100 == 0):
                print(getCurrentTime(), '%d machine initialized\r' % index, end='')
            index += 1
            
            if (len(violate_machine_running_res.running_inst_list) == 1):
                continue

            machine_id = violate_machine_running_res.machine_res.machine_id

            # 查找机器上的 running inst list 是否有违反约束的 inst
            violate_inst_id = violate_machine_running_res.any_self_violate_constriant(self.inst_app_dict, 
                                                                                      self.app_res_dict, 
                                                                                      self.app_constraint_dict)
            while (violate_inst_id is not None):
                # 当前 machine_id 上有违反约束的 inst， 将其迁移到迁入分数最小的机器上
                min_score = 1e9
                violate_app_res = self.app_res_dict[self.inst_app_dict[violate_inst_id]]

                for each_machine_id, immigrate_machine_running_res in self.sorted_machine_res:      
                    if (each_machine_id == machine_id):
                        continue

                    if (immigrate_machine_running_res.can_dispatch(violate_app_res, self.app_constraint_dict)):
                        immigrating_score = immigrate_machine_running_res.immigrating_score(violate_app_res)
                        if (immigrating_score < min_score):
                            min_score = immigrating_score
                            immigrating_machine = each_machine_id

                            if (immigrating_score == 98):
                                break
                   
                if (min_score <= 196):
                    if (not self.used_machine_dict[immigrating_machine].dispatch_app(violate_inst_id, 
                                                                                     violate_app_res, 
                                                                                     self.app_constraint_dict)):
                        print_and_log("ERROR! Failed to migrate inst %d to machine %d" % (violate_inst_id, immigrating_machine))
                        exit(-1)
                else:
                    if (self.immigrate_to_empty_machine(violate_inst_id, violate_app_res)):                
                        violate_machine_running_res.release_app(violate_inst_id, violate_app_res)        
                        violate_inst_id = violate_machine_running_res.any_self_violate_constriant(self.inst_app_dict, 
                                                                                                  self.app_res_dict, 
                                                                                                  self.app_constraint_dict)
            if (len(violate_machine_running_res.running_inst_list) == 0):
                self.machine_runing_info_dict[machine_id] = self.used_machine_dict.pop(machine_id)
        return
    
    def immigrate_to_empty_machine(self, inst_id, app_res):
        for immigrating_machine, machine_running_res in self.machine_runing_info_dict.items():
            if (machine_running_res.meet_inst_res_require(app_res)):
                immigrating_score = machine_running_res.immigrating_score(app_res)
                if (machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                    self.sort_machine()
                    self.migrating_list.append('inst_%d,machine_%d' % (inst_id, immigrating_machine))
                    self.used_machine_dict[immigrating_machine] = self.machine_runing_info_dict.pop(immigrating_machine)         
                    return True
                else:
                    print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (inst_id, immigrating_machine))
                    exit(-1)
        return False
                    
    # 分发 inst_id 到  machine_id, 将分发好的信息写入文件
    # 当现有的 machin 资源无法分发某个 inst 时，就会将某些 inst 从某个 machine 中迁出， 然后将 inst 分发到该机器
    # 迁出的 inst 需要重新分发到其他机器，此时会递归调用 dispatch_cpp, skipped_machine_id 就是迁出 inst 的机器，
    # 重新分发迁出的 inst 时应该跳过该机器
    def dispatch_inst(self, inst_id, skipped_machine_id):
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

        if (skipped_machine_id is not None and len(skipped_machine_id) == len(self.machine_runing_info_dict)):
            cpu_slice, mem_slice, disk, p, m, pm = MachineRes.sum_machine_remaining_res(self.sorted_machine_res)
            if (np.all(cpu_slice >= app_res.cpu_slice) and 
                np.all(mem_slice >= app_res.mem_slice) and 
                disk >= app_res.disk and 
                p >= app_res.p and 
                m >= app_res.m and 
                pm >= app_res.pm):
                logging.info('%s dispatch failed for instance %s, all machine are skipped, but remaining resource is enough '
                             'cpu %s, mem %s, disk %d, p %d, m %d, pm %d' % \
                             (getCurrentTime(), inst_id, cpu_slice, mem_slice, disk, p, m, pm))
            return False

        # 排好序的 sorted_machine_res 中的 machine_res 是个 tuple， 不可修改，根据 machine_id 到  machine_runing_info_dict 
        # 中去更新
        # 在所有已经使用的可迁入的机器中，找到一个迁入分数最小的
        min_immigrating_score = 1e9
        for i, machine_info_tuple in enumerate(self.sorted_machine_res): # sorted_machine_res 是已经部署了 inst 的机器列表
            machine_id =  machine_info_tuple[0]
            if (skipped_machine_id is not None and machine_id in skipped_machine_id):
                continue

            machine_running_res = self.used_machine_dict[machine_id]
            if (machine_running_res.can_dispatch(app_res, self.app_constraint_dict)):
                immigrating_score = machine_running_res.immigrating_score(app_res)
                if (immigrating_score < min_immigrating_score):
                    min_immigrating_score = immigrating_score
                    immigrating_machine = machine_id
                    if (immigrating_score == 98):
                        break

        # 如果迁入分数 <= 196,  在已使用的机器上迁入
        if (min_immigrating_score <= 196):
            if (self.used_machine_dict[immigrating_machine].dispatch_app(inst_id, app_res, self.app_constraint_dict)): 
                self.sort_machine()       
                self.migrating_list.append('inst_%d,machine_%d' % (inst_id, immigrating_machine))         
                return True
            else:
                print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (inst_id, immigrating_machine))
                return False
        else: # 如果迁入分数 > 196, 则在一台空机器上迁入,
            if (self.immigrate_to_empty_machine(inst_id, app_res)):
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
    
    def sort_machine(self, reverse = True):
        self.sort_machine_by_score(reverse)

    # 按照 get_cpu_percentage 从低到高排序, 优先分配使用率低的机器
    def sort_machine_by_cpu_per(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict, key=lambda d:d[1].get_cpu_percentage())

    # 按照 机器的得分 从低到高排序, 优先分配得分低的机器
    def sort_machine_by_score(self, reverse):
        self.sorted_machine_res = sorted(self.used_machine_dict.items(), key=lambda d:d[1].get_machine_score(), reverse=reverse)

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

    def output_submition(self):
        filename = 'submit_%s.csv' % datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = open(r'%s\..\output\%s' % (runningPath, filename), 'w')
        print(getCurrentTime(), 'writing output file %s' % filename)

        for each_migrating in self.migrating_list:
            output_file.write('%s\n' % (each_migrating))

        output_file.close()

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
        
        
        
        
        