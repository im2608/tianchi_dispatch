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
#                 if (machine_id not in self.used_machine_dict):
#                     self.used_machine_dict[machine_id] = self.machine_runing_info_dict.pop(machine_id)
#                 self.used_machine_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)
                
                self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)

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
                violate_app_res = self.app_res_dict[self.inst_app_dict[violate_inst_id]]

                for each_machine_id, immigrate_machine_running_res in self.sorted_machine_res:      
                    if (each_machine_id == machine_id):
                        continue

                    if (immigrate_machine_running_res.dispatch_app(violate_inst_id, 
                                                                   violate_app_res, 
                                                                   self.app_constraint_dict)):
                        self.migrating_list.append('inst_%d,machine_%d' % (violate_inst_id, each_machine_id))
                        violate_machine_running_res.release_app(violate_inst_id, violate_app_res)
                        self.sort_machine()
                        break
                    
                violate_inst_id = violate_machine_running_res.any_self_violate_constriant(self.inst_app_dict, 
                                                                      self.app_res_dict, 
                                                                      self.app_constraint_dict)

#                 if (min_score <= 196):
#                     if (not self.used_machine_dict[immigrating_machine].dispatch_app(violate_inst_id, 
#                                                                                      violate_app_res, 
#                                                                                      self.app_constraint_dict)):
#                 else:
#                     if (self.immigrate_to_empty_machine(violate_inst_id, violate_app_res)):                
#                         violate_machine_running_res.release_app(violate_inst_id, violate_app_res)        
#                         violate_inst_id = violate_machine_running_res.any_self_violate_constriant(self.inst_app_dict, 
#                                                                                                   self.app_res_dict, 
#                                                                                                   self.app_constraint_dict)
#             if (len(violate_machine_running_res.running_inst_list) == 0):
#                 self.machine_runing_info_dict[machine_id] = self.used_machine_dict.pop(machine_id)
        
        cost = self.sum_scores_of_machine()
        
        next_cost = self.adj_dispatch_ex(196)
        while (next_cost < cost):
            print_and_log('After adj_dispatch_ex(), score %f -> %f' % (cost, next_cost))
            cost = next_cost
            next_cost = self.adj_dispatch_ex(100)
            
        next_cost = self.adj_dispatch()
        while (next_cost < cost):
            print_and_log('After adj_dispatch(), score %f -> %f' % (cost, next_cost))
            cost = next_cost
            next_cost = self.adj_dispatch()           
                    
        print(getCurrentTime(), 'init_deploying done...')
        return
    
    def adj_dispatch_ex(self, max_score):
        
        machine_idx = 0
        
        next_cost = self.sum_scores_of_machine()

        while (self.sorted_machine_res[machine_idx][1].get_machine_real_score() > max_score):
            machine_id = self.sorted_machine_res[machine_idx][0]
            heavest_load_machine = self.machine_runing_info_dict[machine_id]

            immigrating_machine_dict = {}
            sum_increased_score = 0 # 将  inst 迁入后增加的分数的总和

            for each_inst in heavest_load_machine.running_inst_list:
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
                min_delta_score = 1e9 # 将 inst 迁入后增加的分数， 找到增加分数最小的机器

                # 在所有的机器上找到迁入最小的分数
                for i in range(machine_idx + 1, len(self.sorted_machine_res)):
                    machine_id = self.sorted_machine_res[i][0]
    
                    heavy_load_machine = self.machine_runing_info_dict[machine_id]

                    # 多个 inst 可能迁移到同一台机器上, 所以判断是否能够迁入应该用多个 inst 一起来判断
                    if (machine_id in immigrating_machine_dict):
                        inst_list = immigrating_machine_dict[machine_id][0] # 已经决定要迁移到该机器上的 inst 
                    else:
                        inst_list = []

                    # 已经决定要迁移到该机器上的 inst + 将要迁入的 inst， 看是否满足迁入条件
                    if (heavy_load_machine.can_dispatch_ex(inst_list + [each_inst], self.inst_app_dict, self.app_res_dict, self.app_constraint_dict)):
                        # 将 inst 迁入后增加的分数
                        sum_app_res = AppRes.sum_app_res_by_inst(inst_list + [each_inst], self.inst_app_dict, self.app_res_dict)
                        increased_score = heavy_load_machine.immigrating_delta_score(sum_app_res)
                        if (increased_score < min_delta_score):
                            min_delta_score = increased_score
                            immigrating_machine = machine_id

                            # 迁入后没有增加分数， 最好的结果， 无需继续查找
                            if (increased_score == 0):
                                break

                if (min_delta_score < 1e9):
                    if (immigrating_machine in immigrating_machine_dict):
                        immigrating_machine_dict[immigrating_machine][0].append(each_inst)
                        immigrating_machine_dict[immigrating_machine][1] = min_delta_score
                    else:
                        immigrating_machine_dict[immigrating_machine] = [[each_inst], min_delta_score]
    
                    sum_increased_score += min_delta_score

                # 增加的分数 > 98, 不可行 , 不用再继续尝试
                if (sum_increased_score > heavest_load_machine.get_machine_real_score()):
                    break
            
            if (sum_increased_score > heavest_load_machine.get_machine_real_score() or len(immigrating_machine_dict) == 0):
                print_and_log('migrating %s, running len %d, increased score %f > (real score %f), continue...' % \
                              (heavest_load_machine.machine_res.machine_id, len(heavest_load_machine.running_inst_list), 
                               sum_increased_score, heavest_load_machine.get_machine_real_score()))
                machine_idx += 1
                continue

            # 迁入
            for immigrating_machine, (inst_list, score) in immigrating_machine_dict.items():
                for each_inst in inst_list:
                    app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
    
                    # 迁入
                    if (not self.machine_runing_info_dict[immigrating_machine].dispatch_app(each_inst, app_res, self.app_constraint_dict)):
                        print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (each_inst, immigrating_machine))
                        return
                    
                    self.migrating_list.append('inst_%d,machine_%d' % (each_inst, immigrating_machine))
            
                    heavest_load_machine.release_app(each_inst, app_res) # 迁出 inst

            self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
            
            # 迁移之后重新计算得分
            next_cost = self.sum_scores_of_machine()   

            print_and_log('migrating %s, increased score %f, next_cost %f' % \
                  (heavest_load_machine.machine_res.machine_id, sum_increased_score, next_cost))

        return next_cost
    
    
    def sum_scores_of_machine(self):
        scores = 0
        for machine_id, machine_running_res in self.sorted_machine_res:
            scores += machine_running_res.get_machine_real_score()
            
        return scores
        
    def adj_dispatch(self):
        machine_idx = 0
        
        non_migratable_machine = set()

        # 得分最高的机器
        while (self.sorted_machine_res[machine_idx][1].get_machine_real_score() > 98):
            heavest_load_machine = self.machine_runing_info_dict[self.sorted_machine_res[machine_idx][0]]
            if (heavest_load_machine.machine_res.machine_id in non_migratable_machine):
                machine_idx += 1
                continue

            # 将 inst 迁出后减少的分数
            decreased_score = 0

            # 尝试迁出全部的 inst 效果不好
            # 找到一个得分最高的 inst
            for each_inst in heavest_load_machine.running_inst_list:
                insts_score = heavest_load_machine.migrating_delta_score(self.app_res_dict[self.inst_app_dict[each_inst]])
                if (decreased_score < insts_score):
                    decreased_score = insts_score
                    migrate_inst = each_inst
    
            max_delta_score = 0 # 将 inst 迁出后减少的分数 - 将 inst 迁入后增加的分数, 找到最大的 max delta

            migrate_app_res = self.app_res_dict[self.inst_app_dict[migrate_inst]]
    
            # 在轻负载的机器上找到迁入分数最小的
            for i in  range(len(self.sorted_machine_res)-1, 0, -1):
                machine_id = self.sorted_machine_res[i][0]
                lightest_load_machine = self.machine_runing_info_dict[machine_id]            
    
                if (lightest_load_machine.can_dispatch(migrate_app_res, self.app_constraint_dict)):
                    # 将 inst 迁入后增加的分数
                    increased_score = lightest_load_machine.immigrating_delta_score(migrate_app_res)
                    if (max_delta_score < decreased_score - increased_score):
                        max_delta_score = decreased_score - increased_score
                        immmigrating_machine = machine_id
                        # 迁入后没有增加分数， 最好的结果， 无需继续查找
                        if (increased_score == 0):
                            break
    
            if (max_delta_score == 0):
                non_migratable_machine.add(heavest_load_machine.machine_res.machine_id)
                print_and_log("%d is not migratable, %d skipped ..." % (heavest_load_machine.machine_res.machine_id, machine_idx) )
                continue
    
            # 迁入
            if (not self.machine_runing_info_dict[immmigrating_machine].dispatch_app(migrate_inst, migrate_app_res, self.app_constraint_dict)):
                print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (migrate_inst, immmigrating_machine))
                return
            
            self.migrating_list.append('inst_%d,machine_%d' % (migrate_inst, immmigrating_machine))
    
            heavest_load_machine.release_app(migrate_inst, migrate_app_res) # 迁出 inst
    
            self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
            
            next_cost = self.sum_scores_of_machine()

            print_and_log('%d -> %d, max_delta_score %f, next_cost %f' % \
                  (heavest_load_machine.machine_res.machine_id, immmigrating_machine, max_delta_score, next_cost))

        return  self.sum_scores_of_machine()
        
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

        for i, machine_info_tuple in enumerate(self.sorted_machine_res): # sorted_machine_res 是已经部署了 inst 的机器列表
            machine_id =  machine_info_tuple[0]
            if (skipped_machine_id is not None and machine_id in skipped_machine_id):
                continue

            machine_running_res = machine_info_tuple[1]
            if (machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                self.migrating_list.append('inst_%d,machine_%d' % (inst_id, machine_id))
                self.sort_machine()
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
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_machine_score(), reverse=reverse)


    def output_submition(self):
        filename = 'submit_%s.csv' % datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = open(r'%s\..\output\%s' % (runningPath, filename), 'w')
        print(getCurrentTime(), 'writing output file %s' % filename)

        for each_migrating in self.migrating_list:
            output_file.write('%s\n' % (each_migrating))

        output_file.close()
        
    def dispatch_app_ant(self, inst_id):
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        return

        
        
        