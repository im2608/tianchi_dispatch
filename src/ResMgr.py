#coding=utf-8
'''
Created on Jun 22, 2018

@author: Heng.Zhang
'''


import csv
import numpy as np

from MachineRes import *
from AppRes import *
from MachineRunningInfo import *
    
import logging
import threading
import datetime
import copy
import os
from multiprocessing import cpu_count, Process, Pipe
from nltk.ccg.lexicon import APP_RE

class MachineResMgr(object):
    def __init__(self):
        
        log_file = r'%s/../log/dispatch_score_%s.txt' % (runningPath, data_set)
        
        self.print_all_scores = False

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')

        #  记录 machine 的运行信息， 包括 cpu 使用量,  cpu 使用率 = cpu 使用量 / cpu 容量， d, p, m, pm, app list
        print(getCurrentTime(), 'loading machine_resources.csv...')
        self.machine_runing_info_dict = {} 
#         machine_res_csv = csv.reader(open(r'%s/../input/%s/machine_resources_reverse.csv' % (runningPath, data_set), 'r'))
        machine_res_csv = csv.reader(open(r'%s/../input/%s/machine_resources.csv' % (runningPath, data_set), 'r'))
        
        self.used_machine_dict = {}

        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 

        print(getCurrentTime(), 'loading app_resources.csv...')
        self.app_res_dict = [0 for x in range(APP_CNT + 1)]
        app_res_csv = csv.reader(open(r'%s/../input/%s/app_resources.csv' % (runningPath, data_set), 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0])
            self.app_res_dict[app_id] = AppRes(each_app)

        print(getCurrentTime(), 'loading app_interference.csv...')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s/../input/%s/app_interference.csv' % (runningPath, data_set), 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.migrating_list = []
    
        print(getCurrentTime(), 'loading instance_deploy.csv...')
        inited_filename = r'%s/../input/%s/initialized_deploy.csv' % (runningPath, data_set)
        b_created_init_filename = os.path.exists(inited_filename)
        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s/../input/%s/instance_deploy.csv' % (runningPath, data_set), 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            app_id = int(each_inst[1])
            self.inst_app_dict[inst_id] = app_id
            if (len(each_inst[2]) > 0):
                machine_id = int(each_inst[2])
                self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)

#         if (b_created_init_filename):
#             print(getCurrentTime(), 'loading initialized_deploy.csv...')
#             inst_disp_csv = csv.reader(open(r'%s\..\input\initialized_deploy.csv' % runningPath, 'r'))
#             for each_inst in inst_disp_csv:
#                 inst_id = int(each_inst[0].split('_')[1])
#                 machine_id = int(each_inst[1].split('_')[1])
#                 self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[self.inst_app_dict[inst_id]], DISPATCH_RATIO)
#                 self.migrating_list.append(each_app)
        self.sort_machine()
        self.init_deploying()
        return
    
    def dispatch_inst_internal(self, inst_id, skipped_machins):
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

        does_prefer = does_prefer_small_machine(app_res) 
        # 优先分发到小机器
        if (does_prefer):
            b_migrated = self.dispatch_inst_with_min_score(inst_id, app_res, 1, 3001, skipped_machins)
            if (not b_migrated): # 小机器分发失败，分发到大机器
                b_migrated = self.dispatch_inst_with_min_score(inst_id, app_res, 3001, 6001, skipped_machins)
        else: # 优先分发到大机器
            b_migrated = self.dispatch_inst_with_min_score(inst_id, app_res, 3001, 6001, skipped_machins)
            if (not b_migrated):
                b_migrated = self.dispatch_inst_with_min_score(inst_id, app_res, 1, 3001, skipped_machins)

        return b_migrated

    # 将 inst 分发到一群机器中的某台上并且迁入分数最小
    def dispatch_inst_with_min_score(self, inst_id, app_res, machine_start_idx, machine_end_idx, skipped_machines):
        min_delta_score = 1e9
        for machine_idx, immigrate_machine_res in self.sorted_machine_res:
            if ((skipped_machines is not None and machine_idx in skipped_machines) or 
                machine_idx not in range(machine_start_idx, machine_end_idx)):
                continue

            if (immigrate_machine_res.can_dispatch(app_res, self.app_constraint_dict)):
                delta_score = immigrate_machine_res.immigrating_delta_score(app_res)
                if (delta_score < min_delta_score):
                    min_delta_score = delta_score
                    immigrate_machine_id = machine_idx

        if (min_delta_score < 1e9):
            if (not self.machine_runing_info_dict[immigrate_machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                print_and_log("ERROR! dispatch_inst_with_min_score() Failed to immigrate inst %d to machine %d" % (inst_id, immigrate_machine_id))
                exit(-1)

            self.migrating_list.append('inst_%d,machine_%d' % (inst_id, immigrate_machine_id))
            return True

        return False
    
    def adj_dispatch_ex(self):
        print_and_log('adj_dispatch_ex with max score')

        machine_start_idx = 0
        
        next_cost = self.sum_scores_of_machine()

        while (machine_start_idx < MACHINE_CNT):
            if (machine_start_idx % 100 == 0):
                print(getCurrentTime(), 'adj_dispatch_ex handled %d machines\r' % machine_start_idx, end='')

            machine_id = self.sorted_machine_res[machine_start_idx][0]

            heavest_load_machine = self.machine_runing_info_dict[machine_id]
            
            if (len(heavest_load_machine.running_inst_list) == 0):
                machine_start_idx += 1
                continue

            inst_id = heavest_load_machine.running_inst_list[0]

            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            # 生成迁移方案的第一步， 以及迁入后增加的分数
            dp_immigrating_solution_list = self.get_immigratable_machine_ex(inst_id, machine_id)
            print(getCurrentTime(), 'machine %d, 1st / %d step solution is %d' % (machine_id, len(heavest_load_machine.running_inst_list),
                                                                                   len(dp_immigrating_solution_list)))
            
            if (len(dp_immigrating_solution_list) == 0):
                machine_start_idx += 1
                continue
            # 生成第 2 -> N 步的迁移方案
            for inst_idx in range(1, len(heavest_load_machine.running_inst_list)):
                print(getCurrentTime(), 'searching machine %d %d/%d\r' % (machine_id, inst_idx,  len(heavest_load_machine.running_inst_list)), end='')
                each_inst = heavest_load_machine.running_inst_list[inst_idx]
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]

                one_step_solution = self.get_immigratable_machine_ex(each_inst, machine_id)
                dp_immigrating_solution_list = self.merge_migration_solution(dp_immigrating_solution_list, 
                                                                             one_step_solution,
                                                                             heavest_load_machine.get_machine_real_score())
                print(getCurrentTime(), 'machine %d, %d step solution is %d' % (machine_id, inst_idx + 1, len(dp_immigrating_solution_list)))
                if (len(dp_immigrating_solution_list) == 0):
                    machine_start_idx += 1
                    break

            # 在所有的迁移方案中找到迁入分数最小的
            min_solution_score = 1e9
            for idx, each_solution in enumerate(dp_immigrating_solution_list):
                if (each_solution[1] < min_solution_score):
                    min_solution_score = each_solution[1]
                    min_solution_idx = idx

            # 迁入所增加的分数至少要减少 1 分 , 否则不用再继续尝试
            if (heavest_load_machine.get_machine_real_score() - min_solution_score <= 1):
                print_and_log('migrating %s, running len %d, increased score %f > (real score %f), continue...' % \
                              (heavest_load_machine.machine_res.machine_id, len(heavest_load_machine.running_inst_list), 
                               min_solution_score, heavest_load_machine.get_machine_real_score()))

                machine_start_idx += 1
                continue

            print_and_log('migrating solution for machine : %d, real score %f, increased %f, delta score %f, %s' % \
                          (machine_id, heavest_load_machine.get_machine_real_score(), min_solution_score,
                           heavest_load_machine.get_machine_real_score() - min_solution_score, 
                           dp_immigrating_solution_list[min_solution_idx]))
            # 迁入
            for immigrating_machine, inst_list in dp_immigrating_solution_list[min_solution_idx][0].items():
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
            # 迁移之后重新计算得分, 仍然从可迁移的且得分最高的机器开始
            next_cost = self.sum_scores_of_machine()   

        print_and_log('leaving adj_dispatch_ex with next cost %f' % next_cost)
        return next_cost

    # 初始化状态，将不满足约束的 inst 迁移出去
    def init_deploying(self):
        print(getCurrentTime(), 'init_deploying...')
        index = 0
        for violate_machine_id, violate_machine_res in self.sorted_machine_res:
            if (index  % 100 == 0):
                print(getCurrentTime(), '%d violating machine initialized\r' % index, end='')
            index += 1

            if (len(violate_machine_res.running_inst_list) <= 1):
                continue

            # 查找机器上的 running inst list 是否有违反约束的 inst
            violate_inst_id = violate_machine_res.any_self_violate_constriant(self.inst_app_dict, 
                                                                              self.app_res_dict, 
                                                                              self.app_constraint_dict)
            # 当前 machine_id 上有违反约束的 inst， 将其迁移到迁入分数最小的机器上
            while (violate_inst_id is not None):
                if (not self.dispatch_inst_internal(violate_inst_id, [violate_machine_id])):
                    print_and_log("ERROR! init_deploying() Failed to immigrate inst %d to machine %d" % (violate_inst_id))
                    exit(-1)

                violate_app_res = self.app_res_dict[self.inst_app_dict[violate_inst_id]]
                violate_machine_res.release_app(violate_inst_id, violate_app_res)
                
                violate_inst_id = violate_machine_res.any_self_violate_constriant(self.inst_app_dict, 
                                                                                  self.app_res_dict, 
                                                                                  self.app_constraint_dict)
                self.sort_machine()
        # 将只有一个 inst 的机器迁移出去
        index = 0
        for machine_id, one_inst_machine_running_res in self.sorted_machine_res:
            if (index  % 100 == 0):
                print(getCurrentTime(), '%d single inst machine initialized\r' % index, end='')
            index += 1

            if (len(one_inst_machine_running_res.running_inst_list) != 1):
                continue

            inst_id = one_inst_machine_running_res.running_inst_list[0]
            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            if (not self.dispatch_inst_internal(inst_id, [machine_id])):
                print_and_log("ERROR! init_deploying() Failed to immigrate inst %d to machine %d" % (violate_inst_id))
                exit(-1)

            one_inst_machine_running_res.release_app(inst_id, app_res)

        inited_filename = r'%s/../input/%s/initialized_deploy.csv' % (runningPath, data_set)
        with open(inited_filename, 'w') as inited_deploy_file:
            for each_migrating in self.migrating_list:
                inited_deploy_file.write('%s\n' % (each_migrating))

        print(getCurrentTime(), 'init_deploying done..., output initialized file to %s' % inited_filename)
        return
    
    
    def sum_scores_of_machine(self):
        scores = 0
        for machine_id, machine_running_res in self.sorted_machine_res:
            scores += machine_running_res.get_machine_real_score()
            
        return scores
    
    def get_immigratable_machine(self, inst_id, skipped_machine_id):
        immigratable_machine_list = []
        scores_dict = {'big_machine':set(), 'small_machine':set()}
        
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        
        for machine_id,  immigrating_machine in self.sorted_machine_res:
            if (machine_id == skipped_machine_id):
                continue
            
            # 大小两种机器中得分相同的只保留一台
            if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
                increased_score = round(immigrating_machine.immigrating_delta_score(app_res), 2)
                if (machine_id <= 3000 and increased_score not in scores_dict['small_machine']):
                    scores_dict['small_machine'].add(increased_score)
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
                elif (machine_id > 3000 and increased_score not in scores_dict['big_machine']):
                    scores_dict['big_machine'].add(increased_score)
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )

        return immigratable_machine_list

    def get_immigratable_machine_ex(self, inst_id, skipped_machine_id):
        immigratable_machine_list = []
        scores_list = []
        
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        
        does_prefer = does_prefer_small_machine(app_res)
        # 倾向于部署在小机器上
        if (does_prefer):
            machine_start_idx = 1
            machine_end_idx = 3001
        else:  # 倾向于部署在大机器上
            machine_start_idx = 3001
            machine_end_idx = 6001

        for machine_id in range(machine_start_idx, machine_end_idx):
            if (machine_id == skipped_machine_id):
                continue
            
            immigrating_machine = self.machine_runing_info_dict[machine_id]
            if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
                increased_score = round(immigrating_machine.immigrating_delta_score(app_res), 2)
                appended, scores_list = append_score_by_score_diff(scores_list, increased_score)
                if (appended):
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
        
        if (len(immigratable_machine_list) > 0):
            return immigratable_machine_list
        
        scores_list.clear()
        # 没有可迁入的 小/大 机器，这里重新尝试 大/小 机器
        if (does_prefer):
            machine_start_idx = 3001
            machine_end_idx = 6001      
        else:
            machine_start_idx = 1
            machine_end_idx = 3001

        for machine_id in range(machine_start_idx, machine_end_idx):
            if (machine_id == skipped_machine_id):
                continue

            immigrating_machine = self.machine_runing_info_dict[machine_id]
            if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
                increased_score = round(immigrating_machine.immigrating_delta_score(app_res), 2)
                appended, scores_list = append_score_by_score_diff(scores_list, increased_score)
                if (appended):
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
                    
        return immigratable_machine_list

    
    # 将前 n-1 步的迁移方案与第 n 步的合并
    # 每个方案的格式为：  [{machine_id:[inst list], machine_id:[inst list], ...}, immigrating score]
    def merge_migration_solution(self, current_solution, one_step_solution, machine_real_score):
        one_step_len = len(one_step_solution)
        current_len = len(current_solution)
        total = len(current_solution) * len(one_step_solution)
        print_and_log('merge_migration_solution, possible steps %d (%d/%d)' % (total, current_len, one_step_len))
        migration_solution = []
        solution_scores_list = []
        idx = 0
        for each_current in current_solution:
            if (idx % 1000 == 0):
                print(getCurrentTime(), '%d / %d handle\r' % (idx, total), end='')

            if (each_current[1] >= machine_real_score):
                idx += one_step_len
                continue

            for one_step in one_step_solution:
                if (one_step[1] >= machine_real_score):
                    idx += 1
                    continue
                
                each_current_tmp = copy.deepcopy(each_current)
            
                for immigrating_machine_id in one_step[0].keys():  # 这里只有 1 个 key
                    idx += 1

                    # one step 中要迁入的 machine 没有出现在前 n-1 步中, 直接加入到当前的迁移方案中
                    if (immigrating_machine_id not in each_current_tmp[0]):
                        each_current_tmp[0][immigrating_machine_id] = one_step[0][immigrating_machine_id]
                        each_current_tmp[1] = round(each_current_tmp[1] + one_step[1], 2)
                    else:  # one step 中要迁入的 machine 已经出现在前 n-1 步中， 需要判断是否能够继续迁入
                        inst_list = each_current_tmp[0][immigrating_machine_id] + one_step[0][immigrating_machine_id]
                        immigrating_machine_res = self.machine_runing_info_dict[immigrating_machine_id] 
                        if (not immigrating_machine_res.can_dispatch_ex(inst_list, self.inst_app_dict, self.app_res_dict, self.app_constraint_dict)):
                            continue

                        # 当前迁入列表所增加的分数
                        tmp_app_res = AppRes.sum_app_res_by_inst(each_current_tmp[0][immigrating_machine_id], self.inst_app_dict, self.app_res_dict)
                        cur_delta_score = immigrating_machine_res.immigrating_delta_score(tmp_app_res)

                        # 继续迁入 inst 所增加的分数
                        tmp_app_res = AppRes.sum_app_res_by_inst(inst_list, self.inst_app_dict, self.app_res_dict)
                        delta_score = immigrating_machine_res.immigrating_delta_score(tmp_app_res)

                        # 两者的差就是继续迁入 inst 后，该方案所增加的分数
                        each_current_tmp[1] = round(each_current_tmp[1] + delta_score - cur_delta_score, 2)
                        each_current_tmp[0][immigrating_machine_id].extend(one_step[0][immigrating_machine_id])

                        if (each_current_tmp[1] >= machine_real_score):
                            continue

                    appended, solution_scores_list = append_score_by_score_diff(solution_scores_list, each_current_tmp[1])
                    if (appended):
                        migration_solution.append(each_current_tmp)

        return migration_solution
     
    def migrate_running_inst_list(self, migrating_machine_res):
        inst_id = migrating_machine_res.running_inst_list[0]
        machine_id = migrating_machine_res.machine_res.machine_id
        dp_immigrating_solution_list = self.get_immigratable_machine_ex(inst_id, migrating_machine_res.get_machine_id())
        print(getCurrentTime(), 'machine %d, 1st / %d step solution is %d' % \
              (machine_id, len(migrating_machine_res.running_inst_list), len(dp_immigrating_solution_list)))
        
        for inst_idx in range(1, len(migrating_machine_res.running_inst_list)):
            each_inst = migrating_machine_res.running_inst_list[inst_idx]
            app_res = self.app_res_dict[self.inst_app_dict[each_inst]]

            one_step_solution = self.get_immigratable_machine_ex(each_inst, migrating_machine_res.get_machine_id())

            dp_immigrating_solution_list = self.merge_migration_solution(dp_immigrating_solution_list, 
                                                                         one_step_solution,
                                                                         migrating_machine_res.get_machine_real_score())
            print(getCurrentTime(), 'machine %d, %d / %d step solution is %d' % 
                  (machine_id,inst_idx + 1, len(migrating_machine_res.running_inst_list), len(dp_immigrating_solution_list)))
            if (len(dp_immigrating_solution_list) == 0):
                break
            
        # 在所有的迁移方案中找到迁入分数最小的
        min_solution_score = 1e9            
        for idx, each_solution in enumerate(dp_immigrating_solution_list):
            if (each_solution[1] < min_solution_score):
                min_solution_score = each_solution[1]
                min_solution_idx = idx

        # 增加的分数 不可行 , 不用再继续尝试
        if (min_solution_score > migrating_machine_res.get_machine_real_score()):
            print_and_log('migrating %s, running len %d, increased score %f > (real score %f), continue...' % \
                          (migrating_machine_res.machine_res.machine_id, len(migrating_machine_res.running_inst_list), 
                           min_solution_score, migrating_machine_res.get_machine_real_score()))
            return False

        print_and_log('migrating solution for machine : %d, %s' % (machine_id, dp_immigrating_solution_list[min_solution_idx]))
        # 迁入
        for immigrating_machine, inst_list in dp_immigrating_solution_list[min_solution_idx][0].items():
            for each_inst in inst_list:
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]

                # 迁入
                if (not self.machine_runing_info_dict[immigrating_machine].dispatch_app(each_inst, app_res, self.app_constraint_dict)):
                    print_and_log("ERROR! migrate_running_inst_list() Failed to immigrate inst %d to machine %d" % (each_inst, immigrating_machine))
                    return False

                self.migrating_list.append('inst_%d,machine_%d' % (each_inst, immigrating_machine))

                migrating_machine_res.release_app(each_inst, app_res) # 迁出 inst
        
        return True

    # First fit 算法将 inst 分发到 prefer 的机器上
    def ff_dispatch(self, inst_id, app_res, machine_start_idx, machine_end_idx):        

        for i, machine_info_tuple in enumerate(self.sorted_machine_res): # sorted_machine_res 是已经部署了 inst 的机器列表, 按照得分由低到高排序
            machine_id =  machine_info_tuple[0]
            if (machine_id not in range(machine_start_idx, machine_end_idx)):
                continue

            machine_running_res = machine_info_tuple[1]
            if (machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                self.migrating_list.append('inst_%d,machine_%d' % (inst_id, machine_id))
                return True

        return False

    # First fit 算法将 inst 分发到 prefer 的机器上, 若分发失败，则使用 DP 算法将得分最高的 prefer 的机器上的 inst 全部迁出
    def dispatch_inst(self, inst_id):
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

        does_prefer = does_prefer_small_machine(app_res)
        if (does_prefer):
            b_dispatched = self.ff_dispatch(inst_id, app_res, 1, 3001)
            if (not b_dispatched):
                b_dispatched = self.ff_dispatch(inst_id, app_res, 3001, 6001)
        else:
            b_dispatched = self.ff_dispatch(inst_id, app_res, 3001, 6001)
            if (not b_dispatched):
                b_dispatched = self.ff_dispatch(inst_id, app_res, 1, 3001)

        if (b_dispatched):
            self.sort_machine()
            return True

        print_and_log("FF failed for inst %d, trying to migrating machine with highest score" % (inst_id))
        
        if (not self.print_all_scores):
            for machine_id, machine_running_res in self.sorted_machine_res:
                logging.info('machine_%d, %f, ' % (machine_id, machine_running_res.get_machine_real_score(), len(machine_running_res.running_inst_list)))
            
            self.print_all_scores = True

        # 没有能分配当前 inst 的机器， 将符合 inst 资源需求且得分最高的机器上的  inst list 都迁出
        max_score = 0
        does_prefer = does_prefer_small_machine(app_res)
        if (does_prefer):
            machine_start_idx = 1
            machine_end_idx = 3001
        else:
            machine_start_idx = 3001
            machine_end_idx = 6001

        for machine_id in range(machine_start_idx, machine_end_idx):
            migrating_machine_res = self.machine_runing_info_dict[machine_id]
            if (migrating_machine_res.meet_inst_res_require(app_res)):
                score = migrating_machine_res.get_machine_real_score()
                if (max_score < score):
                    max_score = score
                    migrating_machine_id = machine_id

        b_migrated = self.migrate_running_inst_list(self.machine_runing_info_dict[migrating_machine_id])        

        if (not b_migrated):
            print_and_log('dispatch failed for instance %s, all machine are skipped' % inst_id)
            return False

        if (not self.machine_runing_info_dict[migrating_machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)):
            print_and_log("ERROR! dispatch_inst() Failed to immigrate inst %d to machine %d" % 
                          (inst_id, migrating_machine_id))
            return False

        self.migrating_list.append('inst_%d,machine_%d' % (inst_id, migrating_machine_id))

        self.sort_machine()

        return True
    
    def sort_machine(self, reverse = False):
        self.sort_machine_by_score(reverse)

    # 按照 get_cpu_percentage 从低到高排序, 优先分配使用率低的机器
    def sort_machine_by_cpu_per(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict, key=lambda d:d[1].get_cpu_percentage())

    # 按照 机器的得分 从低到高排序, 优先分配得分低的机器
    def sort_machine_by_score(self, reverse):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_machine_score(), reverse=reverse)


    def output_submition(self):
        filename = 'submit_%s.csv' % datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = open(r'%s/../output/%s/%s' % (runningPath, data_set, filename), 'w')
        print(getCurrentTime(), 'writing output file %s' % filename)

        for each_migrating in self.migrating_list:
            output_file.write('%s\n' % (each_migrating))

        output_file.close()
        

        
        
        