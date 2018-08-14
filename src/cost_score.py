#coding=utf-8
'''
Created on Jun 29, 2018

@author: Heng.Zhang
'''

from global_param import *
import csv
from MachineRunningInfo import *
from AppRes import *
import math
import logging
import copy
import datetime
import multiprocessing
import os
# from functools import reduce

class AdjustDispatch(object):
    def __init__(self):
        
        self.machine_runing_info_dict = {} 
        print(getCurrentTime(), 'loading machine_resources.csv')
        machine_res_csv = csv.reader(open(r'%s/../input/%s/machine_resources.csv' % (runningPath, data_set), 'r'))
        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 
        
        print(getCurrentTime(), 'loading app_resources.csv')
        self.app_res_dict = {}
        app_res_csv = csv.reader(open(r'%s/../input/%s/app_resources.csv' % (runningPath, data_set), 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0])
            self.app_res_dict[app_id] = AppRes(each_app)

        print(getCurrentTime(), 'loading app_interference.csv')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s/../input/%s/app_interference.csv' % (runningPath, data_set), 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.cost = 0

        if (data_set == 'a'):
            self.submit_filename = 'a_5746'
        else:
            self.submit_filename = 'b_6552'

        time_now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        log_file = r'%s/../log/cost_%s_%s_%s.log' % (runningPath, data_set, self.submit_filename, time_now)

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')
        
        self.output_filename = r'%s/../output/%s/%s_optimized_%s.csv' % (runningPath, data_set, self.submit_filename, time_now)        
        return

    def sorte_machine(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key = lambda d : d[1].get_machine_real_score(), reverse = True)

    def adj_dispatch_reverse(self):

        for machine_idx in range(len(self.sorted_machine_res) - 1, -1, -1):
            machine_id = self.sorted_machine_res[machine_idx][0]
            lightest_load_machine = self.machine_runing_info_dict[machine_id]
            if (len(lightest_load_machine.running_inst_list) > 0):
                break

        for idx in range(machine_idx, -1, -1):
            machine_id = self.sorted_machine_res[idx][0]
            lightest_load_machine = self.machine_runing_info_dict[machine_id]
            if (lightest_load_machine.get_machine_real_score() > 98):
                print_and_log('machine %d \'s %f real socre > 98, breaking...' % lightest_load_machine.get_machine_real_score())
                break

            immigrating_machine_dict = {}
            sum_increated_score = 0 # 将  inst 迁入后增加的分数的总和, 若 > 98 则不迁出

            for each_inst in lightest_load_machine.running_inst_list:
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
                min_delta_score = 1e9 # 将 inst 迁入后增加的分数， 找到增加分数最小的机器

                # 在所有的机器上找到迁入最小的分数
                for i in range(machine_idx):
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
                        increased_score = heavy_load_machine.immigrating_score(sum_app_res)
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
    
                    sum_increated_score += min_delta_score

                # 增加的分数 > 98, 不可行 
                if (sum_increated_score > 98):
                    break
            
            if (sum_increated_score > 98 or len(immigrating_machine_dict) == 0):
                print_and_log('migrating %s, running len %d, increased score %f (real score %f) > 98, continue...' % \
                              (lightest_load_machine.machine_res.machine_id, len(lightest_load_machine.running_inst_list), 
                               sum_increated_score, lightest_load_machine.get_machine_real_score()))
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
            
                    lightest_load_machine.release_app(each_inst, app_res) # 迁出 inst

            self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序

            # 迁移之后重新计算得分
            next_cost = 0
            for machine_id, machine_running_res in self.sorted_machine_res:
                next_cost += machine_running_res.get_machine_real_score()   

            print_and_log('migrating %s, increased score %f, next_cost %f' % \
                  (lightest_load_machine.machine_res.machine_id, sum_increated_score, next_cost))

        return next_cost
    
    def get_immigratable_machine(self, inst_id, machine_start_idx, is_1st_step):
        immigratable_machine_list = []
        empty_small_machine = False
        empty_big_machine = False
        
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        for i in range(machine_start_idx + 1, len(self.sorted_machine_res)):
            machine_id = self.sorted_machine_res[i][0]
            immigrating_machine = self.sorted_machine_res[i][1]
            if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
                increased_score = immigrating_machine.immigrating_delta_score(app_res)
                # 碰到一台空机器， 没必要再在其他的空机器上尝试
                if (increased_score == 98 and is_1st_step):
                    if (machine_id <= 3000 and empty_small_machine == False):
                        empty_small_machine = True
                        immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
                    elif (machine_id > 3000 and empty_big_machine == False):
                        empty_big_machine = True
                        immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
                else:
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )

        return immigratable_machine_list
    
    def get_immigratable_machine_ex(self, inst_id, skipped_machine_id, b_is_first):
        immigratable_machine_list = []
        scores_list = []
        
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        
#         does_prefer = does_prefer_small_machine(app_res)
#         # 倾向于部署在小机器上
#         if (does_prefer):
#             machine_start_idx = 1
#             machine_end_idx = 3001
#         else:  # 倾向于部署在大机器上
#             machine_start_idx = 3001            
#             machine_end_idx = 6001

        machine_start_idx = 1
        machine_end_idx = 6001

        for machine_id in range(machine_start_idx, machine_end_idx):
            if (machine_id == skipped_machine_id):
                continue

            immigrating_machine = self.machine_runing_info_dict[machine_id]
            if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
                
                increased_score = round(immigrating_machine.immigrating_delta_score(app_res), 2)
                if (not b_is_first and increased_score > 0):
                    continue
                
                if (increased_score not in scores_list):
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
                    scores_list.append(increased_score)
        
        # 在 prefer 的机器中没有或只找到一台，则尝试在另外的机器中继续查找
#         if (len(immigratable_machine_list) > 1):
#             return immigratable_machine_list
#         
#         scores_list.clear()
#         # 没有可迁入的 小/大 机器，这里重新尝试 大/小 机器
#         if (does_prefer):
#             machine_start_idx = 3001
#             machine_end_idx = 6001      
#         else:
#             machine_start_idx = 1
#             machine_end_idx = 3001
# 
#         for machine_id in range(machine_start_idx, machine_end_idx):
#             if (machine_id == skipped_machine_id):
#                 continue
# 
#             immigrating_machine = self.machine_runing_info_dict[machine_id]
#             if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
#                 increased_score = round(immigrating_machine.immigrating_delta_score(app_res), 2)
#                 if (not b_is_first and increased_score > 0):
#                     continue
# 
#                 if (increased_score not in scores_list):
#                     immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
#                     scores_list.append(increased_score)
# 
# #                 appended, scores_list = append_score_by_score_diff(scores_list, increased_score)
# #                 if (appended):
# #                     immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )

        return immigratable_machine_list    
    
    # 将前 n-1 步的迁移方案与第 n 步的合并
    # 每个方案的格式为：  [{machine_id:[inst list], machine_id:[inst list], ...}, immigrating score]
    def merge_migration_solution(self, current_solution, one_step_solution, migrating_delta_score, best_migrating_score, machine_real_score):
        one_step_len = len(one_step_solution)
        current_len = len(current_solution)
        total = len(current_solution) * len(one_step_solution)
        print_and_log('merge_migration_solution, possible steps %d (%d/%d)' % (total, current_len, one_step_len))

        cpu_cnt = multiprocessing.cpu_count()
        cur_solution_cnt = len(current_solution)
        solutions_for_each_subprocess = cur_solution_cnt // cpu_cnt

        main_print_once = True
        is_main_process = True

        subprocess_pid_set = set()
        for subproce_idx in range(0, cpu_cnt):
            solution_start = subproce_idx * solutions_for_each_subprocess
            solution_end = solution_start + solutions_for_each_subprocess
            if (subproce_idx == cpu_cnt - 1):  # last sub process handles with the rest solutions
                solution_end = cur_solution_cnt

            # 启动多个子进程，每个子进程处理一部分 merge
            pid = os.fork()
            if (pid != 0):  # main process
                subprocess_pid_set.add(pid)
                if (not main_print_once):
                    solution_start = 0
                    solution_end = 0 # main process does nothing
                    pid = os.getpid()
                    print(getCurrentTime(), 'main process %d started solution (%d, %d)' % (pid, solution_start, solution_end))
                    main_print_once = True
                    is_main_process = True
            else:  # child process
                is_main_process = False
                pid = os.getpid()
#                 print(getCurrentTime(), 'sub-process %d started solution (%d, %d)\r' % (pid, solution_start, solution_end), end='')
                break  # sub process break

        idx = solution_start
        migration_solution = []
        solution_scores_set = set()

        for solution_idx in range(solution_start, solution_end):
            each_current = current_solution[solution_idx]        
            if (idx % 1000 == 0):
                print(getCurrentTime(), '%d / %d handle\r' % (idx, total), end='')

            if (each_current[1] >= machine_real_score):
                idx += one_step_len
                continue

            for one_step in one_step_solution:
                if (one_step[1] > machine_real_score):
                    idx += 1
                    continue
                
                each_current_tmp = copy.deepcopy(each_current)
            
                for immigrating_machine_id in one_step[0].keys():  # 这里只有 1 个 key
                    idx += 1

                    # one step 中要迁入的 machine 没有出现在前 n-1 步中, 直接加入到当前的迁移方案中
                    if (immigrating_machine_id not in each_current_tmp[0]):
                        each_current_tmp[0][immigrating_machine_id] = one_step[0][immigrating_machine_id]
                        each_current_tmp[1] = round(each_current_tmp[1] + one_step[1], 2)                    
                    else: # one step 中要迁入的 machine 已经出现在前 n-1 步中， 需要判断是否能够继续迁入
                        # inst_list 是 running inst list 的一部分，已经符合约束了  
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
                        
                    if (each_current_tmp[1] < machine_real_score and each_current_tmp[1] not in solution_scores_set):
                        migration_solution.append(each_current_tmp)
                        solution_scores_set.add(each_current_tmp[1])

        if (is_main_process):
            # 主进程等待所有的子进程结束， 然后将 数据合并
            finished_subprocess_set = set()
            while (len(subprocess_pid_set) > 0):
                try:
                    pid, result = os.wait()
                except:
                    print(getCurrentTime(), 'result = os.wait() exception')
                if (pid in subprocess_pid_set):
                    subprocess_pid_set.remove(pid)
                    finished_subprocess_set.add(pid)
                    print(getCurrentTime(), 'child process %d ended, %d are running\r' % (pid, len(subprocess_pid_set)), end='')

#                 time.sleep(1)

            # all of sub processes finished, merge solutions here
            solution_scores_set.clear()
            for each_sub_pid in finished_subprocess_set:
                sub_merge_file = r'%s/../output/%s/sub_merge_%d.csv' % (runningPath, data_set, each_sub_pid)
                sub_merge_csv = csv.reader(open(sub_merge_file, 'r'))
                for each_solution in sub_merge_csv:
                    immigrating_dict = {}
                    for each_migrating in each_solution[:-1]:
                        tmp = each_migrating.split(":")
                        immigrating_machine_id = int(tmp[0])
                        immigrating_inst_list = list(map(int, tmp[1].split("|")))
                        immigrating_dict[immigrating_machine_id] = immigrating_inst_list

                    migrating_score = round(float(each_solution[-1]), 2)
                    if (migrating_score not in solution_scores_set):
                        migration_solution.append([immigrating_dict, migrating_score])
                        solution_scores_set.add(migrating_score)
                os.remove(sub_merge_file)
        else:
            # 子进程将迁移方案写入文件， 由主进程合并， 每行格式为       
            # machine_id1:inst_1|inst_2,machine_id2:inst_3|inst_4,immigrating score
            # 1：123|234，2：34|356，123.2243
            with open(r'%s/../output/%s/sub_merge_%d.csv' % (runningPath, data_set, pid), 'w') as sub_merge_file:
                for each_solution in migration_solution:
                    to_string = ""
                    for immigrating_machine_id, immigrating_inst_list in each_solution[0].items():
                        to_string += '%d:%s,' % (immigrating_machine_id, "|".join(list(map(str, immigrating_inst_list))))

                    sub_merge_file.write("%s%s\n" % (to_string, each_solution[1]))

            exit(0)  # 子进程退出
                
        return migration_solution

    def adj_dispatch_ex(self, max_score):
        
        machine_idx = 0
        
        next_cost = self.sum_scores_of_machine()

        while (self.sorted_machine_res[machine_idx][1].get_machine_real_score() > max_score):
            machine_id = self.sorted_machine_res[machine_idx][0]
            if (machine_id == 430):
                print(machine_id)
                
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
    

    def adj_dispatch_dp(self):
        print_and_log('entered adj_dispatch_dp')

        machine_start_idx = 0
        
        next_cost = self.sum_scores_of_machine()

        while (self.sorted_machine_res[machine_start_idx][1].get_machine_real_score() > 98 and machine_start_idx < MACHINE_CNT):
            machine_id = self.sorted_machine_res[machine_start_idx][0]

#       for machine_id in range(1, 3001):
            heavest_load_machine = self.machine_runing_info_dict[machine_id]
            if (len(heavest_load_machine.running_inst_list) == 0 or heavest_load_machine.get_machine_real_score() < 100):
                machine_start_idx += 1
                continue

            heavest_load_machine.sort_running_inst_list(self.app_res_dict, self.inst_app_dict)

            inst_id = heavest_load_machine.running_inst_list[0]
            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            migrating_delta_score = heavest_load_machine.migrating_delta_score(app_res)

            # 生成迁移方案的第一步， 以及迁入后增加的分数
            dp_immigrating_solution_list = self.get_immigratable_machine_ex(inst_id, machine_id, True)
            print(getCurrentTime(), 'machine %d, 1st / %d step solution is %d' % (machine_id, len(heavest_load_machine.running_inst_list),
                                                                                   len(dp_immigrating_solution_list)))
            if (len(heavest_load_machine.running_inst_list) == 0):
                machine_start_idx += 1
                continue

            best_migrating_score = 0
            for each_solution in dp_immigrating_solution_list:
                if (migrating_delta_score - each_solution[1] > best_migrating_score):
                    best_migrating_score = migrating_delta_score - each_solution[1]
                    best_migraring_solution = copy.deepcopy(each_solution)
            
            # 生成第 2 -> N 步的迁移方案
            total_steps = len(heavest_load_machine.running_inst_list)
            for inst_idx in range(1, len(heavest_load_machine.running_inst_list)):
                print(getCurrentTime(), 'searching machine %d %d/%d\r' % (machine_id, inst_idx, total_steps), end='')
                each_inst = heavest_load_machine.running_inst_list[inst_idx]

                # 将 inst list 迁移出后所减少的分数
                tmp_app = AppRes.sum_app_res_by_inst(heavest_load_machine.running_inst_list[:(inst_idx + 1)], self.inst_app_dict, self.app_res_dict)
                migrating_delta_score = heavest_load_machine.migrating_delta_score(tmp_app)

                one_step_solution = self.get_immigratable_machine_ex(each_inst, machine_id, True)

                dp_immigrating_solution_list = self.merge_migration_solution(dp_immigrating_solution_list, one_step_solution,
                                                                             migrating_delta_score, best_migrating_score,
                                                                             heavest_load_machine.get_machine_real_score())
                print(getCurrentTime(), 'machine %d, %d/%d step solution is %d ' % (machine_id, inst_idx + 1, total_steps, len(dp_immigrating_solution_list)))
                if (len(dp_immigrating_solution_list) == 0):
                    break

                # 将 inst list 迁移出后所减少的分数 - 每个迁移方案所增加的分数， 得到差值最大的方案
                for each_solution in dp_immigrating_solution_list:
                    if (migrating_delta_score - each_solution[1] > best_migrating_score):
                        best_migrating_score = migrating_delta_score - each_solution[1]
                        best_migraring_solution = copy.deepcopy(each_solution)

            # 迁入所增加的分数至少要减少 1 分 , 否则不用再继续尝试
            if (best_migrating_score < 1):
                print_and_log('migrating %s, running len %d, migrating delta score %f > (real score %f), continue... ' % \
                              (heavest_load_machine.machine_res.machine_id, len(heavest_load_machine.running_inst_list), 
                               best_migrating_score, heavest_load_machine.get_machine_real_score()))

                machine_start_idx += 1
                continue

            print_and_log('migrating solution for machine : %d, real score %f, migrating delta score %f, %s ' % \
                          (machine_id, heavest_load_machine.get_machine_real_score(), best_migrating_score,
                           best_migraring_solution))
            # 迁入
            for immigrating_machine, inst_list in best_migraring_solution[0].items():
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
            
            self.output_optimized()

        print_and_log('leaving adj_dispatch_dp with next cost %f' % next_cost)
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
        while (self.sorted_machine_res[machine_idx][1].get_machine_real_score() > 100):
            heavest_load_machine = self.machine_runing_info_dict[self.sorted_machine_res[machine_idx][0]]
            if (heavest_load_machine.machine_res.machine_id in non_migratable_machine):
                machine_idx += 1
                continue

            # 将 inst 迁出后减少的分数
            decreased_score = 0

            # 尝试迁出全部的 inst 效果不好
            # 找到一个得分最高的 inst
            heavest_load_machine.sort_running_inst_list()
            migrate_inst = heavest_load_machine.running_inst_list[0]
            decreased_score = heavest_load_machine.migrating_delta_score(self.app_res_dict[self.inst_app_dict[migrate_inst]])

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
            
            self.output_optimized()

        return  self.sum_scores_of_machine()
    
    def dispacth_app(self):
        # inst 运行在哪台机器上
        insts_running_machine_dict = dict()

        print(getCurrentTime(), 'loading instance_deploy.csv...')

        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s/../input/%s/instance_deploy.csv' % (runningPath, data_set), 'r'))
        i = 0
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            app_id = int(each_inst[1])
            self.inst_app_dict[inst_id]  = app_id
            if (len(each_inst[2]) > 0):
                machine_id = int(each_inst[2])
                self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)
                insts_running_machine_dict[inst_id] = machine_id
                i += 1
                
        self.migrating_list = []

        print(getCurrentTime(), 'loading %s.csv' % self.submit_filename)        
        app_dispatch_csv = csv.reader(open(r'%s/../output/%s/%s.csv' % (runningPath, data_set, self.submit_filename), 'r'))
        for each_dispatch in app_dispatch_csv:
            inst_id = int(each_dispatch[0].split('_')[1])
            machine_id = int(each_dispatch[1].split('_')[1])
            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            # inst 已经部署到了其他机器上，这里需要将其迁出
            if (inst_id in insts_running_machine_dict):
                immigrating_machine = insts_running_machine_dict[inst_id]
                self.machine_runing_info_dict[immigrating_machine].release_app(inst_id, app_res)                

            if (not self.machine_runing_info_dict[machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                self.machine_runing_info_dict[machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)
                print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (inst_id, machine_id))
                exit(-1)

            insts_running_machine_dict[inst_id] = machine_id      
            self.migrating_list.append('inst_%d,machine_%d' % (inst_id, machine_id)) 

        self.sorte_machine()
        

    def check_one_constraince(self, app_A_id, app_B_id, app_B_running_inst):
        if (app_A_id in self.app_constraint_dict and app_B_id in self.app_constraint_dict[app_A_id]):
            if (app_A_id == app_B_id):
                return app_B_running_inst <= self.app_constraint_dict[app_A_id][app_B_id] + 1
            else:
                return app_B_running_inst <= self.app_constraint_dict[app_A_id][app_B_id]

        return True
    
    def check_constraince(self, machine_running_res):
        for inst_A in machine_running_res.running_inst_list:
            app_A = self.app_res_dict[self.inst_app_dict[inst_A]]
            for inst_B in machine_running_res.running_inst_list:
                app_B = self.app_res_dict[self.inst_app_dict[inst_B]]
                app_B_running_cnt = machine_running_res.running_app_dict[app_B.app_id]

                if (not self.check_one_constraince(app_A.app_id, app_B.app_id, app_B_running_cnt)):
                    return False

        return True
        
    def check_dispatching(self, machine_running_res):
        if (not self.check_constraince(machine_running_res)):
            return False        

        # 符合约束，检查资源是否满足
        tmp = AppRes.sum_app_res_by_inst(machine_running_res.running_inst_list, self.inst_app_dict, self.app_res_dict)
        return machine_running_res.machine_res.meet_inst_res_require(tmp)
    
    def calculate_cost_score(self):

        self.dispacth_app()
    
        # 得分从高到低排序        
        cost = self.sum_scores_of_machine()
    
        for machine_id, machine_running_res in self.sorted_machine_res:
            if (not self.check_dispatching(machine_running_res)):
                print_and_log('ERROR! machine_%d, score %f, running list %s' % (machine_id, machine_running_res.get_machine_real_score(), machine_running_res.running_inst_list))
                return 
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
        
        print_and_log('cost of %s is %f/%f' % (self.submit_filename, cost, cost/SLICE_CNT))

        if (self.sorted_machine_res[-1][1].get_machine_real_score() > 98):
            return cost;

        print_and_log('optimizing for H -> L')        
        next_cost = self.adj_dispatch_dp()
        print_and_log('After adj_dispatch_dp(), score %f -> %f' % (cost, next_cost))
#         while (next_cost < cost):
#             cost = next_cost
#             next_cost = self.adj_dispatch_ex(100)
#             print_and_log('After adj_dispatch_ex(), score %f -> %f' % (cost, next_cost))

#         next_cost = self.adj_dispatch()
#         print_and_log('After adj_dispatch(), score %f -> %f' % (cost, next_cost))
#         while (next_cost < cost):            
#             cost = next_cost
#             next_cost = self.adj_dispatch()
#             print_and_log('After adj_dispatch(), score %f -> %f' % (cost, next_cost))            
        
#         next_cost = self.adj_dispatch_reverse()            
#         while (next_cost < cost):
#             print_and_log('After adj_dispatch_reverse(), score %f -> %f' % (cost, next_cost))
#             cost = next_cost
#             next_cost = self.adj_dispatch()        
     

        with open(r'%s/../output/%s/%s_optimized_%s.csv' % 
                  (runningPath, data_set, self.submit_filename, datetime.datetime.now().strftime('%Y%m%d_%H%M%S')), 'w') as output_file:
            for each_disp in self.migrating_list:
                output_file.write('%s\n' % (each_disp))

        cost = 0
        for machine_id, machine_running_res in self.sorted_machine_res:
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
            cost += machine_running_res.get_machine_real_score()
    
        print(getCurrentTime(), 'finla cost is %f / %f' % (cost, cost / SLICE_CNT))
        
        return cost / SLICE_CNT
        
    def output_optimized(self):
#         with open(r'%s/../output/%s/%s_optimized_%s.csv' % 
#                   (runningPath, data_set, self.submit_filename, datetime.datetime.now().strftime('%Y%m%d_%H%M%S')), 'w') as output_file:
#             for each_disp in self.migrating_list:
#                 output_file.write('%s\n' % (each_disp))

        with open(self.output_filename, 'w') as output_file:
            for each_disp in self.migrating_list:
                output_file.write('%s\n' % (each_disp))
                
        output_file.close()

        cost = 0
        for machine_id, machine_running_res in self.sorted_machine_res:
            cost += machine_running_res.get_machine_real_score()
    
        print(getCurrentTime(), 'finla cost is %f / %f' % (cost, cost / SLICE_CNT))

def add_name():
    submit_filename = 'submit6006_20180704_092755'
    print(getCurrentTime(), 'loading %s.csv' % submit_filename)        
    with open(r'%s\..\output\%s_a.csv' % (runningPath, submit_filename), 'w') as output_file:
        app_dispatch_csv = csv.reader(open(r'%s\..\output\%s.csv' % (runningPath, submit_filename), 'r'))
        for each_dispatch in app_dispatch_csv:
            output_file.write('inst_%s,machine_%s\n' % (each_dispatch[0], each_dispatch[1]))    
    return

if __name__ == '__main__':      
    adjDis = AdjustDispatch()  
    adjDis.calculate_cost_score()
    
    
    
    
    