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
import random
import platform

# from functools import reduce

class AdjustDispatch(object):
    def __init__(self, job_set):
        
        self.job_set = job_set
        
        self.machine_runing_info_dict = {} 
        print(getCurrentTime(), 'loading machine_resources.csv')
        machine_res_csv = csv.reader(open(r'%s/../input/%s/machine_resources.%s.csv' % (runningPath, data_set, job_set), 'r'))
        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0].split('_')[1])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine, job_set) 
        
        print(getCurrentTime(), 'loading app_resources.csv')
        self.app_res_dict = {}
        app_res_csv = csv.reader(open(r'%s/../input/%s/app_resources.csv' % (runningPath, data_set), 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0].split('_')[1])
            self.app_res_dict[app_id] = AppRes(each_app)

        print(getCurrentTime(), 'loading app_interference.csv')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s/../input/%s/app_interference.csv' % (runningPath, data_set), 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0].split('_')[1])
            app_id_b = int(each_cons[1].split('_')[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.cost = 0

        time_now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        log_file = r'%s/../log/cost_%s_%s_%s.log' % (runningPath, data_set, self.job_set, time_now)

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')
        
        self.output_filename = r'%s/../output/%s/%s_optimized_%s.csv' % (runningPath, data_set, self.job_set, time_now)        
        return

    def sort_machine(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key = lambda d : d[1].get_machine_real_score(), reverse = True)

    def sort_machine_by_running_inst_list(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key = lambda d : len(d[1].running_inst_list))

    
    #  将重载机器上的 inst 随机迁移到其他轻载的机器上直到没有空载的机器
    def balance_inst_between_machine(self):
        
#         b_balanced_atleast_one = True
#         while (self.sorted_machine_res[-1][1].get_machine_real_score() == 0 and b_balanced_atleast_one):            
#             print_and_log("here still are empyty load machines...")
#             b_balanced_atleast_one = False
        for round_num in [1]:
            machine_idx = 0
            
            heavy_load_machine_list = []
            light_load_machine_list = []

            self.sort_machine()
                
            for machine_id, machine_running_res in self.sorted_machine_res:
                score = machine_running_res.get_machine_real_score()
                if (score > BASE_SCORE):
                    heavy_load_machine_list.append(machine_id)
                else:
                    light_load_machine_list.append(machine_id)
                    
            light_load_machine_list = sorted(light_load_machine_list, key = lambda machine_id : self.machine_runing_info_dict[machine_id].get_machine_real_score()) 

            for heavy_load_machine_id in heavy_load_machine_list:                
                heavy_load_machine_running_res = self.machine_runing_info_dict[heavy_load_machine_id]
                heavy_score = heavy_load_machine_running_res.get_machine_real_score()
                
                #  将重载机器上的 inst 随机迁移到其他轻载的机器上直到重载机器也成为轻载的
                b_balanced = True
                while (heavy_load_machine_running_res.get_machine_real_score() > BASE_SCORE and b_balanced):
                    b_balanced = False

                    heavy_load_machine_running_res.sort_running_inst_list(self.app_res_dict, self.inst_app_dict, reverse=True)                    
                    inst_id = heavy_load_machine_running_res.running_inst_list[0]
                    app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
                    
                    light_load_machine_list = sorted(light_load_machine_list, 
                                             key = lambda machine_id : self.machine_runing_info_dict[machine_id].get_machine_real_score())
                    
                    for light_load_machine_id in light_load_machine_list:
                        light_load_machine_running_res = self.machine_runing_info_dict[light_load_machine_id]
                        if (light_load_machine_running_res.get_machine_real_score() > BASE_SCORE):
                            continue
                        
                        # 重负载的 inst 放到空机器上 
                        if (self.app_res_dict[self.inst_app_dict[inst_id]].get_cpu_mean() >= 16  and 
                            light_load_machine_running_res.get_machine_real_score() > 0):
                            continue

                        light_score =  light_load_machine_running_res.get_machine_real_score()
                        if (light_load_machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                            heavy_load_machine_running_res.release_app(inst_id, app_res)
                            b_balanced = True
                            b_balanced_atleast_one = True
                            self.migrating_list.append('%d,inst_%d,machine_%d' % (round_num, inst_id, light_load_machine_id))
                            if (light_load_machine_running_res.get_machine_real_score() > BASE_SCORE):
                                logging.info("machine %s, real score %f -> %f with inst %d" % 
                                             (light_load_machine_id, light_score, light_load_machine_running_res.get_machine_real_score(), inst_id))
                            break

                print_and_log("round %d load balance: machine_%d %f -> %f" % 
                              (round_num, heavy_load_machine_id, heavy_score, heavy_load_machine_running_res.get_machine_real_score()), False)
                machine_idx += 1
    
            self.sort_machine()
            
        self.output_optimized()
        print_and_log('leaving balance_inst_between_machine...')
        
    
    def get_immigratable_machine(self, inst_id, skipped_machine_id):
        immigratable_machine_list = []
        scores_list = []
        
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        
        machine_range = list(range(1, g_prefered_machine[self.job_set][1] + 1))
        random.shuffle(machine_range)

        for machine_id in machine_range:
            if (machine_id in skipped_machine_id):
                continue

            immigrating_machine = self.machine_runing_info_dict[machine_id]
#             if (immigrating_machine.get_machine_real_score() > 0):
#                 continue

            if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
                increased_score = round(immigrating_machine.immigrating_delta_score(app_res))

#                 if (appended):
#                     immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
#                     appended, scores_list = append_score_by_score_diff(scores_list, increased_score)

                if (increased_score not in scores_list):
                    scores_list.append(increased_score)
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
        solution_scores_set = set()
        idx = 0
        for each_current in current_solution:
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

        return migration_solution
    
    def merge_migration_solution_fork(self, current_solution, one_step_solution, machine_real_score):
        one_step_len = len(one_step_solution)
        cur_solution_cnt = len(current_solution)
        total = cur_solution_cnt * one_step_len

        cpu_cnt = multiprocessing.cpu_count()
        if (cur_solution_cnt < cpu_cnt * 10):
            print(getCurrentTime(), "current solution len is %d < %d, start only 1 subprocess" % (cur_solution_cnt, cpu_cnt * 10))
            cpu_cnt = 1
            solutions_for_each_subprocess = cur_solution_cnt            
        else:
            cpu_cnt = cpu_cnt * 4 // 5 # 只用 80% 的cpu
            solutions_for_each_subprocess = cur_solution_cnt // cpu_cnt

        print_and_log('merge_migration_solution, possible steps %d (%d/%d), solutions for each subprocess %d' % 
                      (total, cur_solution_cnt, one_step_len, solutions_for_each_subprocess))

        main_print_once = False
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
                    main_print_once = True
                    is_main_process = True
            else:  # child process
                is_main_process = False
                pid = os.getpid()
                break  # sub process break

        idx = solution_start
        
        solution_scores_set = set()
        migration_solution = []

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
    
    def adj_dispatch_dp(self, round_num, opt):
        print_and_log('entered adj_dispatch_dp{%d, %s)' % (round_num, opt))

        migrating_machine_set = []
        skipped_machine_list = []
        
        if (opt):
            # 将 running inst 最少的机器上的 inst 全部迁出
            self.sort_machine_by_running_inst_list()
            longest_inst_list = 3
            for machine_start_idx in range(g_prefered_machine[self.job_set][1]):
                if (len(self.sorted_machine_res[machine_start_idx][1].running_inst_list) > 0 and 
                    len(self.sorted_machine_res[machine_start_idx][1].running_inst_list) <= longest_inst_list):
                    migrating_machine_set.append(self.sorted_machine_res[machine_start_idx][0])

            print_and_log("Here are %d machines whose running inst list less than %d" % (len(migrating_machine_set), longest_inst_list))
        else:
            # 从得分最高的前 10% 的机器迁出 cpu 最高的 n 个 inst
            self.sort_machine()
            for machine_start_idx in range(g_prefered_machine[job_set][1] // 10):
                migrating_machine_set.append(self.sorted_machine_res[machine_start_idx][0])

            print_and_log("Here are %d machines which top 10per score" % (len(migrating_machine_set)))

        machine_start_idx = 0
                                                                                                     # 有迁出 inst 的机器                不再迁入 inst
        next_round_migrating_machine, migrated_machine_list = self.migrate_machine_dp(opt, round_num, migrating_machine_set, migrating_machine_set)

#         while (len(next_round_migrating_machine) > 0 and len(next_round_migrating_machine) < len(migrating_machine_set)):
#             # 有迁出 inst 的机器不再迁入 inst
#             skipped_machine_list = migrated_machine_list
#             
#             # 将 next_round_migrating_machine 按照 runing inst 数量排序后分成两部分， 将第一部分迁移到第二部分
#             next_round_migrating_machine = sorted(next_round_migrating_machine, key=lambda machine_id: len(self.machine_runing_info_dict[machine_id].running_inst_list))
#             migrating_machine_len = int(len(next_round_migrating_machine) * 0.5)
#             migrating_machine_set = next_round_migrating_machine[0:migrating_machine_len]
#             next_round_migrating_machine = next_round_migrating_machine[migrating_machine_len:len(next_round_migrating_machine)]
#             print_and_log("split next_round_migrating_machine to %d / %d " % (len(migrating_machine_set), len(next_round_migrating_machine)))
#             
#             # 将要迁出的机器也不再迁入 inst
#             skipped_machine_list.extend(migrating_machine_set)
#             
#             next_round_migrating_machine, migrated_machine_list = self.migrate_machine_dp(opt, round_num, next_round_migrating_machine, skipped_machine_list)

        next_cost = self.sum_scores_of_machine()

        print_and_log('leaving adj_dispatch_dp with next cost %f' % next_cost)
        return next_cost
    
    #  将 migrating_machine_set 中的 inst 迁移到其他机器上， 但是要跳过 skipped_machine_list
    def migrate_machine_dp(self, opt, round_num, migrating_machine_set, skipped_machine_list):
        print_and_log("migrate_machine_dp(%s, %d, migrating len %d, skipped len %d)" % 
                      (opt, round_num, len(migrating_machine_set), len(skipped_machine_list)))
        is_windows = 'Windows' in platform.platform()
        
        # 本轮没有迁出 inst 的机器， 下一轮继续尝试
        next_round_migrating_machine = []
        
        # 本轮有迁出 inst 的机器，下一轮迁入时要跳过 
        migrated_machine_list = []
        
        machine_start_idx = 0

        for machine_id in migrating_machine_set:
            heavest_load_machine = self.machine_runing_info_dict[machine_id]
            heavy_score = heavest_load_machine.get_machine_real_score() 
            if (len(heavest_load_machine.running_inst_list) == 0):
                machine_start_idx += 1
                continue

            if (opt):
                heavest_load_machine.sort_running_inst_list(self.app_res_dict, self.inst_app_dict, reverse=False)
                migrating_running_inst_list = heavest_load_machine.running_inst_list
            else:
                n_max_cpu_inst = 5
                migrating_running_inst_list = heavest_load_machine.get_max_cpu_inst_list(self.app_res_dict, self.inst_app_dict, n_max_cpu_inst)

            inst_id = migrating_running_inst_list[0]
            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            # 生成迁移方案的第一步， 以及迁入后增加的分数
            # 本轮迁出 inst 的机器不再迁入 inst
            dp_immigrating_solution_list = self.get_immigratable_machine(inst_id, skipped_machine_list)
            print(getCurrentTime(), 'machine %d, real score %f, 1st / %d step solution is %d' % 
                  (machine_id, heavy_score, len(migrating_running_inst_list), len(dp_immigrating_solution_list)))

            best_migrating_score = 0
                # 将 inst list 迁移出后所减少的分数
            tmp_app = self.app_res_dict[self.inst_app_dict[migrating_running_inst_list[0]]]
            migrating_delta_score = heavest_load_machine.migrating_delta_score(tmp_app)
            
            for each_solution in dp_immigrating_solution_list:
                if (migrating_delta_score - each_solution[1] > best_migrating_score):
                    best_migrating_score = migrating_delta_score - each_solution[1]
                    best_migraring_solution = copy.deepcopy(each_solution)

            # 生成第 2 -> N 步的迁移方案
            total_steps = len(migrating_running_inst_list)
            for inst_idx in range(1, total_steps):
                print(getCurrentTime(), 'searching machine %d %d/%d\r' % (machine_id, inst_idx, total_steps), end='')
                each_inst = migrating_running_inst_list[inst_idx]

                # 将 inst list 迁移出后所减少的分数
                tmp_app = AppRes.sum_app_res_by_inst(migrating_running_inst_list[:(inst_idx + 1)], self.inst_app_dict, self.app_res_dict)
                migrating_delta_score = heavest_load_machine.migrating_delta_score(tmp_app)

                # 本轮迁出 inst 的机器不再迁入 inst
                one_step_solution = self.get_immigratable_machine(each_inst, skipped_machine_list)

                if (is_windows):
                    dp_immigrating_solution_list = self.merge_migration_solution(dp_immigrating_solution_list, one_step_solution,
                                                                                 heavest_load_machine.get_machine_real_score())
                else:
                    dp_immigrating_solution_list = self.merge_migration_solution_fork(dp_immigrating_solution_list, one_step_solution,
                                                                                      heavest_load_machine.get_machine_real_score())
                print(getCurrentTime(), 'machine %d, %d/%d step solution is %d ' % (machine_id, inst_idx + 1, total_steps, len(dp_immigrating_solution_list)))
                if (len(dp_immigrating_solution_list) == 0):
                    break

                # 将 inst list 迁移出后所减少的分数 - 每个迁移方案所增加的分数， 得到差值最大的方案
                best_migrating_score = 0
                for each_solution in dp_immigrating_solution_list:
                    if (migrating_delta_score - each_solution[1] > best_migrating_score):
                        best_migrating_score = migrating_delta_score - each_solution[1]
                        best_migraring_solution = copy.deepcopy(each_solution)

            # 迁入所增加的分数至少要减少 1 分 , 否则不用再继续尝试
            if (best_migrating_score < 1):
                print_and_log('migrating %s ( %d / %d ), running len %d, migrating delta score %f (real score %f), continue... ' % \
                              (heavest_load_machine.machine_res.machine_id, machine_start_idx, len(migrating_machine_set),
                               len(migrating_running_inst_list), best_migrating_score, heavest_load_machine.get_machine_real_score()))
                
                next_round_migrating_machine.append(heavest_load_machine.machine_res.machine_id)

                machine_start_idx += 1
                continue

            print_and_log('migrating solution for machine : %d (%d / %d ), real score %f, migrating delta score %f, %s ' % \
                          (machine_id, machine_start_idx, len(migrating_machine_set), 
                           heavest_load_machine.get_machine_real_score(), best_migrating_score,
                           best_migraring_solution))
            # 迁入
            for immigrating_machine, inst_list in best_migraring_solution[0].items():
                for each_inst in inst_list:
                    app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
    
                    # 迁入
                    if (not self.machine_runing_info_dict[immigrating_machine].dispatch_app(each_inst, app_res, self.app_constraint_dict)):
                        print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (each_inst, immigrating_machine))
                        return

                    self.migrating_list.append('%d,inst_%d,machine_%d' % (round_num, each_inst, immigrating_machine))
            
                    heavest_load_machine.release_app(each_inst, app_res) # 迁出 inst

            if (len(heavest_load_machine.running_inst_list) > 0):
                next_round_migrating_machine.append(heavest_load_machine.machine_res.machine_id)
            else:
                migrated_machine_list.append(heavest_load_machine.machine_res.machine_id)

            machine_start_idx += 1

            # save solutio any time
            self.output_optimized()

        print_and_log("migrate_machine_dp(%s, %d) leaving..., next round len %d, migrated len %d)" % 
              (opt, round_num, len(next_round_migrating_machine), len(migrated_machine_list)))

        return next_round_migrating_machine, migrated_machine_list

    def sum_scores_of_machine(self):
        scores = 0
        for machine_id, machine_running_res in self.sorted_machine_res:
            scores += machine_running_res.get_machine_real_score()
            
        return scores
    
    def dispacth_app(self):
        # inst 运行在哪台机器上
        insts_running_machine_dict = dict()

        print(getCurrentTime(), 'loading instance_deploy.csv...')

        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s/../input/%s/instance_deploy.%s.csv' % (runningPath, data_set, self.job_set), 'r'))
        i = 0
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0].split('_')[1])
            app_id = int(each_inst[1].split('_')[1])
            self.inst_app_dict[inst_id]  = app_id
            if (len(each_inst[2]) > 0):
                machine_id = int(each_inst[2].split('_')[1])
                self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)
                insts_running_machine_dict[inst_id] = machine_id
                i += 1

        self.migrating_list = []

#         optimized_file = r'%s/../output/%s/%s_optimized_20180826_194316.csv' % (runningPath, data_set, self.job_set)
#         if (os.path.exists(optimized_file)):
#             print(getCurrentTime(), 'loading %s' % optimized_file)
#             app_dispatch_csv = csv.reader(open(optimized_file, 'r'))
#             for each_dispatch in app_dispatch_csv:
#                 inst_id = int(each_dispatch[1].split('_')[1])
#                 machine_id = int(each_dispatch[2].split('_')[1])
#                 app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
#       
#                 # inst 已经部署到了其他机器上，这里需要将其迁出
#                 if (inst_id in insts_running_machine_dict):
#                     immigrating_machine = insts_running_machine_dict[inst_id]
#                     self.machine_runing_info_dict[immigrating_machine].release_app(inst_id, app_res)                
#       
#                 if (not self.machine_runing_info_dict[machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)):
#                     self.machine_runing_info_dict[machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)
#                     print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (inst_id, machine_id))
#                     exit(-1)
#       
#                 insts_running_machine_dict[inst_id] = machine_id      
#                 self.migrating_list.append('1,inst_%d,machine_%d' % (inst_id, machine_id)) 

        self.sort_machine()

        self.balance_inst_between_machine()   

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
        
        print_and_log('cost of [%s] is %f/%f' % (self.job_set, cost, cost/SLICE_CNT))

        # round 1, 2 are in self.balance_inst_between_machine()
        opt = True
        for round_num in [2, 3]:
            next_cost = self.adj_dispatch_dp(round_num, opt)
            opt = not opt
            print_and_log('After adj_dispatch_dp(%d, %s), score %f -> %f' % (round_num, opt, cost, next_cost))

        cost = 0
        for machine_id, machine_running_res in self.sorted_machine_res:
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
            cost += machine_running_res.get_machine_real_score()
    
        print(getCurrentTime(), 'finla cost is %f / %f' % (cost, cost / SLICE_CNT))
        
        return cost / SLICE_CNT
        
    def output_optimized(self):
        with open(self.output_filename, 'w') as output_file:
            for each_disp in self.migrating_list:
                output_file.write('%s\n' % (each_disp))
                
        output_file.close()

        cost = 0
        for machine_id, machine_running_res in self.sorted_machine_res:
            cost += machine_running_res.get_machine_real_score()
    
        print_and_log('finla cost is %f / %f' % (cost, cost / SLICE_CNT))

def add_name():
    submit_filename = 'submit6006_20180704_092755'
    print(getCurrentTime(), 'loading %s.csv' % submit_filename)        
    with open(r'%s\..\output\%s_a.csv' % (runningPath, submit_filename), 'w') as output_file:
        app_dispatch_csv = csv.reader(open(r'%s\..\output\%s.csv' % (runningPath, submit_filename), 'r'))
        for each_dispatch in app_dispatch_csv:
            output_file.write('inst_%s,machine_%s\n' % (each_dispatch[0], each_dispatch[1]))    
    return

if __name__ == '__main__':
    job_set = sys.argv[1].split("=")[1]     
    
    adjDis = AdjustDispatch(job_set)  
    adjDis.calculate_cost_score()
    
    
    
    
    