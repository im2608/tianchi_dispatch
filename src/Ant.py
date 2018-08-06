'''
Created on Jul 19, 2018

@author: Heng.Zhang
'''
from global_param import *
from MachineRunningInfo import *
import random
import copy

import logging
import csv
import os
import datetime
import json
from sklearn.utils import shuffle

class Ant(object):
    def __init__(self, iter_idx, ant_number):
        self.iter_idx = iter_idx
        self.ant_number = ant_number
        
        log_file = r'%s\..\log\iter_%d_ant_%d.txt' % (runningPath, iter_idx, ant_number)

        self.print_all_scores = False

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')

        #  记录 machine 的运行信息， 包括 cpu 使用量,  cpu 使用率 = cpu 使用量 / cpu 容量， d, p, m, pm, app list
        print(getCurrentTime(), 'loading machine_resources.csv...')
        self.machine_runing_info_dict = {} 
#         machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources_reverse.csv' % runningPath, 'r'))
        machine_res_csv = csv.reader(open(r'%s\..\input\%s\machine_resources.csv' % (runningPath, data_set), 'r'))

        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 

        print(getCurrentTime(), 'loading app_resources.csv...')
        self.app_res_dict = [0 for x in range(APP_CNT + 1)]
        app_res_csv = csv.reader(open(r'%s\..\input\%s\app_resources.csv' % (runningPath, data_set), 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0])
            self.app_res_dict[app_id] = AppRes(each_app)

        print(getCurrentTime(), 'loading app_interference.csv...')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\%s\app_interference.csv' % (runningPath, data_set), 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])
        
        self.inst_running_machine_dict = {}
        self.migrating_list = []        
        print(getCurrentTime(), 'loading instance_deploy.csv...')
        self.inst_app_dict = {}
        self.dispatchable_inst_list = []
        inst_app_csv = csv.reader(open(r'%s\..\input\%s\instance_deploy.csv' % (runningPath, data_set), 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            app_id = int(each_inst[1])
            self.inst_app_dict[inst_id] = app_id
            self.dispatchable_inst_list.append(inst_id)
            if (len(each_inst[2]) > 0):
                machine_id = int(each_inst[2])
                self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id],DISPATCH_RATIO)
                if (inst_id in self.inst_running_machine_dict):
                    self.machine_runing_info_dict[self.inst_running_machine_dict[inst_id]].release_app(inst_id, 
                                                                self.app_res_dict[self.inst_app_dict[inst_id]])
                self.inst_running_machine_dict[inst_id] = machine_id
        
        self.dispatchable_inst_list = shuffle(self.dispatchable_inst_list)

        with open(r'%s\..\input\%s\machine_item_pheromone.txt' % (runningPath, data_set), 'r') as pheromone_file:
            self.machine_item_pheromone = json.load(pheromone_file)  
            self.cur_def_pheromone = float(self.machine_item_pheromone['def'])               

        # 加载一个可行解，在它的基础上进行优化
        inited_filename = r'%s\..\input\%s\feasible_solution.csv' % (runningPath, data_set)
        print(getCurrentTime(), 'loading a solution %s' % inited_filename)

        inst_disp_csv = csv.reader(open(inited_filename, 'r'))
        for each_inst in inst_disp_csv:
            inst_id = int(each_inst[0].split('_')[1])
            machine_id = int(each_inst[1].split('_')[1])

            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
            if (not self.machine_runing_info_dict[machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                print_and_log("ERROR! Ant(%d, %d) Failed to immigrate inst %d to machine %d" % (self.iter_idx, self.ant_number, inst_id, machine_id))
                exit(-1)
                
            if (inst_id in self.inst_running_machine_dict):
                self.machine_runing_info_dict[self.inst_running_machine_dict[inst_id]].release_app(inst_id, app_res)
                
            self.inst_running_machine_dict[inst_id] = machine_id
            self.migrating_list.append('%s,%s' % (each_inst[0], each_inst[1]))
                
            # 根据可行解来更新信息素
            str_machine_id = str(machine_id)
            str_inst_id = str(machine_id)

            if (str_machine_id not in self.machine_item_pheromone):
                self.machine_item_pheromone[str_machine_id] = {}

            if (str_inst_id  not in self.machine_item_pheromone[str_machine_id]):
                self.machine_item_pheromone[str_machine_id][str_inst_id] = self.cur_def_pheromone 

            self.machine_item_pheromone[str_machine_id][str_inst_id] = self.cur_def_pheromone + 10000 / 7500


    def get_immigratable_machine_ex(self, inst_id, skipped_machine_id):
        immigratable_machine_list = []
        scores_set = set()
        
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
                if (increased_score not in scores_set):
                    scores_set.add(increased_score)
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
        
        if (len(immigratable_machine_list) > 0):
            return immigratable_machine_list
        
        scores_set.clear()
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
                if (increased_score not in scores_set):
                    scores_set.add(increased_score)
                    immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )
                    
        return immigratable_machine_list
    
    def adj_dispatch_dp(self):
        print_and_log('Ant(%d, %d) adj_dispatch_dp' % (self.iter_idx, self.ant_number))

        machine_start_idx = 0
        
        next_cost = self.sum_scores_of_machine()
        
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_machine_score(), reverse=False)

        while (machine_start_idx < MACHINE_CNT):
            if (machine_start_idx % 100 == 0):
                print(getCurrentTime(), 'Ant(%d, %d) adj_dispatch_ex handled %d machines\r' % 
                      (self.iter_idx, self.ant_number, machine_start_idx), end='')

            machine_id = self.sorted_machine_res[machine_start_idx][0]

            heavest_load_machine = self.machine_runing_info_dict[machine_id]
            
            if (len(heavest_load_machine.running_inst_list) == 0):
                machine_start_idx += 1
                continue

            inst_id = heavest_load_machine.running_inst_list[0]

            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            # 生成迁移方案的第一步， 以及迁入后增加的分数
            dp_immigrating_solution_list = self.get_immigratable_machine_ex(inst_id, machine_id)
            print(getCurrentTime(), 'Ant(%d, %d) machine %d, 1st / %d step solution is %d' % 
                  (self.iter_idx, self.ant_number, machine_id, len(heavest_load_machine.running_inst_list), len(dp_immigrating_solution_list)))
            
            if (len(dp_immigrating_solution_list) == 0):
                machine_start_idx += 1
                continue
            # 生成第 2 -> N 步的迁移方案
            for inst_idx in range(1, len(heavest_load_machine.running_inst_list)):
                print(getCurrentTime(), 'Ant(%d, %d) searching machine %d %d/%d\r' % 
                      (self.iter_idx, self.ant_number, machine_id, inst_idx,  len(heavest_load_machine.running_inst_list)), end='')
                each_inst = heavest_load_machine.running_inst_list[inst_idx]
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]

                one_step_solution = self.get_immigratable_machine_ex(each_inst, machine_id)
                dp_immigrating_solution_list = self.merge_migration_solution(dp_immigrating_solution_list, 
                                                                             one_step_solution,
                                                                             heavest_load_machine.get_machine_real_score())
                print(getCurrentTime(), 'Ant(%d, %d) machine %d, %d step solution is %d' % 
                      (self.iter_idx, self.ant_number, machine_id, inst_idx + 1, len(dp_immigrating_solution_list)))
                if (len(dp_immigrating_solution_list) == 0):                    
                    break

            # 在所有的迁移方案中找到迁入分数最小的
            min_solution_score = 1e9
            for idx, each_solution in enumerate(dp_immigrating_solution_list):
                if (each_solution[1] < min_solution_score):
                    min_solution_score = each_solution[1]
                    min_solution_idx = idx

            # 迁入所增加的分数至少要减少 1 分 , 否则不用再继续尝试
            if (heavest_load_machine.get_machine_real_score() - min_solution_score <= 1):
                print_and_log('Ant(%d, %d) migrating %s, running len %d, increased score %f > (real score %f), continue...' % \
                              (self.iter_idx, self.ant_number, heavest_load_machine.machine_res.machine_id, len(heavest_load_machine.running_inst_list), 
                               min_solution_score, heavest_load_machine.get_machine_real_score()))

                machine_start_idx += 1
                continue

            print_and_log('Ant(%d, %d) migrating solution for machine : %d, real score %f, increased %f, delta score %f, %s' % \
                          (self.iter_idx, self.ant_number, machine_id, heavest_load_machine.get_machine_real_score(), min_solution_score,
                           heavest_load_machine.get_machine_real_score() - min_solution_score, 
                           dp_immigrating_solution_list[min_solution_idx]))
            # 迁入
            for immigrating_machine, inst_list in dp_immigrating_solution_list[min_solution_idx][0].items():
                for each_inst in inst_list:
                    app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
    
                    # 迁入
                    if (not self.machine_runing_info_dict[immigrating_machine].dispatch_app(each_inst, app_res, self.app_constraint_dict)):
                        print_and_log("Ant(%d, %d) ERROR! Failed to immigrate inst %d to machine %d" % 
                                      (self.iter_idx, self.ant_number, each_inst, immigrating_machine))
                        return
                    
                    self.migrating_list.append('inst_%d,machine_%d' % (each_inst, immigrating_machine))
            
                    heavest_load_machine.release_app(each_inst, app_res) # 迁出 inst

            self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
            # 迁移之后重新计算得分, 仍然从可迁移的且得分最高的机器开始
            next_cost = self.sum_scores_of_machine()   

        print_and_log('Ant(%d, %d) leaving adj_dispatch_ex with next cost %f' % (self.iter_idx, self.ant_number, next_cost))
        return next_cost
    
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
    
    def dispatch_inst(self):
        total_inst = len(self.dispatchable_inst_list)
        
        total_inst = int(total_inst / 100)        
        self.dispatchable_inst_list = shuffle(self.dispatchable_inst_list[0:total_inst])
        
        part1_time = 0        
        part21_time = 0
        part22_time = 0
        part23_time = 0
        part11_time = 0
        part12_time = 0
        
        part_can = 0
        
        machine_id_list = [i for i in range(1, MACHINE_CNT + 1)]
        
        for i in range(total_inst):
            inst_id = self.dispatchable_inst_list[i]            
            str_inst_id = str(inst_id)

            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            # 在每台机器上计算启发式信息
            machine_heuristic_dict = {}
            cpu_mean_group_list = [0 for i in range(int(MAX_CPU/MAX_SCORE_DIFF + 1))]
            start_time = time.time()
#             machine_id_list = shuffle(machine_id_list)
            for machine_id in machine_id_list:
                part11_s = time.time()
                machine_running_res = self.machine_runing_info_dict[machine_id]
                if (machine_id == self.inst_running_machine_dict[inst_id]): 
                    continue
                
                part11_e = time.time()
                part11_time += part11_e - part11_s

                part12_s = time.time()
                # cpu 均值相近的机器启发式信息也相近，这里只保留 cpu 均值有一定距离的机器
                idx = machine_running_res.get_cpu_mean_idx()
                
                part12_e = time.time()
                part12_time += part12_e - part12_s

                if (cpu_mean_group_list[idx] == 0):
                    can_s = time.time()
                    can = machine_running_res.can_dispatch(app_res, self.app_constraint_dict)
                    can_e = time.time()
                    part_can += can_e - can_s
                    if (can):
                        heuristic = round(machine_running_res.get_heuristic(app_res), 2)
                        machine_heuristic_dict[machine_id] = heuristic
                        cpu_mean_group_list[idx] = 1

            end1_time = time.time()
            part1_time += end1_time - start_time

            if (len(machine_heuristic_dict) == 0):
                print("Ant(%d, %d) inst %d is not migratable on machine %d \r" % \
                              (self.iter_idx, self.ant_number, inst_id, self.inst_running_machine_dict[inst_id]), end='')
                continue

            # 各个 machine 被选中的概率
            total_proba = 0
            machine_proba_dict = {}
    
            start21_time = time.time()

            max_proba = 0

            for machine_id, machine_heuristic in machine_heuristic_dict.items():
                str_machine_id = str(machine_id)
                machine_running_res = self.machine_runing_info_dict[machine_id]
    
                pheromone = self.cur_def_pheromone
    
                if (str_machine_id in self.machine_item_pheromone and str_inst_id in self.machine_item_pheromone[str_machine_id]):
                    pheromone = self.machine_item_pheromone[str_machine_id][str_inst_id]
    
                machine_proba_dict[machine_id] = pow(pheromone, ALPHA) * pow(machine_heuristic, BETA)
                total_proba += machine_proba_dict[machine_id]
                if (max_proba < machine_proba_dict[machine_id]):
                    max_proba = machine_proba_dict[machine_id]
                    max_proba_machine = machine_id

            end21_time = time.time()
            part21_time += end21_time - start21_time

            Q = 0.5
            if (random.uniform(0.0, 1.0) < Q):
                selected_machine = max_proba_machine
            else:
                selected_machine = None
                random_proba = random.uniform(0.0, total_proba)
                for machine_id, machine_proba in machine_proba_dict.items():
                    random_proba -= machine_proba
                    if (random_proba <= 0.0001):
                        selected_machine = machine_id
                        break
        
                if (selected_machine is None):
                    if (len(machine_proba_dict) == 1):
                        rand_machine = 0
                    else:
                        rand_machine = random.randint(0, len(machine_proba_dict) - 1)
                    tmp = list(machine_proba_dict.keys())
                    selected_machine = tmp[rand_machine]
            end22_time = time.time()
            part22_time += end22_time - end21_time

            if (self.machine_runing_info_dict[selected_machine].dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                if (inst_id in self.inst_running_machine_dict):
                    self.machine_runing_info_dict[self.inst_running_machine_dict[inst_id]].release_app(inst_id, app_res)

                self.inst_running_machine_dict[inst_id] = selected_machine
                self.migrating_list.append('inst_%d,machine_%d' % (inst_id, selected_machine))
            else:
                print_and_log("ERROR! dispatch_inst() Failed to immigrate inst %d to machine %d" % (inst_id, selected_machine))
                exit(-1)
            
            end23_time = time.time()
            part23_time += end23_time - end22_time

            if (i % 100 == 0):
                print(getCurrentTime(), "Ant(%d, %d) %d / %d inst handled, part1 %d, part11 %d, part12 %d, part21 %d, part22 %d, part23 %d, part_can %d \r" % \
                      (self.iter_idx, self.ant_number, i, total_inst, part1_time, part11_time, part12_time,
                       part21_time, part22_time, part23_time, part_can), end='')

        return 
    
    def sum_scores_of_machine(self):
        scores = 0
        for machine_id, machine_running_res in self.machine_runing_info_dict.items():
            scores += machine_running_res.get_machine_real_score()
            
        return scores   
     
    def update_pheromone(self):
        
        scors = self.sum_scores_of_machine()
        ant_pheromone_dict = {}
        for i in range(len(self.dispatch_path) - 2):
            machine_a = self.dispatch_path[i][1]
            machine_b = self.dispatch_path[i + 2][1]
            if (machine_a not in ant_pheromone_dict):
                ant_pheromone_dict[machine_a] = {}

            if (machine_b not in ant_pheromone_dict[machine_a]):
                ant_pheromone_dict[machine_a][machine_b] = 0.0

            # 更新 ant 的信息素， 得分越少， 信息素越多
            ant_pheromone_dict[machine_a][machine_b] += 100000 / scors
        
        return

    def output_ant_solution(self):
        filename = 'iter_%d_ant_%d.csv' % (self.iter_idx, self.ant_number)
        output_file = open(r'%s\..\output\%s\%s' % (runningPath, data_set, filename), 'w')
        print(getCurrentTime(), 'writing output file %s' % filename)

        for each_migrating in self.migrating_list:
            output_file.write('%s\n' % (each_migrating))

        output_file.close()
            
if __name__ == '__main__':
    iter_idx = int(sys.argv[1].split("=")[1])
    ant_number = int(sys.argv[2].split("=")[1])
    
    ant = Ant(iter_idx, ant_number)  
    ant.dispatch_inst()
    ant.output_ant_solution()
    
    
    