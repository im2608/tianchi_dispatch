#coding=utf-8
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
    def __init__(self, iter_idx, ant_number, inst_start):
        self.iter_idx = iter_idx
        self.ant_number = ant_number
        self.inst_start = inst_start
        
        log_file = r'%s/../log/iter_%d_ant_%d.txt' % (runningPath, iter_idx, ant_number)

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
        machine_res_csv = csv.reader(open(r'%s/../input/%s/machine_resources.csv' % (runningPath, data_set), 'r'))

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
        
        self.inst_running_machine_dict = {}
        self.migrating_list = []        
        print(getCurrentTime(), 'loading instance_deploy.csv...')
        self.inst_app_dict = {}
        self.dispatchable_inst_list = []
        inst_app_csv = csv.reader(open(r'%s/../input/%s/instance_deploy.csv' % (runningPath, data_set), 'r'))
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
        
        self.load_pheromone()

        # 加载一个可行解，在它的基础上进行优化, 并根据可行解来更新信息素
        if (self.inst_start == 0):
            inited_filename = r'%s/../input/%s/feasible_solution.csv' % (runningPath, data_set)
        else:
            inited_filename = r'%s/../input/%s/feasible_solution_ant.csv' % (runningPath, data_set)

        print(getCurrentTime(), 'loading a solution %s' % inited_filename)

        inst_disp_csv = csv.reader(open(inited_filename, 'r'))
        for each_inst in inst_disp_csv:
            inst_id = int(each_inst[0].split('_')[1])
            machine_id = int(each_inst[1].split('_')[1])

            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
            if (not self.machine_runing_info_dict[machine_id].dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                print_and_log("ERROR! Ant(%d, %d, %d) Failed to immigrate inst %d to machine %d" % 
                              (self.iter_idx, self.ant_number, self.inst_start, inst_id, machine_id))
                exit(-1)

            str_machine_id = str(machine_id)
            str_inst_id = str(machine_id)

            if (inst_id in self.inst_running_machine_dict):
                migrating_machine_id = self.inst_running_machine_dict[inst_id]
                self.machine_runing_info_dict[migrating_machine_id].release_app(inst_id, app_res)

                # 从 migrating_machine_id 迁出，相应地删除信息素
                str_migrating_machine_id = str(migrating_machine_id)
                if (str_migrating_machine_id in self.machine_item_pheromone and str_inst_id in self.machine_item_pheromone[str_migrating_machine_id]):
                    self.machine_item_pheromone[str_migrating_machine_id].pop(str_inst_id)
                    if (len(self.machine_item_pheromone[str_migrating_machine_id]) == 0):
                        self.machine_item_pheromone.pop(str_migrating_machine_id)

            # 迁入 machine_id ， 相应地增加信息素
            self.inst_running_machine_dict[inst_id] = machine_id
            self.migrating_list.append('%s,%s' % (each_inst[0], each_inst[1]))

            if (str_machine_id not in self.machine_item_pheromone):
                self.machine_item_pheromone[str_machine_id] = {}

            if (str_inst_id  not in self.machine_item_pheromone[str_machine_id]):
                self.machine_item_pheromone[str_machine_id][str_inst_id] = self.cur_def_pheromone 

#             self.machine_item_pheromone[str_machine_id][str_inst_id] = self.cur_def_pheromone + 10000 / 7500

        print(getCurrentTime(), 'calculate_migrating_delta_score()..')
        machine_id_list = [i for i in range(1, MACHINE_CNT + 1)]
        for machine_id in machine_id_list:
            self.machine_runing_info_dict[machine_id].calculate_migrating_delta_score(self.app_res_dict)

        print(getCurrentTime(), 'init() done..')


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
    
    
    def dispatch_inst(self):
        total_inst = len(self.dispatchable_inst_list)
        
#         total_inst = int(total_inst / 100)        
#         self.dispatchable_inst_list = shuffle(self.dispatchable_inst_list[0:total_inst])
        
        part1_time = 0        
        part21_time = 0
        part22_time = 0
        part23_time = 0
        part11_time = 0
        part12_time = 0
        
        part_can = 0
        
        machine_id_list = [i for i in range(1, MACHINE_CNT + 1)]
        inst_end = self.inst_start + int(total_inst / 100)
        for i in range(self.inst_start, inst_end):
            inst_id = self.dispatchable_inst_list[i]            
            str_inst_id = str(inst_id)

            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            # 在每台机器上计算启发式信息
            machine_heuristic_dict = {}
            cpu_mean_group_list = [0 for i in range(int(MAX_CPU/MAX_SCORE_DIFF + 1))]
            start_time = time.time()
            
            for machine_id in machine_id_list:
                
                machine_running_res = self.machine_runing_info_dict[machine_id]
                if (machine_id == self.inst_running_machine_dict[inst_id]): 
                    continue

                # cpu 均值相近的机器启发式信息也相近，这里只保留 cpu 均值有一定距离的机器
                idx = machine_running_res.get_cpu_mean_idx()

                migrating_delta_score = 1e9
                if (inst_id in self.inst_running_machine_dict):
                    part11_s = time.time()
                    migrating_delta_score = self.machine_runing_info_dict[self.inst_running_machine_dict[inst_id]].migrating_delta_score_ex(app_res)
                    part11_e = time.time()
                    part11_time += part11_e - part11_s

                if (cpu_mean_group_list[idx] == 0):
                    can_s = time.time()
                    can = machine_running_res.can_dispatch(app_res, self.app_constraint_dict)
                    can_e = time.time()
                    part_can += can_e - can_s
                    if (can):
                        part12_s = time.time()
                        immigrate_deleta_score = machine_running_res.immigrating_delta_score(app_res)
                        part12_e = time.time()
                        part12_time += part12_e - part12_s

                        if (migrating_delta_score > immigrate_deleta_score):
                            heuristic = round(machine_running_res.get_heuristic(app_res), 2)
                            machine_heuristic_dict[machine_id] = heuristic
                            cpu_mean_group_list[idx] = 1

            end1_time = time.time()
            part1_time += end1_time - start_time

            if (len(machine_heuristic_dict) == 0):
#                 print("Ant(%d, %d) inst %d is not migratable on machine %d \r" % \
#                               (self.iter_idx, self.ant_number, inst_id, self.inst_running_machine_dict[inst_id]), end='')
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
                    immigrating_machine_res = self.machine_runing_info_dict[self.inst_running_machine_dict[inst_id]] 
                    immigrating_machine_res.release_app(inst_id, app_res)
                    immigrating_machine_res.calculate_migrating_delta_score(self.app_res_dict)

                self.inst_running_machine_dict[inst_id] = selected_machine
                self.migrating_list.append('inst_%d,machine_%d' % (inst_id, selected_machine))
            else:
                print_and_log("ERROR! dispatch_inst() Failed to immigrate inst %d to machine %d" % (inst_id, selected_machine))
                exit(-1)

            end23_time = time.time()
            part23_time += end23_time - end22_time

            if (i % 100 == 0):
                print(getCurrentTime(), "Ant(%d, %d, %d) %d / %d inst handled, part1 %d, part11 %d, part12 %d, part21 %d, part22 %d, part23 %d, part_can %d \r" % \
                      (self.iter_idx, self.ant_number, self.inst_start, i, inst_end, part1_time, part11_time, part12_time,
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
    
    def load_pheromone(self):
        self.cur_def_pheromone = 1 / 7280
        self.machine_item_pheromone = {}
        pheromone_file_name = r'%s/../input/%s/machine_item_pheromone.txt' % (runningPath, data_set)
        print(getCurrentTime(), 'loading machine_item_pheromone')
        if (os.path.exists(pheromone_file_name)):
            with open(pheromone_file_name , 'r') as pheromone_file:
                pheromone_csv = csv.reader(pheromone_file)
                for each_pheromone in pheromone_csv:
                    machine_id = int(each_pheromone[0])
                    inst_id = int(each_pheromone[1]) 
                    pheromone = float(each_pheromone[2])
                    
                    if (machine_id not in self.machine_item_pheromone):
                        self.machine_item_pheromone[machine_id] = {}
                        
                    self.machine_item_pheromone[machine_id][inst_id] = pheromone
        

    def output_ant_solution(self):
        filename = 'iter_%d_ant_%d.csv' % (self.iter_idx, self.ant_number)
        output_file = open(r'%s/../output/%s/%s' % (runningPath, data_set, filename), 'w')
        print(getCurrentTime(), 'writing output file %s' % filename)

        for each_migrating in self.migrating_list:
            output_file.write('%s\n' % (each_migrating))

        output_file.close()
    
def test_proba():
    proba = 9.8242
    each_proba = [3.456, 4.5678, 1.8004]
    proba_dict = {0:0, 1:0, 2:0}
    for i in range(10000):
        random_proba = random.uniform(0.0, proba)
        for i in range(len(each_proba)):
            random_proba -= each_proba[i]
            if (random_proba <= 0):
                proba_dict[i] += 1
                break
                
    print(proba_dict)

if __name__ == '__main__':
#     test_proba()
#     exit(1)
    
    iter_idx = int(sys.argv[1].split("=")[1])
    ant_number = int(sys.argv[2].split("=")[1])
    inst_start = int(sys.argv[3].split("=")[1])
    
    ant = Ant(iter_idx, ant_number, inst_start)  
    ant.dispatch_inst()
    ant.output_ant_solution()
    
    
    