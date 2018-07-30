'''
Created on Jul 20, 2018

@author: Heng.Zhang
'''
import logging
import csv
import os
import datetime
import json
import subprocess  

import copy
from global_param import *
from MachineRunningInfo import *
from Ant import *

# Ant Colony System
class ACS(object):
    def __init__(self):
        self.evaporating_rate = 0.4 # 信息素挥发率
        self.cur_def_pheromone = 10000 / 6100
        self.max_pheromone = self.cur_def_pheromone
        self.min_pheromon = 0.5 * self.max_pheromone
        self.machine_item_pheromone = {}

        self.global_min_ant_dispatch = None
        self.global_min_ant_score = 1e9
        self.global_min_ant_migration_list = []
        
        print(getCurrentTime(), 'loading app_resources.csv...')
        self.app_res_dict = [0 for x in range(APP_CNT + 1)]
        app_res_csv = csv.reader(open(r'%s\..\input\%s\app_resources.csv' % (runningPath, data_set), 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0])
            self.app_res_dict[app_id] = AppRes(each_app)
            
#         machine_res_csv = csv.reader(open(r'%s\..\input\%s\machine_resources_reverse.csv' % (runningPath, data_set), 'r'))
        machine_res_csv = csv.reader(open(r'%s\..\input\%s\machine_resources.csv' % (runningPath, data_set), 'r'))
        
        self.machine_runing_info_dict = {}
        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 

        # 初始化，但不解决约束冲突, 约束冲突在 Ant 中解决
        print(getCurrentTime(), 'loading instance_deploy.csv...')
        self.inst_running_machine_dict = {}
        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s\..\input\%s\instance_deploy.csv' % (runningPath, data_set), 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            app_id = int(each_inst[1])
            self.inst_app_dict[inst_id] = app_id
            if (len(each_inst[2]) > 0):
                machine_id = int(each_inst[2])
                self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)
                self.inst_running_machine_dict[inst_id] = machine_id

        self.global_min_iter = 0
        self.global_min_ant = 0
        self.global_min_score = 1e9
            

    def submiteOneSubProcess(self, iter_idx, ant_number, runningSubProcesses):
        cmdLine = "python Ant.py iter=%d number=%d" % (iter_idx, ant_number)
        sub = subprocess.Popen(cmdLine, shell=True)
        runningSubProcesses[ant_number] = sub
        print_and_log("running cmd line: %s" % cmdLine)
        time.sleep(1)
        return
    

    def waitSubprocesses(self, runningSubProcesses):
        for ant_number in runningSubProcesses:
            sub = runningSubProcesses[ant_number]
            ret = subprocess.Popen.poll(sub)
            if ret == 0:
                runningSubProcesses.pop(ant_number)
                return ant_number
            elif ret is None:
                time.sleep(1) # running
            else:
                runningSubProcesses.pop(ant_number)
                return ant_number

        return -1
    
    # 在不解决约束冲突的基础上加载 Ant 的输出， Ant 会解决约束冲突 
    def dispatch_inst(self, iter_idx, ant_number):
        ant_output_filename = r'%s\..\output\%s\iter_%d_ant_%d.csv' % (runningPath, data_set, iter_idx, ant_number)
        print(getCurrentTime(), 'loading ', ant_output_filename)

        ant_machine_runing_info_dict = copy.deepcopy(self.machine_runing_info_dict)
        ant_inst_running_machine_dict = copy.deepcopy(self.inst_running_machine_dict)

        ant_output_csv = csv.reader(open(ant_output_filename, 'r'))
        for each_inst in ant_output_csv:
            inst_id = int(each_inst[0].split('_')[1])
            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
            machine_id = int(each_inst[1].split('_')[1])
            ant_machine_runing_info_dict[machine_id].update_machine_res(inst_id, app_res, DISPATCH_RATIO)
            if (inst_id in ant_inst_running_machine_dict):
                ant_machine_runing_info_dict[ant_inst_running_machine_dict[inst_id]].release_app(inst_id, app_res)
            
            ant_inst_running_machine_dict[inst_id] = machine_id

        return ant_machine_runing_info_dict

    def sum_scores_of_machine(self, ant_machine_runing_info_dict):
        scores = 0
        for machine_id, machine_running_res in ant_machine_runing_info_dict.items():
            scores += machine_running_res.get_machine_real_score()

        return scores

    def ant_search(self):
        ant_cnt = 10
        iteration_cnt = 5

        for iter_idx in range(iteration_cnt):

            runningSubProcesses = {}

            self.machine_item_pheromone['def'] = self.cur_def_pheromone
            with open(r'%s\..\input\%s\machine_item_pheromone.txt' % (runningPath, data_set), 'w') as pheromone_file:
                json.dump(self.machine_item_pheromone, pheromone_file)

            for ant_number in range(ant_cnt):
                self.submiteOneSubProcess(iter_idx, ant_number, runningSubProcesses)
  
            while True:
                if (len(runningSubProcesses) == 0):
                    print_and_log("iter %d, All of ant finished" % (iter_idx))
                    break
  
                ant_number = self.waitSubprocesses(runningSubProcesses)
                if (ant_number >= 0):
                    print_and_log('Iter %d Ant %d finished, %d ants are running' % (iter_idx, ant_number, len(runningSubProcesses)))
  
            # 得到得分最低的 ant
            cycle_min_ant_score = 1e9;

            for ant_number in range(ant_cnt):
                ant_machine_runing_info_dict = self.dispatch_inst(iter_idx, ant_number)
                ant_score = self.sum_scores_of_machine(ant_machine_runing_info_dict)
                if (ant_score < cycle_min_ant_score):
                    cycle_min_ant_score = ant_score
                    cycle_min_ant_dispatch_dict = ant_machine_runing_info_dict
                    cycle_min_ant_number = ant_number

            # 用本次迭代 得分最低的蚂蚁的分配方案来更新信息素， 这样可以使更多的解元素有机会获得信息素的增强，
            # 避免陷入较差解的风险
            if (cycle_min_ant_score < self.global_min_score):
                self.global_min_score = cycle_min_ant_score
                self.global_min_iter = iter_idx
                self.global_min_ant = cycle_min_ant_number

            self.cur_def_pheromone *= 1 - self.evaporating_rate # 每次迭代都会导致信息素挥发
            print_and_log('iteration %d min score %f on, global min %f, update def pheromone to %f' % 
                          (iter_idx, cycle_min_ant_score, self.global_min_score, self.cur_def_pheromone))

            for machine_id, machine_running_res in cycle_min_ant_dispatch_dict.items():
                if (machine_id not in self.machine_item_pheromone):
                    self.machine_item_pheromone[machine_id] = {}

                for inst_id in machine_running_res.running_inst_list:
                    if (inst_id  in self.machine_item_pheromone[machine_id]):
                        self.machine_item_pheromone[machine_id][inst_id] = \
                            (1 - self.evaporating_rate) * self.machine_item_pheromone[machine_id][inst_id] + 10000 / cycle_min_ant_score
                    else:
                        self.machine_item_pheromone[machine_id][inst_id] = self.cur_def_pheromone + 10000 / cycle_min_ant_score
        return

    def output_submition(self):
        print_and_log("Globa min iter %d, global min ant %d" % (self.global_min_iter, self.global_min_ant))
#         filename = 'submit_%s.csv' % datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
#         output_file = open(r'%s\..\output\%s\%s' % (runningPath, data_set, filename), 'w')
#         print(getCurrentTime(), 'writing output file %s' % filename)
# 
#         for each_migrating in self.migrating_list:
#             output_file.write('%s\n' % (each_migrating))
# 
#         for each_migrating in self.global_min_ant_migration_list:
#             output_file.write('%s\n' % (each_migrating))
# 
#         output_file.close()
        
if __name__ == '__main__':
    acs = ACS()
    acs.ant_search()
    acs.output_submition()