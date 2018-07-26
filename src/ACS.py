'''
Created on Jul 20, 2018

@author: Heng.Zhang
'''
import logging
import csv
import os
import datetime

import copy
from global_param import *
from MachineRunningInfo import *
from Ant import *

# Ant Colony System
class ACS(object):
    def __init__(self):
        log_file = r'%s\..\log\ant_colony.txt' % runningPath
        
        self.print_all_scores = False

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')

        #  记录 machine 的运行信息， 包括 cpu 使用量,  cpu 使用率 = cpu 使用量 / cpu 容量， d, p, m, pm, app list
        print(getCurrentTime(), 'loading machine_resources.csv...')
        self.machine_runing_info_dict = {} 
        machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources_reverse.csv' % runningPath, 'r'))
#         machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources.csv' % runningPath, 'r'))

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

        print(getCurrentTime(), 'loading app_interference.csv...')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.migrating_list = []
        
#         inited_filename = r'%s\..\input\initialized_deploy.csv' % (runningPath)
#         print(getCurrentTime(), 'loading initialized_deploy.csv...')
#         self.inst_app_dict = {}
#         inst_disp_csv = csv.reader(open(inited_filename, 'r'))
#         for each_inst in inst_disp_csv:
#             inst_id = int(each_inst[0].split('_')[1])
#             machine_id = int(each_inst[1].split('_')[1])
#             self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[self.inst_app_dict[app_id]], DISPATCH_RATIO)
#             self.migrating_list.append(each_app)

        print(getCurrentTime(), 'loading instance_deploy.csv...')
        self.inst_app_dict = {}
        self.dispatchable_inst_set = set()
        inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            app_id = int(each_inst[1])
            self.inst_app_dict[inst_id] = app_id
            if (len(each_inst[2]) == 0):
                self.dispatchable_inst_set.add(inst_id) # 没有初始化的 inst， 需要分配

        self.evaporating_rate = 0.4 # 信息素挥发率
        self.cur_def_pheromone = 10000 / 6100
        self.max_pheromone = self.cur_def_pheromone
        self.min_pheromon = 0.5 * self.max_pheromone
        self.machine_item_pheromone = {}

        self.global_min_ant_dispatch = None
        self.global_min_ant_score = 1e9
        self.global_min_ant_migration_list = []
        
    def ant_search(self):
        ant_cnt = 1
        iteration_cnt = 1
        
        for iter_idx in range(iteration_cnt):
            # 每次迭代时生成新的蚁群
            ant_colony = [Ant(self.app_res_dict, 
                              self.inst_app_dict, 
                              self.app_constraint_dict, 
                              self.machine_runing_info_dict, 
                              self.machine_item_pheromone, 
                              self.cur_def_pheromone) for i in range(ant_cnt)]

            for inst_cnt, inst_id in enumerate(self.dispatchable_inst_set):
                if (inst_cnt % 10 == 0):
                    print(getCurrentTime(), "iteration %d handled %d inst\r" % (iter_idx, inst_cnt), end='')
                for ant_idx in range(ant_cnt):
                    ant_colony[ant_idx].dispatch_inst(inst_id)

            # 得到得分最低的 ant
            cycle_min_ant_score = 1e9;
            for ant_idx in range(ant_cnt):
                ant_score = ant_colony[ant_idx].sum_scores_of_machine()
                if (ant_score < cycle_min_ant_score):
                    cycle_min_ant_score = ant_score
                    min_ant_idx = ant_idx

            # 用本次迭代 得分最低的蚂蚁的分配方案来更新信息素， 这样可以使更多的解元素有机会获得信息素的增强，
            # 避免陷入较差解的风险
            if (self.global_min_ant_score < cycle_min_ant_score):
                self.global_min_ant_score = cycle_min_ant_score
                self.global_min_ant_dispatch = copy.deepcopy(ant_colony[min_ant_idx].machine_runing_info_dict)
                self.global_min_ant_migration_list = ant_colony[min_ant_idx].migrating_list.copy()

                pheromone_update_dispatch = self.global_min_ant_dispatch                
            else:
                pheromone_update_dispatch = ant_colony[min_ant_idx].machine_runing_info_dict

            self.cur_def_pheromone *= 1 - self.evaporating_rate # 每次迭代都会导致信息素挥发
            print_and_log('iterator %d min score %f on ant %d, update def pheromone to %f' % 
                          (iter_idx, cycle_min_ant_score, min_ant_idx, self.cur_def_pheromone))

            for machine_id, machine_running_res in pheromone_update_dispatch.items():
                if (machine_id not in self.machine_item_pheromone):
                    self.machine_item_pheromone[machine_id] = {}

                for inst_id in machine_running_res.running_inst_list:
                    # 只考虑那些使用 ant 算法来分发的 inst
                    if (inst_id not in self.dispatchable_inst_set):
                        continue

                    if (inst_id not in self.machine_item_pheromone[machine_id]):
                        self.machine_item_pheromone[machine_id][inst_id] = self.cur_def_pheromone + 10000 / self.global_min_ant_dispatch

        return

    def output_submition(self):
        filename = 'submit_%s.csv' % datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = open(r'%s\..\output\%s' % (runningPath, filename), 'w')
        print(getCurrentTime(), 'writing output file %s' % filename)

        for each_migrating in self.migrating_list:
            output_file.write('%s\n' % (each_migrating))
            
        for each_migrating in self.global_min_ant_migration_list:
            output_file.write('%s\n' % (each_migrating))

        output_file.close()
        
if __name__ == '__main__':
    acs = ACS()
    acs.ant_search()
    acs.output_submition()