'''
Created on Jul 19, 2018

@author: Heng.Zhang
'''
from global_param import *
import random


class Ant(object):
    def __init__(self, app_res_dict, inst_app_dict, app_constraint_dict, machine_runing_info_dict, current_machine, current_dispatchs):
        self.dispatch = []
        self.app_res_dict = app_res_dict
        self.inst_app_dict = inst_app_dict
        self.app_constraint_dict = app_constraint_dict
        self.machine_runing_info_dict = machine_runing_info_dict.copy()
        self.current_machine = current_machine
        self.current_dispatchs = current_dispatchs.copy()
        self.dispatch_path = []

    def chooseNextMachine(self, inst_id):
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        
        machine_proba_dict = {} # 各个 machine 被选中的概率
        total_proba = 0.0
        for machine_id, machine_running_res in self.machine_runing_info_dict.items():
            if (machine_running_res.can_dispatch(app_res, self.app_constraint_dict)):
                heuristic_score = 100 / machine_running_res.immigrating_delta_score(app_res)  # 以 100 / 迁入后增加的分数 作为启发信息
                pheromone = g_cur_def_pheromone
                if (self.current_machine in g_pheromone_dict and machine_id in g_pheromone_dict[self.current_machine]):
                    pheromone = g_pheromone_dict[self.current_machine][machine_id]

                machine_proba_dict[machine_id] = pow(pheromone, ALPHA) * pow(heuristic_score, BETA)
                total_proba += machine_proba_dict[machine_id]

        selected_machine = None
        random_proba = random.uniform(0.0, total_proba)
        for machine_id, machine_proba in machine_proba_dict.items():
            random_proba -= machine_proba
            if (random_proba < 0):
                selected_machine = machine_id
                
        if (selected_machine is None):
            rand_machine = random.randint(len(machine_proba_dict))
            tmp = list(machine_proba_dict.keys())
            selected_machine = tmp[rand_machine]
            
        if (not self.machine_runing_info_dict[selected_machine].dispatch_app()):
            print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (inst_id, selected_machine))
            exit(-1)

        self.dispatch_path.append((inst_id, selected_machine))
        
        self.current_machine = selected_machine

        return selected_machine
    
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
                
        
        
        