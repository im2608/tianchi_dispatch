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

# from functools import reduce

class AdjustDispatch(object):
    def __init__(self):
        log_file = r'%s\..\log\cost.log' % runningPath
    
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')
        
        self.machine_runing_info_dict = {} 
        print(getCurrentTime(), 'loading machine_resources.csv')
        machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources.csv' % runningPath, 'r'))
        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 
        
        print(getCurrentTime(), 'loading app_resources.csv')
        self.app_res_dict = {}
        app_res_csv = csv.reader(open(r'%s\..\input\app_resources.csv' % runningPath, 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0])
            self.app_res_dict[app_id] = AppRes(each_app)

        print(getCurrentTime(), 'loading instance_deploy.csv')
        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0])
            self.inst_app_dict[inst_id] = int(each_inst[1])

        print(getCurrentTime(), 'loading app_interference.csv')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])
    
        # inst 运行在哪台机器上
        self.insts_running_machine_dict = dict()
    
        self.submit_filename = 'submit6027'
        print(getCurrentTime(), 'loading submit6027.csv')        
        app_dispatch_csv = csv.reader(open(r'%s\..\output\%s.csv' % (runningPath, self.submit_filename), 'r'))
        for each_dispatch in app_dispatch_csv:
            self.insts_running_machine_dict[each_dispatch[0]] = each_dispatch[1]       

        self.cost = 0 
        return

    def sorte_machine(self):
        self.sorted_machine_cost = sorted(self.machine_runing_info_dict.items(), key = lambda d : d[1].get_machine_real_score(), reverse = True)
        
    # 从得分最低的机器上迁出所有的 inst, 如果增加的分数 < 98, 则可行 
    def adj_dispatch_reverse(self):

        for machine_idx in range(len(self.sorted_machine_cost) - 1, -1, -1):
            machine_id = self.sorted_machine_cost[machine_idx][0]
            lightest_load_machine = self.machine_runing_info_dict[machine_id]
            if (len(lightest_load_machine.running_inst_list) > 0):
                break

        for idx in range(machine_idx, -1, -1):
            machine_id = self.sorted_machine_cost[idx][0]
            lightest_load_machine = self.machine_runing_info_dict[machine_id]
            
            migrating_machine_dict = {}
            for each_inst in lightest_load_machine.running_inst_list:
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
                min_delta_score = 1e9 # 将 inst 迁出后减少的分数 - 将 inst 迁入后增加的分数, 找到最大的 max delta
        
                for i in range(machine_idx):
                    machine_id = self.sorted_machine_cost[i][0]
    
                    heavy_load_machine = self.machine_runing_info_dict[machine_id]            
        
                    if (heavy_load_machine.can_dispatch(app_res, self.app_constraint_dict)):
                        # 将 inst 迁入后增加的分数
                        increased_score = heavy_load_machine.immigrating_delta_score(self.app_res_dict[self.inst_app_dict[each_inst]])
                        if (increased_score < min_delta_score):
                            min_delta_score = increased_score
                            migrating_machine_dict[each_inst] = (machine_id, increased_score)
                            # 迁入后没有增加分数， 最好的结果， 无需继续查找
                            if (increased_score == 0):
                                break
            sum_increated_score = 0
            for each_inst, (machine_id, score) in migrating_machine_dict.items():
                sum_increated_score += score

            # 增加的分数 > 98, 不可行 
            if (sum_increated_score > 98 or len(migrating_machine_dict) == 0):
                print(getCurrentTime(), 'migrating %s, running len %d, increased score %f > 98, continue...' % \
                      (lightest_load_machine.machine_res.machine_id, len(lightest_load_machine.running_inst_list), sum_increated_score ))
                continue
    
            for each_inst, (migrating_machine, score) in migrating_machine_dict.items():
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
                
                # 迁入
                self.machine_runing_info_dict[migrating_machine].dispatch_app(each_inst, app_res, self.app_constraint_dict)
        
                lightest_load_machine.release_app(each_inst, app_res) # 迁出 inst
                self.insts_running_machine_dict[each_inst] = migrating_machine # 更新 running dit
        
            self.sorted_machine_cost = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
            
            # 迁移之后重新计算得分，分数降低的话则继续尝试
            next_cost = 0
            for machine_id, machine_running_res in self.sorted_machine_cost:
                next_cost += machine_running_res.get_machine_real_score()        
            
            print(getCurrentTime(), 'migrating %s, increased score %f, next_cost %f' % \
                  (lightest_load_machine.machine_res.machine_id, sum_increated_score, next_cost))
    
    def adj_dispatch(self):
        # 得分最高的机器
        machine_id, _ = self.sorted_machine_cost[0]

#         while (self.machine_runing_info_dict[machine_id].get_machine_real_score() > 98):
#             machine_id, _ = self.sorted_machine_cost[0]

        # 将 inst 迁出后减少的分数
        decreased_score = 0
        
        # self.sorted_machine_cost 是个tuple，不可修改
        heavest_load_machine = self.machine_runing_info_dict[machine_id]
        
        # 尝试迁出全部的 inst 效果不好
        # 找到一个得分最高的 inst
        for each_inst in heavest_load_machine.running_inst_list:
            app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
            insts_score = heavest_load_machine.migrating_delta_score(self.app_res_dict[self.inst_app_dict[each_inst]])
            if (decreased_score < insts_score):
                decreased_score = insts_score
                immigrate_inst = each_inst

        max_delta_score = 0 # 将 inst 迁出后减少的分数 - 将 inst 迁入后增加的分数, 找到最大的 max delta

        for i in  range(len(self.sorted_machine_cost)-1, 0, -1):
            machine_id, machine_res = self.sorted_machine_cost[i]
            lightest_load_machine = self.machine_runing_info_dict[machine_id]            

            if (lightest_load_machine.can_dispatch(app_res, self.app_constraint_dict)):
                # 将 inst 迁入后增加的分数
                increased_score = lightest_load_machine.immigrating_delta_score(self.app_res_dict[self.inst_app_dict[immigrate_inst]])
                if (max_delta_score < decreased_score - increased_score):
                    max_delta_score = decreased_score - increased_score
                    migrating_machine = machine_id
                    # 迁入后没有增加分数， 最好的结果， 无需继续查找
                    if (increased_score == 0):
                        break

        if (max_delta_score == 0):
            return self.cost

        # 迁入
        self.machine_runing_info_dict[migrating_machine].dispatch_app(immigrate_inst, \
                self.app_res_dict[self.inst_app_dict[immigrate_inst]], \
                self.app_constraint_dict)

        heavest_load_machine.release_app(immigrate_inst, self.app_res_dict[self.inst_app_dict[immigrate_inst]]) # 迁出 inst
        self.insts_running_machine_dict[immigrate_inst] = machine_id # 更新 running dit

        self.sorted_machine_cost = sorted(self.machine_runing_info_dict.items(), \
                                     key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
    
        # 迁移之后重新计算得分，分数降低的话则继续尝试
        next_cost = 0
        for machine_id, machine_running_res in self.sorted_machine_cost:
            next_cost += machine_running_res.get_machine_real_score()        
        
        return next_cost
    
    def calculate_cost_score(self):
    
        for inst_id, machine_id in self.insts_running_machine_dict.items():
            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
    
            # 将 inst 分发到该机器上并计算分数
            machine_running_res = self.machine_runing_info_dict[machine_id]
            if (not machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
                print('%s failed to dispatch %s, running inst list %s' % \
                      machine_id, inst_id, machine_running_res.running_inst_list)
                return
            
        self.sorte_machine()
    
        # 得分从高到低排序        
        for machine_id, machine_running_res in self.sorted_machine_cost:
            self.cost += machine_running_res.get_machine_real_score()
            logging.info('%s,%f' % (machine_id, machine_running_res.get_machine_real_score()))
            
        print(getCurrentTime(), 'cost of %s is %f' % (self.submit_filename, self.cost))
        
        if (self.sorted_machine_cost[-1][1].get_machine_real_score() > 98):
            return self.cost;
        
        print(getCurrentTime(), 'optimizing for L -> H')
        self.adj_dispatch_reverse()        

        print(getCurrentTime(), 'optimizing for H -> L')
        next_cost = self.adj_dispatch()
        while (next_cost < self.cost):
            print(getCurrentTime(), 'cost is optimized from %f to %f' % (self.cost, next_cost))
            self.cost = next_cost
            next_cost = self.adj_dispatch()

        with open(r'%s\..\output\%s_optimized.csv' % (runningPath, self.submit_filename), 'w') as output_file:
            for inst_id, machine_id in self.insts_running_machine_dict.items():
                output_file.write('%s,%s\r' % (inst_id, machine_id))
        
        with open(r'%s\..\output\%s_cost_optimized.csv' % (runningPath, self.submit_filename), 'w') as output_file:
            for machine_id, machine_running_res in self.sorted_machine_cost:    
                output_file.write('%s,%f\r' % (machine_id, machine_running_res.get_machine_real_score()))
    
        print(getCurrentTime(), 'finla cost is %f / %f' % (self.cost, self.cost / SLICE_CNT))
        
        return self.cost / SLICE_CNT
        
if __name__ == '__main__':      
    adjDis = AdjustDispatch()  
    adjDis.calculate_cost_score()
    