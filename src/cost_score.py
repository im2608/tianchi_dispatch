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

# from functools import reduce

class AdjustDispatch(object):
    def __init__(self):
        
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

        print(getCurrentTime(), 'loading app_interference.csv')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0])
            app_id_b = int(each_cons[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.cost = 0 

        self.submit_filename = 'submit_20180712_113242_optimized_5899'

        log_file = r'%s\..\log\cost_%s.log' % (runningPath, self.submit_filename)

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')
        return

    def sorte_machine(self):
        self.sorted_machine_cost = sorted(self.machine_runing_info_dict.items(), key = lambda d : d[1].get_machine_real_score(), reverse = True)

    def adj_dispatch_reverse(self):

        for machine_idx in range(len(self.sorted_machine_cost) - 1, -1, -1):
            machine_id = self.sorted_machine_cost[machine_idx][0]
            lightest_load_machine = self.machine_runing_info_dict[machine_id]
            if (len(lightest_load_machine.running_inst_list) > 0):
                break

        for idx in range(machine_idx, -1, -1):
            machine_id = self.sorted_machine_cost[idx][0]
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
                    machine_id = self.sorted_machine_cost[i][0]
    
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

            self.sorted_machine_cost = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
            
            # 迁移之后重新计算得分
            next_cost = 0
            for machine_id, machine_running_res in self.sorted_machine_cost:
                next_cost += machine_running_res.get_machine_real_score()   

            print_and_log('migrating %s, increased score %f, next_cost %f' % \
                  (lightest_load_machine.machine_res.machine_id, sum_increated_score, next_cost))

        return next_cost
    
    def get_immigratable_machine(self, inst_id, machine_start_idx):
        immigratable_machine_list = []
        app_res = self.app_res_dict[self.inst_app_dict[inst_id]]
        for i in range(machine_start_idx + 1, len(self.sorted_machine_cost)):
            machine_id = self.sorted_machine_cost[i][0]
            immigrating_machine = self.sorted_machine_cost[i][1]
            if (immigrating_machine.can_dispatch(app_res, self.app_constraint_dict)):
                increased_score = immigrating_machine.immigrating_delta_score(app_res)
                immigratable_machine_list.append( [{machine_id : [inst_id]},increased_score] )

        return immigratable_machine_list
    
    
    # 将前 n-1 步的迁移方案与第 n 步的合并
    # 每个方案的格式为：  [{machine_id:[inst list], machine_id:[inst list], ...}, immigrating score]
    def merge_migration_solution(self, current_solution, one_step_solution, machine_real_score):
        total = len(current_solution) * len(one_step_solution)
        print_and_log('merge_migration_solution, possible steps %d' % (total))
        migration_solution = []
        scores_dict = set()
        idx = 0
        for each_current in current_solution:
            if (idx % 1000 == 0):
                print(getCurrentTime(), '%d / %d handle\r' % (idx, total), end='')

            if (each_current[1] >= machine_real_score):
                continue

            for one_step in one_step_solution:
                if (one_step[1] >= machine_real_score):
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
                        if (immigrating_machine_res.can_dispatch_ex(inst_list, self.inst_app_dict, self.app_res_dict, self.app_constraint_dict)):
                            
                            # 当前迁入列表所增加的分数
                            tmp_app_res = AppRes.sum_app_res_by_inst(each_current_tmp[0][immigrating_machine_id], self.inst_app_dict, self.app_res_dict)
                            cur_delta_score = immigrating_machine_res.immigrating_delta_score(tmp_app_res)
                            
                            # 继续迁入 inst 所增加的分数
                            tmp_app_res = AppRes.sum_app_res_by_inst(inst_list, self.inst_app_dict, self.app_res_dict)
                            delta_score = immigrating_machine_res.immigrating_delta_score(tmp_app_res)
                            
                            # 两者的差就是继续迁入 inst 后，该方案所增加的分数
                            each_current_tmp[1] = round(each_current_tmp[1] + delta_score - cur_delta_score, 2)                            
                            each_current_tmp[0][immigrating_machine_id].extend(one_step[0][immigrating_machine_id])
                            

                if (each_current_tmp[1] < machine_real_score and each_current_tmp[1] not in scores_dict):
                    migration_solution.append(each_current_tmp)
                    scores_dict.add(each_current_tmp[1])
                            
        return migration_solution

    def adj_dispatch_ex(self, max_score):
        
        machine_start_idx = 0
        
        next_cost = self.sum_scores_of_machine()

        while (self.sorted_machine_cost[machine_start_idx][1].get_machine_real_score() > max_score):
            machine_id = self.sorted_machine_cost[machine_start_idx][0]

            heavest_load_machine = self.machine_runing_info_dict[machine_id]

            inst_id = heavest_load_machine.running_inst_list[0]

            app_res = self.app_res_dict[self.inst_app_dict[inst_id]]

            # 生成迁移方案的第一步， 以及迁入后增加的分数
            dp_immigrating_solution_list = self.get_immigratable_machine(inst_id, machine_start_idx)
            print(getCurrentTime(), 'machine %d, 1st step solution is %d' % (machine_id, len(dp_immigrating_solution_list)))

            # 生成第 2 -> N 步的迁移方案            
            for inst_idx in range(1, len(heavest_load_machine.running_inst_list)):
                print(getCurrentTime(), 'searching machine %d %d/%d\r' % (machine_id, inst_idx,  len(heavest_load_machine.running_inst_list)), end='')
                each_inst = heavest_load_machine.running_inst_list[inst_idx]
                app_res = self.app_res_dict[self.inst_app_dict[each_inst]]

                immigratable_machine_dict = self.get_immigratable_machine(each_inst, machine_start_idx)
                dp_immigrating_solution_list = self.merge_migration_solution(dp_immigrating_solution_list, 
                                                                             immigratable_machine_dict, 
                                                                             heavest_load_machine.get_machine_real_score())
                print(getCurrentTime(), 'machine %d, %d step solution is %d' % (machine_id, inst_idx + 1, len(dp_immigrating_solution_list)))

            # 在所有的迁移方案中找到迁入分数最小的
            min_solution_score = 1e9            
            for idx, each_solution in enumerate(dp_immigrating_solution_list):
                if (each_solution[1] < min_solution_score):
                    min_solution_score = each_solution[1]
                    min_solution_idx = idx

            # 增加的分数 不可行 , 不用再继续尝试
            if (min_solution_score > heavest_load_machine.get_machine_real_score()):
                print_and_log('migrating %s, running len %d, increased score %f > (real score %f), continue...' % \
                              (heavest_load_machine.machine_res.machine_id, len(heavest_load_machine.running_inst_list), 
                               min_solution_score, heavest_load_machine.get_machine_real_score()))

                machine_start_idx += 1
                break

            # 迁入
            for immigrating_machine, inst_list in dp_immigrating_solution_list[min_solution_idx].items():
                for each_inst in inst_list:
                    app_res = self.app_res_dict[self.inst_app_dict[each_inst]]
    
                    # 迁入
                    if (not self.machine_runing_info_dict[immigrating_machine].dispatch_app(each_inst, app_res, self.app_constraint_dict)):
                        print_and_log("ERROR! Failed to immigrate inst %d to machine %d" % (each_inst, immigrating_machine))
                        return
                    
                    self.migrating_list.append('inst_%d,machine_%d' % (each_inst, immigrating_machine))
            
                    heavest_load_machine.release_app(each_inst, app_res) # 迁出 inst

            self.sorted_machine_cost = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
            
            # 迁移之后重新计算得分
            next_cost = self.sum_scores_of_machine()   

            print_and_log('migrating %s, increased score %f, next_cost %f' % \
                  (heavest_load_machine.machine_res.machine_id, min_solution_score, next_cost))

        return next_cost
    
    
    def sum_scores_of_machine(self):
        scores = 0
        for machine_id, machine_running_res in self.sorted_machine_cost:
            scores += machine_running_res.get_machine_real_score()
            
        return scores
        
    def adj_dispatch(self):
        machine_idx = 0
        
        non_migratable_machine = set()

        # 得分最高的机器
        while (self.sorted_machine_cost[machine_idx][1].get_machine_real_score() > 98):
            heavest_load_machine = self.machine_runing_info_dict[self.sorted_machine_cost[machine_idx][0]]
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
            for i in  range(len(self.sorted_machine_cost)-1, 0, -1):
                machine_id = self.sorted_machine_cost[i][0]
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
    
            self.sorted_machine_cost = sorted(self.machine_runing_info_dict.items(), \
                                         key = lambda d : d[1].get_machine_real_score(), reverse = True) # 排序
            
            next_cost = self.sum_scores_of_machine()

            print_and_log('%d -> %d, max_delta_score %f, next_cost %f' % \
                  (heavest_load_machine.machine_res.machine_id, immmigrating_machine, max_delta_score, next_cost))

        return  self.sum_scores_of_machine()
    
    def dispacth_app(self):
        # inst 运行在哪台机器上
        insts_running_machine_dict = dict()

        print(getCurrentTime(), 'loading instance_deploy.csv...')

        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
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
        app_dispatch_csv = csv.reader(open(r'%s\..\output\%s.csv' % (runningPath, self.submit_filename), 'r'))
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
    
        for machine_id, machine_running_res in self.sorted_machine_cost:
            if (not self.check_dispatching(machine_running_res)):
                print_and_log('ERROR! machine_%d, score %f, running list %s' % (machine_id, machine_running_res.get_machine_real_score(), machine_running_res.running_inst_list))
                self.check_dispatching(machine_running_res)
                return 
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
        
        print_and_log('cost of %s is %f/%f' % (self.submit_filename, cost, cost/SLICE_CNT))

        if (self.sorted_machine_cost[-1][1].get_machine_real_score() > 98):
            return self.cost;

        print(getCurrentTime(), 'optimizing for H -> L')
        logging.info('optimizing for H -> L')
        cost = self.sum_scores_of_machine()
        
        next_cost = self.adj_dispatch_ex(196)
        while (next_cost < cost):
            print_and_log('After adj_dispatch_ex(), score %f -> %f' % (cost, next_cost))
            cost = next_cost
            next_cost = self.adj_dispatch_ex(100)
            
#         next_cost = self.adj_dispatch()
#         while (next_cost < cost):
#             print_and_log('After adj_dispatch(), score %f -> %f' % (cost, next_cost))
#             cost = next_cost
#             next_cost = self.adj_dispatch()            
        
#         next_cost = self.adj_dispatch_reverse()            
#         while (next_cost < cost):
#             print_and_log('After adj_dispatch_reverse(), score %f -> %f' % (cost, next_cost))
#             cost = next_cost
#             next_cost = self.adj_dispatch()        
     

        with open(r'%s\..\output\%s_optimized.csv' % (runningPath, self.submit_filename), 'w') as output_file:
            for each_disp in self.migrating_list:
                output_file.write('%s\n' % (each_disp))

        cost = 0
        for machine_id, machine_running_res in self.sorted_machine_cost:
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
            cost += machine_running_res.get_machine_real_score()
    
        print(getCurrentTime(), 'finla cost is %f / %f' % (cost, cost / SLICE_CNT))
        
        return cost / SLICE_CNT
        
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
    
    
    
    
    