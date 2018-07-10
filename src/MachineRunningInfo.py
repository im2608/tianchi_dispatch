'''
Created on Jun 25, 2018

@author: Heng.Zhang
'''

from MachineRes import *
from AppRes import *
from global_param import *
 
class MachineRunningInfo(object):
    def __init__(self, each_machine):
        self.running_machine_res = MachineRes(each_machine)  # 剩余的资源
        self.machine_res = MachineRes(each_machine) # 机器的资源
        self.running_inst_list = []
        self.running_app_dict = {}
        return
    
    # ratio 为 1 或 -1，  dispatch app 时 为 -1， 释放app时 为 1
    def update_machine_res(self, inst_id, app_res, ratio):
        self.running_machine_res.update_machine_res(app_res, ratio)

        if (ratio == DISPATCH_RATIO):
            self.running_inst_list.append(inst_id)
            if (app_res.app_id not in self.running_app_dict):
                self.running_app_dict[app_res.app_id] = 0

            self.running_app_dict[app_res.app_id] += 1
        else:
            self.running_inst_list.remove(inst_id)

            self.running_app_dict[app_res.app_id] -= 1
            if (self.running_app_dict[app_res.app_id] == 0):
                self.running_app_dict.pop(app_res.app_id)

        return True

    # 查找机器上的 running inst list 是否有违反约束的 inst
    def any_self_violate_constriant(self, inst_app_dict, app_res_dict, app_constraint_dict):
        for inst_a in self.running_inst_list:
            app_res_a = app_res_dict[inst_app_dict[inst_a]]
            for inst_b in self.running_inst_list:
                app_res_b = app_res_dict[inst_app_dict[inst_b]]
                immmigrate_app_b_running_inst = self.running_app_dict[app_res_b.app_id]

                # 存在 app_a, app_b, k 约束
                if (app_res_a.app_id in app_constraint_dict and app_res_b.app_id in app_constraint_dict[app_res_a.app_id]):
                    if (app_res_a.app_id == app_res_b.app_id):
                        if (immmigrate_app_b_running_inst > app_constraint_dict[app_res_a.app_id][app_res_b.app_id] + 1):                         
                            return inst_b
                    else:
                        if (immmigrate_app_b_running_inst > app_constraint_dict[app_res_a.app_id][app_res_b.app_id]):
                            return inst_b
        return None

    def print_remaining_res(self, inst_app_dict, app_res_dict):
        for each_inst in self.running_inst_list:
            print(getCurrentTime(), '%s, %s ' % (each_inst, app_res_dict[inst_app_dict[each_inst]].to_string()))
            
        print(getCurrentTime(), self.running_machine_res.to_string())
    
    def get_cpu(self):
        return self.running_machine_res.cpu

    def get_cpu_percentage(self):
        return max(self.running_machine_res.cpu_percentage - 0.5, 0) # cpu 使用率低于0.5 的归为一类 6027
#         return self.running_machine_res.cpu_percentage

    def get_machine_score(self):
        return max(self.running_machine_res.machine_score - 100, 0) # 得分低于100 的归为一类

    def get_machine_real_score(self):
        return self.running_machine_res.machine_score
    
    def get_cpu_useage(self):
        return self.running_machine_res.cpu_useage
    
    # 得到剩余可用资源
    def get_res_sum(self):
        return self.running_machine_res.get_res_sum()
    
    # 查看机器总的资源是否能容纳 app
    def meet_inst_res_require(self, app_res):
        return self.machine_res.meet_inst_res_require(app_res)
    
    # 如果符合约束，则可以迁入，则 app_B_running_inst 会 +1， 所以这里用 <, 不能用 <=
    def check_if_meet_A_B_constraint(self, app_A_id, app_B_id, app_B_running_inst, app_constraint_dict):
        if (app_A_id in app_constraint_dict and app_B_id in app_constraint_dict[app_A_id]):
            if (app_A_id == app_B_id):
                return app_B_running_inst < app_constraint_dict[app_A_id][app_B_id] + 1
            else:
                return app_B_running_inst < app_constraint_dict[app_A_id][app_B_id]

        return True
        
    
    # 迁入 app_res 是否满足约束条件
    def meet_constraint(self, app_res, app_constraint_dict):
        # 需要迁入的 app 在当前机器上运行的实例数
        immmigrate_app_running_inst = 0
        if (app_res.app_id in self.running_app_dict):
            immmigrate_app_running_inst = self.running_app_dict[app_res.app_id]

        # 在当前机器上运行的 app 与需要迁入的 app 是否有约束，有约束的话看 immmigrate_app_running_inst 是否满足约束条件
        # 不满足约束的情况下 1. 不能部署在当前机器上，  2. 迁移走某些 app 使得可以部署
        # 当前先实现 1
        for app_id, inst_cnt in self.running_app_dict.items():
            if (not self.check_if_meet_A_B_constraint(app_A_id = app_id, 
                                                      app_B_id = app_res.app_id, 
                                                      app_B_running_inst = immmigrate_app_running_inst,
                                                      app_constraint_dict=app_constraint_dict)):
                return False

            if (not self.check_if_meet_A_B_constraint(app_A_id = app_res.app_id, 
                                                      app_B_id = app_id,
                                                      app_B_running_inst = inst_cnt,
                                                      app_constraint_dict = app_constraint_dict)):
                return False

        return True
    
    # 迁入一个 app list 是否满足约束条件
    def meet_constraint_ex(self, inst_list, inst_app_dict, app_res_dict, app_constraint_dict):
        tmp_running_app_dict = self.running_app_dict.copy()

        for each_inst in inst_list:
            app_res = app_res_dict[inst_app_dict[each_inst]]
            # 需要迁入的 app 在当前机器上运行的实例数
            immmigrate_app_running_inst = 0
            if (app_res.app_id in tmp_running_app_dict):
                immmigrate_app_running_inst = tmp_running_app_dict[app_res.app_id]
    
            # 在当前机器上运行的 app 与需要迁入的 app 是否有约束，有约束的话看 immmigrate_app_running_inst 是否满足约束条件
            # 不满足约束的情况下 1. 不能部署在当前机器上，  2. 迁移走某些 app 使得可以部署
            # 当前先实现 1
            for app_id, inst_cnt in tmp_running_app_dict.items():
                if (not self.check_if_meet_A_B_constraint(app_A_id = app_id, 
                                                          app_B_id = app_res.app_id, 
                                                          app_B_running_inst = immmigrate_app_running_inst,
                                                          app_constraint_dict = app_constraint_dict)):
                    return False
    
                if (not self.check_if_meet_A_B_constraint(app_A_id = app_res.app_id, 
                                                          app_B_id = app_id,
                                                          app_B_running_inst = inst_cnt,
                                                          app_constraint_dict = app_constraint_dict)):
                    return False
    
            # 要迁入的 app_res.app_id 都符合 running inst 的约束
            if (app_res.app_id not in tmp_running_app_dict):
                tmp_running_app_dict[app_res.app_id] = 0

            tmp_running_app_dict[app_res.app_id] += 1

        return True

    # 是否可以将 app_res_list 分发到当前机器
    def can_dispatch_ex(self, inst_list, inst_app_dict, app_res_dict, app_constraint_dict):
        if (not self.meet_constraint_ex(inst_list, inst_app_dict, app_res_dict, app_constraint_dict)):
            return False
        
        tmp_app_res = AppRes.sum_app_res_by_inst(inst_list, inst_app_dict, app_res_dict)
        
        # 满足约束条件，看剩余资源是否满足
        return self.running_machine_res.meet_inst_res_require(tmp_app_res)
    
    
    # 是否可以将 app_res 分发到当前机器
    def can_dispatch(self, app_res, app_constraint_dict):
        
        if (not self.meet_constraint(app_res, app_constraint_dict)):
            return False        

        # 满足约束条件，看剩余资源是否满足
        return self.running_machine_res.meet_inst_res_require(app_res)
    
    def dispatch_app(self, inst_id, app_res, app_constraint_dict):
        if (self.can_dispatch(app_res, app_constraint_dict)):
            self.update_machine_res(inst_id, app_res, DISPATCH_RATIO)
            return True

        return False
    
    # 将 app 迁出后所减少的分数
    def migrating_delta_score(self, app_res):
        tmp = self.running_machine_res.cpu_slice + app_res.cpu_slice # app 迁出后， 剩余的cpu 容量增加
        
        score = score_of_cpu_percent_slice((self.machine_res.cpu - tmp) / self.machine_res.cpu)
        return self.get_machine_real_score() - score  
    
    # 将 app 迁入后所增加的分数
    def immigrating_delta_score(self, app_res):
        tmp = self.running_machine_res.cpu_slice - app_res.cpu_slice # app 迁入后， 剩余的cpu 容量减少
        tmp = np.where(np.less(tmp, 0.001), 0, tmp) # slice 由于误差可能不会为0， 这里凡是 < 0.001 的 slice 都设置成0
        score = score_of_cpu_percent_slice((self.machine_res.cpu - tmp) / self.machine_res.cpu)
        return score - self.get_machine_real_score()   

    def release_app(self, inst_id, app_res):
        if (inst_id in self.running_inst_list):
            self.update_machine_res(inst_id, app_res, RELEASE_RATIO)
            return True

        return False

    # 为了将  immgrate_inst_id 迁入， 需要将 running_inst_list 中的一个或多个 inst 迁出，
    # 迁出的规则为： 满足迁入app cpu 的最小值，迁出的 app 越多越好，越多表示迁出的 app cpu 越分散，迁移到其他机器上也就越容易
    def cost_of_immigrate_app(self, immgrate_inst_id, inst_app_dict, app_res_dict, app_constraint_dict):
       
        start_time = time.time()
        candidate_apps_list_of_machine = []
        # 候选 迁出  inst list 的长度从 1 到 len(self.runing_app_list)
        candidate_insts = self.running_inst_list.copy()
        for inst_list_size in range(1, len(candidate_insts) + 1):
            app_list_at_size = []
            end_idx_of_running_set = len(candidate_insts) - inst_list_size + 1 
            for i in range(end_idx_of_running_set): 
                cur_inst_list = [candidate_insts[i]]
                self.find_migratable_app(cur_inst_list, inst_list_size - 1, i + 1, candidate_insts, \
                                         app_list_at_size, immgrate_inst_id, \
                                         inst_app_dict, app_res_dict, app_constraint_dict)

            candidate_apps_list_of_machine.extend(app_list_at_size)
            # 若 inst 出现在长度为 n 的候选迁出列表中，则该 inst 不会出现在长度为 n+1 的列表中， 将 inst 从候选列表中删除，
            # 这样可以极大地减小枚举的数量
            for each_list in app_list_at_size:               
                for each_inst in each_list:
                    candidate_insts.remove(each_inst)

            # len(candidate_insts) <= inst_list_size , inst_list_size 为已经枚举完毕的长度，下次循环会+1， 所以这里是 <=
            if (len(candidate_insts) == 0 or len(candidate_insts) <= inst_list_size):
                break

        # 在所有符合条件的可迁出 app list 中， 找到在当前机器上得分最高的作为迁出列表
        if (len(candidate_apps_list_of_machine) > 0):
            max_score = 0
            max_idx = 0
            for i, each_candidate_list in enumerate(candidate_apps_list_of_machine):
                score_of_list = AppRes.get_socre_of_apps(each_candidate_list, inst_app_dict, app_res_dict, self.machine_res.cpu)
                if (score_of_list < max_score):
                    score_of_list = score_of_list
                    max_idx = i

            end_time = time.time()
            
            print(getCurrentTime(), " done, running inst len %d, ran %d seconds" % \
                  (len(self.running_inst_list), end_time - start_time))

            return candidate_apps_list_of_machine[max_idx], max_score
        else:
            return []
        
    # 在 running_inst_list 的 [start_idx, end_idx) 范围内， 找到一个 app_list_size 长度的 app_list, 
    # 使得 app_list 的 cpu 满足迁入的  app cpu， 保存起来作为迁出的 app list 候选
    def find_migratable_app(self, cur_inst_list, left_inst_list_size, start_idx, candidate_insts,
                            candidate_apps_list, immgrate_inst_id, inst_app_dict, app_res_dict, app_constraint_dict):
        if (left_inst_list_size == 0):
            # 将要迁出的资源之和
            tmp_app_res = AppRes.sum_app_res_by_inst(cur_inst_list, inst_app_dict, app_res_dict)

            # 候选的迁出 app list 资源加上剩余的资源 满足迁入的  app cpu， 保存起来作为迁出的 app list 候选
            immigrating_app_res = app_res_dict[inst_app_dict[immgrate_inst_id]]
            if (np.all(tmp_app_res.cpu_slice + self.running_machine_res.cpu_slice >= immigrating_app_res.cpu_slice) and 
                np.all(tmp_app_res.mem_slice + self.running_machine_res.mem >= immigrating_app_res.mem_slice) and 
                tmp_app_res.disk_usg + self.running_machine_res.disk >= immigrating_app_res.disk and 
                tmp_app_res.p_usg + self.running_machine_res.p >= immigrating_app_res.p and 
                tmp_app_res.m_usg + self.running_machine_res.m >= immigrating_app_res.m and
                tmp_app_res.pm_usg + self.running_machine_res.pm >= immigrating_app_res.pm):
                candidate_apps_list.append(cur_inst_list)
            return 
        
        for i in range(start_idx, len(candidate_insts)):
            self.find_migratable_app(cur_inst_list + [candidate_insts[i]], left_inst_list_size - 1, i + 1, candidate_insts,
                                     candidate_apps_list, immgrate_inst_id, inst_app_dict, app_res_dict, app_constraint_dict)
        return