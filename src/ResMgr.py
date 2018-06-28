'''
Created on Jun 22, 2018

@author: Heng.Zhang
'''

from global_param import *
import csv
import numpy as np

from MachineRes import *
from AppRes import *
from MachineRunningInfo import *
    
import logging
    
class MachineResMgr(object):
    def __init__(self):
        
        log_file = r'%s\..\log\dispatch.txt' % runningPath
    
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')

        
        #  记录 machine 的运行信息， 包括 cpu 使用量,  cpu 使用率 = cpu 使用量 / cpu 容量， d, p, m, pm, app list
        self.machine_runing_info_dict = {} 
#         machine_res_csv = csv.reader(open(r'%s\..\input\normal_machine_resources.csv' % runningPath, 'r'))
        machine_res_csv = csv.reader(open(r'%s\..\input\part_machine.csv' % runningPath, 'r'))
        for each_machine in machine_res_csv:
            machine_id = each_machine[0]
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 
        
        self.app_res_dict = {}
#         app_res_csv = csv.reader(open(r'%s\..\input\normal_app_resources.csv' % runningPath, 'r'))
        app_res_csv = csv.reader(open(r'%s\..\input\app_resources.csv' % runningPath, 'r'))
        for each_app in app_res_csv:
            app_id = each_app[0]
            self.app_res_dict[app_id] = AppRes(each_app)
            
        self.inst_app_dict = {}
        inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
        for each_inst in inst_app_csv:
            inst_id = each_inst[0]
            self.inst_app_dict[inst_id] = (each_inst[1], each_inst[2]) # app id, machine id

        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
        for each_cons in app_cons_csv:
            app_id_a = each_cons[0]
            app_id_b = each_cons[1]
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])

        self.sort_machine_by_cpu_per()

        self.output_file = open(r'%s\..\output\submit.csv' % runningPath, 'w')

        return
       
#     def dispatch_app(self, app_id, machine_id):
#         app_res = self.app_res[app_id]
#         
#         # machine_id 有值时表示 app 需分配到该  macine
#         if (machine_id is not None and len(machine_id) > 0):
#             machine_running_res = self.machine_runing_info_dict[machine_id]            
#             if (self.machine_running_res.dispatch_app(app_res)):
#                 self.sort_machine_by_cpu_per()
#                 return True
#             else:
#                 # 不能将 app 分发到 machine_id 上，需要将一些 app 从 machine_id 上迁出
#                 migrate_app_list = machine_running_res.cost_of_immigrate_app(app_id, self.app_res_dict)
#                 for each_app_id in migrate_app_list:
#                     machine_running_res.release_app(self.app_res_dict[each_app_id]) 
# 
#                 machine_running_res.dispatch_app(self.app_res_dict[each_app_id])
#                 self.sort_machine_by_cpu_per()              
#         else:
#             # 排好序的 sorted_machine_res 中的 machine_res 是个 tuple， 不可修改，根据 machine_id 到  machine_runing_info_dict 
#             # 中去更新
#             for machine_info_tuple in self.sorted_machine_res:
#                 machine_id =  machine_info_tuple[0]
#                 machine_running_res = self.machine_runing_info_dict[machine_id]
#                 if (machine_running_res.dispatch_app(app_res)): 
#                     self.sort_machine_by_cpu_per()
#                     return True
#         return False

    # 分发 inst_id 到  machine_id, 将分发好的信息写入文件
    # 当现有的 machin 资源无法分发某个 inst 时，就会将某些 inst 从某个 machine 中迁出， 然后将 inst 分发到该机器
    # 迁出地 inst 需要重新分发到其他机器，此时会递归调用 dispatch_cpp, skipped_machine_id 就是迁出 inst 的机器，
    # 重新分发迁出的 inst 时应该跳过该机器
    def dispatch_inst(self, inst_id, skipped_machine_id):
        if (skipped_machine_id is not None and len(skipped_machine_id) == len(self.machine_runing_info_dict)):
            print(getCurrentTime(), 'dispatch failed for instance %s, all machine are skipped, here is no solution' % inst_id)
            logging.info('%s dispatch failed for instance %s, all machine are skipped, here is no solution' % \
                         (getCurrentTime(), inst_id))
            return False
        
        if (inst_id == 'inst_7948'):
            print(0)
        
        
        app_res = self.app_res_dict[self.inst_app_dict[inst_id][0]]

        # 排好序的 sorted_machine_res 中的 machine_res 是个 tuple， 不可修改，根据 machine_id 到  machine_runing_info_dict 
        # 中去更新
        for machine_info_tuple in self.sorted_machine_res:
            machine_id =  machine_info_tuple[0]
            if (skipped_machine_id is not None and machine_id in skipped_machine_id):
                continue

            machine_running_res = self.machine_runing_info_dict[machine_id]
            if (machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)): 
                self.sort_machine_by_cpu_per()
                self.output_file.write('%s,%s\n' % (inst_id, machine_id))
                return True

        # 没有能分配当前 inst 的机器，在每台机器上得到可迁出的 app list， 在最长的 app list 的机器上迁出
        print(getCurrentTime(), 'no machine could dispatch inst %s, finding some migrating insts' % (inst_id))
        logging.info('%s no machine could dispatch inst %s, finding some migrating insts' % (getCurrentTime(), inst_id))

        migrating_machine_running_res = None
        migrating_insts = []
        min_sum = 1e9
        for idx, machine_info_tuple in enumerate(self.sorted_machine_res):
            if (idx % 100 == 0):
                print(getCurrentTime(), '%d machined through...\n' % (idx), end='')

            machine_id =  machine_info_tuple[0]
            machine_running_res = self.machine_runing_info_dict[machine_id]
            if ((skipped_machine_id is not None and machine_id in skipped_machine_id) or 
                # 在当前机器能够容纳 inst 时， 才需要查找迁出列表
                not machine_running_res.meet_inst_res_require(app_res)):
                continue

            # 在每台机器上得到可迁出的 app list
            tmp = machine_running_res.cost_of_immigrate_app(inst_id, self.inst_app_dict, self.app_res_dict, self.app_constraint_dict)
            if (len(tmp) == 0):
                continue

            sum_of_list = AppRes.get_res_sum_of_apps(tmp, self.inst_app_dict, self.app_res_dict)
            # 找到迁出资源之和最小的一个作为最终的迁出 app list
            if (sum_of_list < min_sum):
                migrating_insts = tmp
                migrating_machine_running_res = self.machine_runing_info_dict[machine_id]
                min_sum = sum_of_list

        # 在 migrating_machine_running_res 上将 migrating_insts 迁出
        logging.info("%s. To migrat %s/%s into %s, immigrating %s " %(getCurrentTime(), inst_id, app_res.app_id, migrating_insts))
        for each_inst in migrating_insts:
            migrating_machine_running_res.release_app(each_inst, self.app_res_dict[self.inst_app_dict[each_inst][0]])

        # 迁出一些 inst 后，当前机器可以分发 inst
        if (migrating_machine_running_res.dispatch_app(inst_id, app_res, self.app_constraint_dict)):
            self.sort_machine_by_cpu_per()
        else:
            print(getCurrentTime(), 'ERROR, dispatch instance %s failed after migrating %s on machine %s' % \
                  (inst_id, migrating_insts, migrating_machine_running_res.machine_res.machine_id))
            return False
        
        # 递归调用 dispatch_app, 将迁出的 inst 分发到其他的机器上， 但是要跳过迁出的机器
        if (skipped_machine_id is None):
            skipped_machine_id = [migrating_machine_running_res.machine_res.machine_id]
        else:
            skipped_machine_id.append(migrating_machine_running_res.machine_res.machine_id)
        for each_inst in migrating_insts:
            if (not self.dispatch_inst(each_inst, skipped_machine_id)):
                return False

        return True

    # 按照 get_cpu_percentage 从低到高排序
    def sort_machine_by_cpu_per(self):
        self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_cpu_percentage())
        
#         if (self.sorted_machine_res is None):
#             self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_res_sum())
#         else:
#             machine_id =  self.sorted_machine_res[0][0]  # cpu 剩余最多的机器
#             machine_running_res = self.machine_runing_info_dict[machine_id]
#             
#             # 在cpu < 0.5 并且还有其他资源的时候，优先往
#             if (machine_running_res.cpu_percentage >= 0.5 or 
#                 machine_running_res.disk == 0 or
#                 machine_running_res.mem == 0 or
#                 machine_running_res.p == 0 or
#                 machine_running_res.m == 0 or
#                 machine_running_res.pm == 0):
#                 self.sorted_machine_res = sorted(self.machine_runing_info_dict.items(), key=lambda d:d[1].get_res_sum())
#             else:
#                 pass 




        
        
        
        
        
        