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
from audioop import reverse


def split_slice(slice):
    return np.array(list(map(float, slice.split('|'))))

def score_of_slice(slice):
    return np.where(np.greater(slice, 0.5), 1 + 10 * (np.exp(slice - 0.5) - 1), 1).sum()
    
def calculate_cost_score():
    log_file = r'%s\..\log\cost.log' % runningPath

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=log_file,
                        filemode='w')
    
    machine_runing_info_dict = {} 
    print(getCurrentTime(), 'loading machine_resources.csv')
    machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources.csv' % runningPath, 'r'))
    for each_machine in machine_res_csv:
        machine_id = each_machine[0]
        machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 
    
    print(getCurrentTime(), 'loading app_resources.csv')
    app_res_dict = {}
    app_res_csv = csv.reader(open(r'%s\..\input\app_resources.csv' % runningPath, 'r'))
    for each_app in app_res_csv:
        app_id = each_app[0]
        app_res_dict[app_id] = AppRes(each_app)
    
    print(getCurrentTime(), 'loading instance_deploy.csv')
    inst_app_dict = {}
    inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
    for each_inst in inst_app_csv:
        inst_id = each_inst[0]
        inst_app_dict[inst_id] = (each_inst[1], each_inst[2]) # app id, machine id
        
    print(getCurrentTime(), 'loading app_interference.csv')
    app_constraint_dict = {}
    app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
    for each_cons in app_cons_csv:
        app_id_a = each_cons[0]
        app_id_b = each_cons[1]
        if (app_id_a not in app_constraint_dict):
            app_constraint_dict[app_id_a] = {}

        app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])
        
    print(getCurrentTime(), 'loading submit.csv')
    app_dispatch_csv = csv.reader(open(r'%s\..\output\submit6027.csv' % runningPath, 'r'))
    for each_dispatch in app_dispatch_csv:
        inst_id = each_dispatch[0]
        machine_id = each_dispatch[1]
        app_res = app_res_dict[inst_app_dict[inst_id][0]]
        
        machine_running_res = machine_runing_info_dict[machine_id]
        if (not machine_running_res.dispatch_app(inst_id, app_res, app_constraint_dict)):
            print('%s failed to dispatch %s, running inst list %s' % \
                  machine_id, inst_id, machine_running_res.running_inst_list)
            return

    print(getCurrentTime(), 'converting cpu useage into percentage...')
    # 将 cpu 的使用量转换成百分比    
    for machine_id, machine_running_res in machine_runing_info_dict.items():        
        machine_running_res.running_machine_res.cpu_slice /= machine_running_res.machine_res.cpu 

    cost = 0.0
    max_machine_cost = 0
    idx = 0
    max_c = 0
    max_machine = 0
    machines_larger98 = {}
    for machine_id, machine_running_res in machine_runing_info_dict.items():
        machine_cost = 0
        if (machine_running_res.running_machine_res.cpu_slice.sum() == 0):
            continue

        machine_cost = score_of_slice(machine_running_res.running_machine_res.cpu_slice)
        if (machine_cost > 98.0):
            machines_larger98[machine_id] = machine_cost

        if (max_machine_cost < machine_cost):
            max_machine_cost = machine_cost
            max_machine = machine_id

        cost += machine_cost
        idx += 1
        
        if (idx % 1000 == 0):
            print(getCurrentTime(), 'calculating %d machines\r' % idx, end='')

    machines_larger98 = sorted(machines_larger98.items(), key = lambda d : d[1], reverse = True)
    logging.info('%s' % machines_larger98)

    cost /= SLICE_CNT
    
    print(getCurrentTime(), ' cost score %f' % cost)
    
    return cost
        
        
if __name__ == '__main__':        
    calculate_cost_score()
    