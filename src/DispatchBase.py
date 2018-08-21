'''
Created on Aug 15, 2018

@author: Heng.Zhang
'''
from global_param import *
import csv
from AppRes import *
from MachineRunningInfo import *
import os
import datetime

class DispatchBase(object):

    def __init__(self, job_set):
        self.job_set = job_set
        
        print(getCurrentTime(), 'loading app_resources.csv...')
        self.app_res_dict = [0 for x in range(APP_CNT + 1)]
        app_res_csv = csv.reader(open(r'%s/../input/%s/app_resources.csv' % (runningPath, data_set), 'r'))
        for each_app in app_res_csv:
            app_id = int(each_app[0].split('_')[1])
            self.app_res_dict[app_id] = AppRes(each_app)

        print(getCurrentTime(), 'loading app_interference.csv...')
        self.app_constraint_dict = {}
        app_cons_csv = csv.reader(open(r'%s/../input/%s/app_interference.csv' % (runningPath, data_set), 'r'))
        for each_cons in app_cons_csv:
            app_id_a = int(each_cons[0].split('_')[1])
            app_id_b = int(each_cons[1].split('_')[1])
            if (app_id_a not in self.app_constraint_dict):
                self.app_constraint_dict[app_id_a] = {}

            self.app_constraint_dict[app_id_a][app_id_b] = int(each_cons[2])
            
        print(getCurrentTime(), 'loading machine_resources.csv...')
        self.machine_runing_info_dict = {} 
        machine_res_csv = csv.reader(open(r'%s/../input/%s/machine_resources.%s.csv' % (runningPath, data_set, job_set), 'r'))

        for each_machine in machine_res_csv:
            machine_id = int(each_machine[0].split('_')[1])
            self.machine_runing_info_dict[machine_id] = MachineRunningInfo(each_machine) 

        insts_running_machine_dict = dict()
        self.inst_app_dict = {}
        
        inst_app_csv = csv.reader(open(r'%s/../input/%s/instance_deploy.%s.csv' % (runningPath, data_set, self.job_set), 'r'))
        for each_inst in inst_app_csv:
            inst_id = int(each_inst[0].split('_')[1])
            app_id = int(each_inst[1].split('_')[1])
            self.inst_app_dict[inst_id]  = app_id
            if (len(each_inst[2]) > 0):
                machine_id = int(each_inst[2].split('_')[1])
                self.machine_runing_info_dict[machine_id].update_machine_res(inst_id, self.app_res_dict[app_id], DISPATCH_RATIO)
                insts_running_machine_dict[inst_id] = machine_id    
                
        optimized_file = r'%s/../output/%s/%s_refined.csv' % (runningPath, data_set, self.job_set)
        
        if (os.path.exists(optimized_file)):
            print(getCurrentTime(), 'loading %s' % optimized_file)
            app_dispatch_csv = csv.reader(open(optimized_file, 'r'))
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
                
        time_now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        log_file = r'%s/../log/offline_%s_%s_%s.log' % (runningPath, data_set, job_set, time_now)

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file,
                            filemode='w')

        self.output_filename = r'%s/../output/%s/dispatch_offline.%s.%s.csv' % (runningPath, data_set, self.job_set, time_now)
        
        self.dispatch_job_list = []    

    def output_optimized(self):

        with open(self.output_filename, 'w') as output_file:
            for each_disp in self.dispatch_job_list:
                output_file.write('%s\n' % (each_disp))

        output_file.close()

        cost = 0
        for machine_id, machine_running_res in self.machine_runing_info_dict.items():
            cost += machine_running_res.get_machine_real_score()
    
        print(getCurrentTime(), 'finla cost is %f / %f' % (cost, cost / SLICE_CNT))
        