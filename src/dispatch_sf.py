'''
Created on Aug 15, 2018

@author: Heng.Zhang
'''
from global_param import *
import csv
from OfflineJob import *
import pandas as pd


def correct_offline_dispatch():
    file_name_abcd = ['dispatch_offline.a.20180822_075423.csv',]
    
    for each_file in file_name_abcd:
        offline_jobs_csv = csv.reader(open(r'%s/../output/%s/%s' % (runningPath, data_set, each_file), 'r'))
        with open(r'%s/../output/%s/%s.corrected' % (runningPath, data_set, each_file), 'w') as output_file:
            for each_job in offline_jobs_csv:
                output_file.write('%s,machine_%s,%s,%s/n' % (each_job[0], each_job[1], each_job[2], each_job[3]))
                
# 校验 offline job 分发顺序以及 inst 数量        
def verify_offline_dispatch(job_set):
    offline_jobs_dispatch_csv = csv.reader(open(r'%s/../output/%s/dispatch_offline.%s.csv' % (runningPath, data_set, job_set), 'r'))
    offline_jobs_info_csv = csv.reader(open(r'%s/../input/%s/job_info.%s.csv' % (runningPath, data_set, job_set), 'r'))
    
    offline_jobs_dict = {}
    
    for each_job in offline_jobs_info_csv:
        job_id = each_job[0]
        if (job_id not in offline_jobs_dict):
            offline_jobs_dict[job_id] = OfflineJob(each_job)

    # 保存 offline job 的分发状态
    dispatched_job_dict = {}
    for dispatch in offline_jobs_dispatch_csv:
        job_id = dispatch[0]
        inst_cnt = int(dispatch[3])
        job_symbol = job_id.split("-")[0]
        
        offlineJob = offline_jobs_dict[job_id]
        if (len(offlineJob.prefix_jobs) > 0):
            if (job_symbol not in dispatched_job_dict):
                print("Error, job %s is dispatching, but all of its prefix job did run" % job_id)
                exit(-1)
                
            for each_prefix in offlineJob.prefix_jobs:
                if (each_prefix not in dispatched_job_dict[job_symbol]):
                    print("Error, job %s is dispatching, but its prefix job %s did run" % (job_id, each_prefix))
                    exit(-1)

        if (job_symbol not in dispatched_job_dict):
            dispatched_job_dict[job_symbol] = set()

        offline_jobs_dict[job_id].inst_cnt -= inst_cnt
        if (offline_jobs_dict[job_id].inst_cnt == 0): # 一个 job 已经分发完毕
            dispatched_job_dict[job_symbol].add(job_id)
            
    for each_job_id, offlineJOb in offline_jobs_dict.items():
        if (offlineJob.inst_cnt > 0):
            print("Error, job %s still has %d instances left" % (each_job_id, offlineJob.inst_cnt))
            
    print("job set %s Done" % job_set)
            

# 对 offline job 进行拓扑排序
def topological_sort(job_set):
    offline_jobs_csv = csv.reader(open(r'%s/../input/%s/job_info.%s.csv' % (runningPath, data_set, job_set), 'r'))

    offline_jobs_dict = {}
    for each_job in offline_jobs_csv:
        job_symbol = each_job[0].split("-")[0]
        if (job_symbol not in offline_jobs_dict):
            offline_jobs_dict[job_symbol] = []

        offline_jobs_dict[job_symbol].append(OfflineJob(each_job))
        
    sorted_job_file = open(r'%s/../input/%s/sorted_job.%s.csv' % (runningPath, data_set, job_set), 'w')

    for job_symbol in offline_jobs_dict.keys():
        sorted_jobs_list = []
        offline_jobs = offline_jobs_dict[job_symbol]
        while (len(offline_jobs) > 0):
            # 在排序的每一步中可能同时有多个 job 可以运行, dispatchable_jobs 记录了每一步可以同时运行的 job
            dispatchable_jobs = []  
            for i in range(len(offline_jobs) - 1, -1, -1):                
                if (len(offline_jobs[i].prefix_jobs) == 0): # 找到一个入度为 0 的job， 即没有前驱的 job
                    job_id = offline_jobs[i].job_id                    
                    offline_jobs.pop(i) # 将 job_id 从列表中删除， 直到删除所有的 job

                    dispatchable_jobs.append(job_id)

            # 将 job_id 从其他 job 的前驱列表中删除
            for each_dispatchable_id in dispatchable_jobs:
                for each_job in offline_jobs:
                    if (each_dispatchable_id in each_job.prefix_jobs):
                        each_job.prefix_jobs.remove(each_dispatchable_id)
        
            # 每步可同时运行的 job 用冒号 隔开            
            sorted_jobs_list.append(":".join(sorted(dispatchable_jobs))) 

        # 每步之间用逗号隔开
        sorted_job_file.write("%s/n" % ",".join(sorted_jobs_list))
        
    sorted_job_file.close()
            
def refine_online_dispatch(job_set):
    dispache_df = pd.read_csv(r'%s/../output/%s/%s_optimized.csv' % (runningPath, data_set, job_set), header=None)
    s = dispache_df.drop_duplicates(subset=[0, 1], keep='last')
    s.to_csv(r'%s/../output/%s/%s_refined.csv' % (runningPath, data_set, job_set), index=False, header=None)
    return
            
def get_max_step_of_offline(job_set):
    sorted_job_file = open(r'%s/../input/%s/sorted_job.%s.csv' % (runningPath, data_set, job_set), 'r')
    sorted_job_csv = csv.reader(sorted_job_file)
    
    max_step = 0
    for job_q in sorted_job_csv:
        if max_step < len(job_q):
            max_step = len(job_q)
            
    print(getCurrentTime(), 'job set %s max step %d' % (job_set, max_step))

    
if __name__ == '__main__':
#     correct_offline_dispatch()
    
    job_set = 'abcde'    
    for each in job_set:
#         topological_sort(each)
#         refine_online_dispatch(each)
#         get_max_step_of_offline(each)
        verify_offline_dispatch(each)            
        