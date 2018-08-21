'''
Created on Aug 15, 2018

@author: Heng.Zhang
'''
from global_param import *
import csv
from OfflineJob import *
import pandas as pd

# 对 offline job 进行拓扑排序
def topological_sort(job_set):
    offline_jobs_csv = csv.reader(open(r'%s\..\input\%s\job_info.%s.csv' % (runningPath, data_set, job_set), 'r'))

    offline_jobs_dict = {}
    for each_job in offline_jobs_csv:
        job_symbol = each_job[0].split("-")[0]
        if (job_symbol not in offline_jobs_dict):
            offline_jobs_dict[job_symbol] = []

        offline_jobs_dict[job_symbol].append(OfflineJob(each_job))
        
    sorted_job_file = open(r'%s\..\input\%s\sorted_job.%s.csv' % (runningPath, data_set, job_set), 'w')

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
        sorted_job_file.write("%s\n" % ",".join(sorted_jobs_list))
        
    sorted_job_file.close()
            
def refine_online_dispatch(job_set):
    dispache_df = pd.read_csv(r'%s\..\output\%s\%s_optimized.csv' % (runningPath, data_set, job_set), header=None)
    s = dispache_df.drop_duplicates(subset=[0], keep='last')
    s.to_csv(r'%s\..\output\%s\%s_refined.csv' % (runningPath, data_set, job_set), index=False, header=None)
    return
            
if __name__ == '__main__':
    job_set = ['a', 'b', 'c', 'd', 'e']
    for each in job_set:
#         topological_sort(each)
        refine_online_dispatch(each)            
