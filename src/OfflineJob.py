'''
Created on Aug 15, 2018

@author: Heng.Zhang
'''

class OfflineJob(object):

    def __init__(self, each_job_in_csv):
        self.job_id = each_job_in_csv[0]
        self.cpu = float(each_job_in_csv[1])
        self.mem = float(each_job_in_csv[2])
        self.inst_cnt = int(each_job_in_csv[3])
        self.run_mins = int(each_job_in_csv[4])
        self.prefix_jobs = []
        if (len(each_job_in_csv) > 5):
            for i in range(5, len(each_job_in_csv)):
                if (len(each_job_in_csv[i]) > 0):
                    self.prefix_jobs.append(each_job_in_csv[i])
                    
    # 得到某个 offline job 的 prefix job 的最后完成时间
    @staticmethod
    def get_max_finish_slice_of_offline(offlineJob, machine_runing_info_dict, offline_jobs_dict):
        max_finish_slice = 0
        for machine_id, machine_running_res in machine_runing_info_dict.items():
            finish_slice = machine_running_res.get_max_prefix_finish_slice(offlineJob, offline_jobs_dict)
            if (finish_slice > 0 and finish_slice > max_finish_slice):
                max_finish_slice = finish_slice

        return max_finish_slice
                
            
        
        
        
        
        