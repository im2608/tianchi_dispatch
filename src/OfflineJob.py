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
        