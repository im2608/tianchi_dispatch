'''
Created on Aug 20, 2018

@author: Heng.Zhang
'''

from DispatchBase import *
from OfflineJob import *
import csv
import random

class DispatchOfflineJob(DispatchBase):

    def __init__(self, job_set):
        DispatchBase.__init__(self, job_set)

        offline_jobs_csv = csv.reader(open(r'%s/../input/%s/job_info.%s.csv' % (runningPath, data_set, job_set), 'r'))

        self.offline_jobs_dict = {}
        for each_job in offline_jobs_csv:
            job_id = each_job[0]
    
            self.offline_jobs_dict[job_id] = OfflineJob(each_job)
         
    def dispatch_offline_jobs(self):
        sorted_job_file = open(r'%s/../input/%s/sorted_job.%s.csv' % (runningPath, data_set, self.job_set), 'r')
        sorted_job_csv = csv.reader(sorted_job_file)

        # 每一行是一个 job 的执行序列， 每一 step 之间用逗号隔开， 每一 step 可能会同时执行多个job，用冒号隔开
        dispatched_job = 0
        for job_q in sorted_job_csv:
            print_and_log("dispatching new job queue")
            # 分发每个 step 上的所有 job
            for job_q_idx in range(len(job_q)):
                job_list = job_q[job_q_idx].split(':')  
                              
                for job_id in job_list:
                    offlineJob = self.offline_jobs_dict[job_id]

                    shuffled_machine_list = list(range(1, g_prefered_machine[self.job_set][1] + 1))
                    random.shuffle(shuffled_machine_list)

                    # 每分发一个 job， 都是从零时刻开始
                    dispatch_slice = 0
                    if (len(offlineJob.prefix_jobs) > 0):
                        # 得到当前 offline job 的 prefix job 的最后完成时间, 该时间与当前时间晚的那个作为当前 offline job 的启动时间
                        max_finish_slice = OfflineJob.get_max_finish_slice_of_offline(offlineJob, self.machine_runing_info_dict, self.offline_jobs_dict)
                        if (dispatch_slice < max_finish_slice):
                            dispatch_slice = max_finish_slice
                        else:
                            # 应该不会走到这里
                            print(getCurrentTime(), "dispatch_slice %d < %d max_finish_slice" % (dispatch_slice,  max_finish_slice))

                    while (offlineJob.inst_cnt > 0):
                        # 在可用的机器上遍历， 找到最小可分发的 slice
                        min_dispatchable_slice = SLICE_CNT
                        dispatchable_machine_list = []
                        for machine_id in shuffled_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_id] 
                            dispatchable_slice = machine_running_res.seek_min_dispatchable_slice(offlineJob, dispatch_slice)
                            if (dispatchable_slice < SLICE_CNT and dispatchable_slice <= min_dispatchable_slice):                                
                                if (dispatchable_slice < min_dispatchable_slice):
                                    min_dispatchable_slice = dispatchable_slice
                                    dispatchable_machine_list.clear()
                                dispatchable_machine_list.append(machine_id)

                        if (min_dispatchable_slice == SLICE_CNT):
                            print_and_log("Error, no enough machine for job %s, %d inst left" % (offlineJob.job_id, offlineJob.inst_cnt))
                            exit(-1)

                        print_and_log("job %s, %d inst left, start slice %d, min dispatch slice %d" % 
                                      (offlineJob.job_id, offlineJob.inst_cnt, dispatch_slice, min_dispatchable_slice))

                        dispatch_slice = min_dispatchable_slice

                        for machine_id in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_id]
                            dispatch_cnt = machine_running_res.dispatch_offline_job_one(offlineJob, dispatch_slice)
                            if (dispatch_cnt > 0):
                                offlineJob.inst_cnt -= dispatch_cnt                                
                                self.dispatch_job_list.append([job_id, machine_id, dispatch_slice, dispatch_cnt])
                                logging.info("job %s -> machine %d, inst %d at slice %d, %d left" % 
                                             (offlineJob.job_id, machine_id, dispatch_cnt, dispatch_slice, offlineJob.inst_cnt ))

                                if (offlineJob.inst_cnt == 0):
                                    dispatched_job += 1
                                    print_and_log("all instance of job %s are dispatched, last start time %d, will finish at %d (%d/%d)" %
                                             (offlineJob.job_id, min_dispatchable_slice, dispatch_slice + offlineJob.run_mins, 
                                              dispatched_job, g_job_cnt[self.job_set]))
                                    break

                    # while offlineJob.inst_cnt > 0:
                # for job_id in job_list:                
            # for job_q in sorted_job_csv:       
        # for job_q in sorted_job_csv:
        self.output_optimized()
        
        cost = self.sum_scores_of_machine()
        for machine_id, machine_running_res in self.sorted_machine_res:
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
        print_and_log('cost of [%s] is %f/%f' % (self.job_set, cost, cost/SLICE_CNT))

                
if __name__ == '__main__':
    job_set = sys.argv[1].split("=")[1]

    dispatch_offline_job = DispatchOfflineJob(job_set)
    dispatch_offline_job.dispatch_offline_jobs()
    
    
    