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
        for job_q in sorted_job_csv:
            
            # 每分发一个 job 序列，都是从零时刻开始
            current_slice = 0
            
            # 分发每个 step 上的所有 job
            for job_q_idx in range(len(job_q)):
                job_list = job_q[job_q_idx].split(':')  
                print_and_log("dispatching %s..." % (job_list[0]))              
                for job_id in job_list:
                    offlineJob = self.offline_jobs_dict[job_id]

                    dispatchable_machine_list = [i for i in range(1, g_prefered_machine[self.job_set][1] + 1)]

                    # 尝试在 dispatchable_machine_list 个机器上分发每个 job 上的所有 inst
                    while offlineJob.inst_cnt > 0:
                        dispatch_slice = current_slice
                        if (len(offlineJob.prefix_jobs) > 0):
                            # 得到当前 offline job 的 prefix job 的最后完成时间, 该时间与当前时间晚的那个作为当前 offline job 的启动时间
                            max_finish_slice = OfflineJob.get_max_finish_slice_of_offline(offlineJob, self.machine_runing_info_dict, self.offline_jobs_dict)
                            if (dispatch_slice < max_finish_slice):
                                dispatch_slice = max_finish_slice
                            else:
                                print(getCurrentTime(), "dispatch_slice %d < %d max_finish_slice" % (dispatch_slice,  max_finish_slice))

                        print_and_log("job %s start slice %d" % (offlineJob.job_id, dispatch_slice))

                        # 在可用的机器上遍历， 看哪些机器能分发多少个 inst
                        for machine_idx in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_idx] 
                            dispatched_cnt = machine_running_res.dispatch_offline_job(offlineJob, dispatch_slice)
                            if (dispatched_cnt > 0):
                                offlineJob.inst_cnt -= dispatched_cnt
                                self.dispatch_job_list.append([job_id, machine_idx, dispatch_slice, dispatched_cnt])
                                logging.info("job %s -> machine %d, inst %d, %d left" % 
                                             (offlineJob.job_id, machine_idx, dispatched_cnt, offlineJob.inst_cnt ))

                                if (offlineJob.inst_cnt == 0):
                                    break

                        # job 的所有实例都已经分发完毕
                        if (offlineJob.inst_cnt == 0):
                            self.output_optimized()
                            break

                        print_and_log('no enough machine for job %s, %d instance left at slice %d' % 
                                      (offlineJob.job_id, offlineJob.inst_cnt, current_slice))

                        # 当前时间在所有机器上没有能够分发 job 上的所有 inst， 则需要一直等待直到某些 offline job 结束 
                        # 则查找已分发的 inst 的最小完成时间， 在最小完成时间之后查找是否有可用的 slice  
                        min_finish_slice = 1e9
                        for machine_idx in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_idx]
#                             finish_slice = machine_running_res.running_offline_min_dispatchable_slice(self.offline_jobs_dict, offlineJob, current_slice)
                            finish_slice = machine_running_res.running_offline_min_finish_slice(self.offline_jobs_dict, current_slice)
                            if (finish_slice < min_finish_slice):
                                min_finish_slice = finish_slice

#                         # 找到最小完成时间后， 有可能在最小完成时间上有多台机器上的 offline job 结束，
#                         # 按照当前时间结束 offline job， 然后将当前时间更新为最小完成时间
#                         for machine_idx in range(1, g_prefered_machine[self.job_set][1] + 1):
#                             machine_running_res = self.machine_runing_info_dict[machine_idx]                        
#                             released_job = machine_running_res.release_offline_job(self.offline_jobs_dict, current_slice, min_finish_slice)
#                             if (not released_job):
#                                 continue
# 
#                             if (machine_running_res.can_dispatch_offline_job(offlineJob, current_slice + min_finish_slice)):
#                                 dispatchable_machine_list.append(machine_idx)

                        # 将当前时间更新为最小完成时间, 当前 job 序列之后的 job 都从此时开始分发
                        current_slice = min_finish_slice
                        print_and_log("current slice updated to %d" % (current_slice))
    
                        if (current_slice >= SLICE_CNT):
                            print_and_log("ERROR current_slice %d > 1470" % current_slice)
                            exit(-1)
                    # while offlineJob.inst_cnt > 0:
                # for job_id in job_list:                
            # for job_q in sorted_job_csv:       
        # for job_q_idx in range(10):             
                
    def dispatch_offline_jobs_2(self):
        sorted_job_file = open(r'%s/../input/%s/sorted_job.%s.csv' % (runningPath, data_set, self.job_set), 'r')
        sorted_job_csv = csv.reader(sorted_job_file)

        # 每一行是一个 job 的执行序列， 每一 step 之间用逗号隔开， 每一 step 可能会同时执行多个job，用冒号隔开
        for job_q in sorted_job_csv:
            print_and_log("dispatching new job queue")
            # 分发每个 step 上的所有 job
            for job_q_idx in range(len(job_q)):
                job_list = job_q[job_q_idx].split(':')  
                              
                for job_id in job_list:
                    offlineJob = self.offline_jobs_dict[job_id]

                    dispatchable_machine_list = list(range(1, g_prefered_machine[self.job_set][1] + 1))
                    random.shuffle(dispatchable_machine_list)

                    # 尝试在 dispatchable_machine_list 个机器上分发每个 job 上的所有 inst
                    while offlineJob.inst_cnt > 0:
                        # 每分发一个 job，都是从零时刻开始
                        dispatch_slice = 0
                        if (len(offlineJob.prefix_jobs) > 0):
                            # 得到当前 offline job 的 prefix job 的最后完成时间, 该时间与当前时间晚的那个作为当前 offline job 的启动时间
                            max_finish_slice = OfflineJob.get_max_finish_slice_of_offline(offlineJob, self.machine_runing_info_dict, self.offline_jobs_dict)
                            if (dispatch_slice < max_finish_slice):
                                dispatch_slice = max_finish_slice
                            else:
                                # 应该不会走到这里
                                print(getCurrentTime(), "dispatch_slice %d < %d max_finish_slice" % (dispatch_slice,  max_finish_slice))

                        print_and_log("job %s start slice %d, %d inst left" % (offlineJob.job_id, dispatch_slice, offlineJob.inst_cnt))

                        # 在可用的机器上遍历， 找到最小可分发的 slice
                        min_dispatchable_slice = SLICE_CNT
                        min_dispatch_cnt = 1e9
                        for machine_idx in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_idx] 
                            dispatchable_slice, dispatch_cnt = machine_running_res.seek_min_dispatchable_slice_and_cnt(offlineJob, dispatch_slice)
                            if (dispatch_cnt > 0 and dispatchable_slice < SLICE_CNT and 
                                dispatchable_slice <= min_dispatchable_slice and # dispatch 相等的情况下，选择  dispatch_cnt 小的机器, 让 inst 尽量分散
                                dispatch_cnt < min_dispatch_cnt):
                                min_dispatchable_slice = dispatchable_slice
                                min_dispatch_cnt = dispatch_cnt
                                min_dispatchable_machine_id = machine_idx

                        if (min_dispatchable_slice < SLICE_CNT):
                            dispatched_cnt = self.machine_runing_info_dict[min_dispatchable_machine_id].dispatch_offline_job(offlineJob, min_dispatchable_slice)
                            if (dispatched_cnt == 0):
                                print_and_log("Error, machine %d dispatched 0 inst of job %s, %d inst left" % 
                                              (min_dispatchable_machine_id, offlineJob.job_id, offlineJob.inst_cnt))
                                exit(-1)
                            offlineJob.inst_cnt -= dispatched_cnt
                            self.dispatch_job_list.append([job_id, min_dispatchable_machine_id, min_dispatchable_slice, dispatched_cnt])
                            logging.info("job %s -> machine %d, inst %d at slice %d, %d left" % 
                                         (offlineJob.job_id, min_dispatchable_machine_id, dispatched_cnt, min_dispatchable_slice, offlineJob.inst_cnt ))
                            if (len(offlineJob.prefix_jobs) == 0 and min_dispatchable_slice > 0):
                                print_and_log("no-prefix-job job %s started from %d" % (offlineJob.job_id, min_dispatchable_slice))

                            if (offlineJob.inst_cnt == 0):
                                logging.info("all instance of job %s are dispatched, last start time %d, will finish at %d" %
                                             (offlineJob.job_id, min_dispatchable_slice, min_dispatchable_slice + offlineJob.run_mins))
                        else:
                            print_and_log("Error, no enough machine for job %s, %d inst left" % (offlineJob.job_id, offlineJob.inst_cnt))
                            exit(-1)

                    # while offlineJob.inst_cnt > 0:
                # for job_id in job_list:                
            # for job_q in sorted_job_csv:       
        # for job_q_idx in range(10):
        self.output_optimized()
        
        cost = self.sum_scores_of_machine()
        for machine_id, machine_running_res in self.sorted_machine_res:
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
        print_and_log('cost of [%s] is %f/%f' % (self.job_set, cost, cost/SLICE_CNT))

                
if __name__ == '__main__':
    job_set = sys.argv[1].split("=")[1]

    dispatch_offline_job = DispatchOfflineJob(job_set)
    dispatch_offline_job.dispatch_offline_jobs_2()
                
                
                
                
                
                
                
                
                
                
            
        