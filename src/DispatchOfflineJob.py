'''
Created on Aug 20, 2018

@author: Heng.Zhang
'''

from DispatchBase import *
from OfflineJob import *
import csv
from blaze.dispatch import dispatch

class DispatchOfflineJob(DispatchBase):

    def __init__(self, job_set):
        DispatchBase.__init__(self, job_set)

        offline_jobs_csv = csv.reader(open(r'%s\..\input\%s\job_info.%s.csv' % (runningPath, data_set, job_set), 'r'))

        self.offline_jobs_dict = {}
        for each_job in offline_jobs_csv:
            job_id = each_job[0]
    
            self.offline_jobs_dict[job_id] = OfflineJob(each_job)
        
    def dispatch_offline_jobs(self):
        
        current_slice = 0
        
        # 分发一个 step 上的 job
        for job_q_idx in range(g_max_offline_job_step[self.job_set] + 1):
            print(getCurrentTime(), "dispatching step %d" % job_q_idx)
            sorted_job_file = open(r'%s\..\input\%s\sorted_job.%s.csv' % (runningPath, data_set, self.job_set), 'r')
            sorted_job_csv = csv.reader(sorted_job_file)

            # 每一行是一个 job 的执行序列， 每一 step 之间用逗号隔开， 每一 step 可能会同时执行多个job，用冒号隔开
            for job_q in sorted_job_csv: 
                if (len(job_q) < job_q_idx):
                    continue

                # 分发每个 step 上的所有 job
                job_list = job_q[job_q_idx].split(':')                
                for job_id in job_list:
                    offlineJob = self.offline_jobs_dict[job_id]

                    dispatchable_machine_list = [i for i in range(1, g_prefered_machine[self.job_set][1] + 1)]

                    # 尝试在 dispatchable_machine_list 个机器上分发每个 job 上的所有 inst
                    while offlineJob.inst_cnt > 0:
                        dispatch_slice = current_slice
                        if (len(offlineJob.prefix_jobs) > 0):
                            # 得到某个 offline job 的 prefix job 的最后完成时间, 该时间作为当前 offline job 的启动时间
                            dispatch_slice = OfflineJob.get_max_finish_slice_of_offline(offlineJob, self.machine_runing_info_dict, self.offline_jobs_dict)
                            print(getCurrentTime(), "job %s start slice %d" % (offlineJob.job_id, dispatch_slice))

                        # 在可用的机器上遍历， 看哪些机器能分发多少个 inst
                        for machine_idx in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_idx] 
                            dispatched_cnt = machine_running_res.dispatch_offline_job(offlineJob, dispatch_slice)
                            if (dispatched_cnt > 0):
                                offlineJob.inst_cnt -= dispatched_cnt
                                self.dispatch_job_list.append([job_id, machine_idx, dispatch_slice, dispatched_cnt])

                                if (offlineJob.inst_cnt == 0):
                                    break

                        # job 的所有实例都已经分发完毕
                        if (offlineJob.inst_cnt == 0):
#                             print_and_log("all instance of %s dispatched" % offlineJob.job_id)
                            self.output_optimized()
                            break

                        print_and_log('no enough machine for job %s, %d instance left' % (offlineJob.job_id, offlineJob.inst_cnt))

                        # 在所有机器上没有能够分发 job 上的所有 inst， 则查找 inst 的最小完成时间， 将当前时间更新为最小完成时间
                        min_finish_slice = 1e9
                        for machine_idx in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_idx]
                            finish_slice = machine_running_res.running_offline_min_finish_slice(self.offline_jobs_dict)
                            if (finish_slice < min_finish_slice):
                                min_finish_slice = finish_slice

                        # 找到最小完成时间后， 有可能在最小完成时间上有多个offline job 结束，
                        # 按照当前时间结束 offline job， 然后将当前时间更新为最小完成时间
                        dispatchable_machine_list.clear()
                        for machine_idx in range(1, g_prefered_machine[self.job_set][1] + 1):
                            machine_running_res = self.machine_runing_info_dict[machine_idx]                        
                            released_job = machine_running_res.release_offline_job(self.offline_jobs_dict, current_slice, min_finish_slice)
                            if (not released_job):
                                continue

                            if (machine_running_res.can_dispatch_offline_job(offlineJob, current_slice + min_finish_slice)):
                                dispatchable_machine_list.append(machine_idx)

                        current_slice = min_finish_slice
                        print_and_log("current slice updated to %d" % (current_slice))
    
                        if (current_slice >= SLICE_CNT):
                            print_and_log("ERROR current_slice %d > 1470" % current_slice)
                            exit(-1)
                    # while offlineJob.inst_cnt > 0:
                # for job_id in job_list:
                dispatchable_machine_list = [i for i in range(1, g_prefered_machine[self.job_set][1] + 1)]
                
            # for job_q in sorted_job_csv:       
        # for job_q_idx in range(10):       
        
    def dispatch_offline_jobs_2(self):
        sorted_job_file = open(r'%s\..\input\%s\sorted_job.%s.csv' % (runningPath, data_set, self.job_set), 'r')
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

                                if (offlineJob.inst_cnt == 0):
                                    break

                        # job 的所有实例都已经分发完毕
                        if (offlineJob.inst_cnt == 0):
#                             print_and_log("all instance of %s dispatched" % offlineJob.job_id)
                            self.output_optimized()
                            break

                        print_and_log('no enough machine for job %s, %d instance left' % (offlineJob.job_id, offlineJob.inst_cnt))

                        # 当前时间在所有机器上没有能够分发 job 上的所有 inst， 则查找已分发的 inst 的最小完成时间，  意味着一直等待直到最小完成时间
                        min_finish_slice = 1e9
                        for machine_idx in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_idx]
                            finish_slice = machine_running_res.running_offline_min_finish_slice(self.offline_jobs_dict, current_slice)
                            if (finish_slice < min_finish_slice):
                                min_finish_slice = finish_slice

#                         # 找到最小完成时间后， 有可能在最小完成时间上有多个offline job 结束，
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
                
                
                
if __name__ == '__main__':
    job_set = sys.argv[1].split("=")[1]

    dispatch_offline_job = DispatchOfflineJob(job_set)
    dispatch_offline_job.dispatch_offline_jobs_2()
                
                
                
                
                
                
                
                
                
                
            
        