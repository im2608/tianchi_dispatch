'''
Created on Aug 20, 2018

@author: Heng.Zhang
'''

from DispatchBase import *
from OfflineJob import *
import csv
import random

class DispatchOfflineJob(DispatchBase):

    def __init__(self, job_set, optimized_dispatch_file):
        DispatchBase.__init__(self, job_set, optimized_dispatch_file)
        
        self.total_cpu = 0.0
        self.total_mem = 0.0
        
        self.total_expanded_idle_machine = 0

        offline_jobs_csv = csv.reader(open(r'%s/../input/%s/job_info.%s.csv' % (runningPath, data_set, job_set), 'r'))
        self.offline_jobs_dict = {}
        for each_job in offline_jobs_csv:
            job_id = each_job[0]    
            self.offline_jobs_dict[job_id] = OfflineJob(each_job)
            
            self.total_cpu += self.offline_jobs_dict[job_id].cpu * self.offline_jobs_dict[job_id].inst_cnt * self.offline_jobs_dict[job_id].run_mins
            self.total_mem += self.offline_jobs_dict[job_id].mem * self.offline_jobs_dict[job_id].inst_cnt * self.offline_jobs_dict[job_id].run_mins

        print_and_log("total cpu %f, total mem %f" % (self.total_cpu, self.total_mem))
        
        self.offline_job_last_finish_slice_dict = {}
        
            
    def dispatch_offline_jobs(self):
        
        idle_machine_list = []
        usable_machine_list = []
        for machine_id in range(1, g_prefered_machine[self.job_set][1] + 1):
            machine_running_res = self.machine_runing_info_dict[machine_id]
            if (len(machine_running_res.running_inst_list) == 0):
                idle_machine_list.append(machine_id)
            else:
                usable_machine_list.append(machine_id)

        # 每一行是一个 job 的执行序列， 每一 step 之间用逗号隔开， 每一 step 可能会同时执行多个job，用冒号隔开
        sorted_job_file = open(r'%s/../input/%s/sorted_job.%s.csv' % (runningPath, data_set, self.job_set), 'r')
        sorted_job_csv = csv.reader(sorted_job_file)
        
        dispatched_job = 0
        
        for job_q in sorted_job_csv:
            job_q_dispatch_soltion = []
            print_and_log("dispatching new job queue, estimated running time %s" % job_q[-1])
            # 分发每个 step 上的所有 job
            job_step_idx = 0
            dispatched_job_in_q = 0            
            
#             for job_q_idx in range(len(job_q) - 1): # 最后一项是估计的运行时间
            while (job_step_idx < len(job_q) - 1): # 最后一项是估计的运行时间
                job_list = job_q[job_step_idx].split(':')  

#                 for job_id in job_list:
                job_id_idx = 0

                continue_dispatch = True

                while job_id_idx < len(job_list) and continue_dispatch:
                    job_id = job_list[job_id_idx]
                    offlineJob = self.offline_jobs_dict[job_id]

                    random.shuffle(usable_machine_list)

                    # 每分发一个 job， 都是从零时刻开始
                    dispatch_slice = 0
                    if (len(offlineJob.prefix_jobs) > 0):
                        # 得到当前 offline job 的 prefix job 的最后完成时间, 该时间与当前时间晚的那个作为当前 offline job 的启动时间
                        max_finish_slice = 0
                        for prefix_job in offlineJob.prefix_jobs:
                            if (max_finish_slice < self.offline_job_last_finish_slice_dict[prefix_job]):
                                max_finish_slice = self.offline_job_last_finish_slice_dict[prefix_job]

#                         max_finish_slice = OfflineJob.get_max_finish_slice_of_offline(offlineJob, self.machine_runing_info_dict, self.offline_jobs_dict)
                        if (dispatch_slice < max_finish_slice):
                            dispatch_slice = max_finish_slice
                        else:
                            # 应该不会走到这里
                            print(getCurrentTime(), "dispatch_slice %d < %d max_finish_slice" % (dispatch_slice,  max_finish_slice))

                        print_and_log("job %s, pre fix job finish at %d" % (job_id, dispatch_slice))

                    while (offlineJob.inst_cnt > 0):
                        # 在可用的机器上遍历， 找到最小可分发的 slice
                        min_dispatchable_slice = SLICE_CNT
                        dispatchable_machine_list = []
                        for machine_id in usable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_id] 

                            dispatchable_slice = machine_running_res.seek_min_dispatchable_slice(offlineJob, dispatch_slice)
                            if (dispatchable_slice < SLICE_CNT and dispatchable_slice <= min_dispatchable_slice):
                                if (dispatchable_slice < min_dispatchable_slice):
                                    min_dispatchable_slice = dispatchable_slice
                                    dispatchable_machine_list.clear()
                                dispatchable_machine_list.append(machine_id)

                        # 分发失败， 分配一台空闲机器
                        if (min_dispatchable_slice == SLICE_CNT):
                            print_and_log("Warning! No enough machine for job %s, %d inst left" % (offlineJob.job_id, offlineJob.inst_cnt))
                            for each_disp in job_q_dispatch_soltion:
                                offlineJob = self.offline_jobs_dict[each_disp[0]]
                                machine_id = each_disp[1]
                                dispatch_slice = each_disp[2]
                                dispatch_cnt = each_disp[3]

                                self.machine_runing_info_dict[machine_id].release_offline_job(offlineJob, dispatch_slice, dispatch_cnt)
                                offlineJob.inst_cnt += dispatch_cnt
                                
                                if (offlineJob.job_id in self.offline_job_last_finish_slice_dict):
                                    self.offline_job_last_finish_slice_dict.pop(offlineJob.job_id)

                            usable_machine_list, idle_machine_list = self.extend_usable_machine_list(usable_machine_list, idle_machine_list)
                            job_step_idx = 0
                            job_id_idx = 0
                            job_q_dispatch_soltion = []
                            dispatched_job_in_q = 0
                            continue_dispatch = False
                            break

                        print_and_log("job %s, %d inst left, start slice %d, min dispatch slice %d, usable machine cnt %d  " % 
                                      (offlineJob.job_id, offlineJob.inst_cnt, dispatch_slice, min_dispatchable_slice, len(dispatchable_machine_list)), print_new_line=False)

                        dispatch_slice = min_dispatchable_slice

                        for machine_id in dispatchable_machine_list:
                            machine_running_res = self.machine_runing_info_dict[machine_id]
#                             dispatch_cnt = machine_running_res.dispatch_offline_job_one(offlineJob, dispatch_slice)
                            dispatch_cnt = machine_running_res.dispatch_offline_job(offlineJob, dispatch_slice)
                            if (dispatch_cnt > 0):
                                offlineJob.inst_cnt -= dispatch_cnt                                
                                job_q_dispatch_soltion.append([job_id, machine_id, dispatch_slice, dispatch_cnt])
                                logging.info("job %s -> machine %d, inst %d at slice %d, %d left" % 
                                             (offlineJob.job_id, machine_id, dispatch_cnt, dispatch_slice, offlineJob.inst_cnt ))

                                if (offlineJob.inst_cnt == 0):
                                    dispatched_job_in_q += 1
                                    self.offline_job_last_finish_slice_dict[offlineJob.job_id] = dispatch_slice + offlineJob.run_mins
                                    print_and_log("all instance of job %s are dispatched, last start time %d, will finish at %d (%d/%d/%d) %s" %
                                             (offlineJob.job_id, min_dispatchable_slice, dispatch_slice + offlineJob.run_mins, 
                                              dispatched_job_in_q, dispatched_job, g_job_cnt[self.job_set], self.job_set))

                                    job_id_idx += 1
                                    break
                    # while offlineJob.inst_cnt > 0:
                # while job_id_idx < len(job_list) and continue_dispatch:
                if (continue_dispatch):
                    job_step_idx += 1

            if (continue_dispatch):
                self.dispatch_job_list.extend(job_q_dispatch_soltion)
                dispatched_job += dispatched_job_in_q
                usable_machine_list = self.remove_heavy_load_machine(usable_machine_list)
            # while (job_step_idx < len(job_q) - 1): # 最后一项是估计的运行时间       
        # for job_q in sorted_job_csv:
        self.output_optimized()
        
        self.sorte_machine()
        cost = self.sum_scores_of_machine()
        for machine_id, machine_running_res in self.sorted_machine_res:
            logging.info('machine_%d,%f' % (machine_id, machine_running_res.get_machine_real_score()))
        print_and_log('cost of [%s] is %f/%f, cpu per is %f, total expanded idle machine  %d' % 
                      (self.job_set, cost, cost/SLICE_CNT, g_min_cpu_left_useage_per[self.job_set], 
                       self.total_expanded_idle_machine ))
        
    def remove_heavy_load_machine(self, usable_machine_list):
        tmp = []
        for machine_id in usable_machine_list:
            machine_running_res = self.machine_runing_info_dict[machine_id]
            if (not machine_running_res.is_heavy_load()):
                tmp.append(machine_id)
        
        print_and_log("remove_heavy_load_machine %d -> %d" % (len(usable_machine_list), len(tmp)))
        return tmp

    def extend_usable_machine_list(self, usable_machine_list, idle_machine_list):
        
        if (len(idle_machine_list) < g_extend_idle_machine_cnt):
            usable_machine_list.extend(idle_machine_list)
            return usable_machine_list, []
        
        print(getCurrentTime(), 'extend %s to usable machine list' % idle_machine_list[-g_extend_idle_machine_cnt:])
        for machine_id in idle_machine_list[-g_extend_idle_machine_cnt:]:
            usable_machine_list.append(machine_id)
            idle_machine_list.remove(machine_id)
            
        self.total_expanded_idle_machine += g_extend_idle_machine_cnt

        return usable_machine_list, idle_machine_list
            
            
        
    def dispatch_job_q_on_idle_machine(self, job_q):
        for job_q_idx in range(len(job_q) - 1): # 最后一项是估计的运行时间
            job_list = job_q[job_q_idx].split(':')  

            for job_id in job_list:
                offlineJob = self.offline_jobs_dict[job_id]
        
                
if __name__ == '__main__':
    job_set = sys.argv[1].split("=")[1]
    optimized_dispatch_file = sys.argv[2].split("=")[1]
    
    print("running DispatchOfflineJob... cpu per %f" % g_min_cpu_left_useage_per[job_set])

    dispatch_offline_job = DispatchOfflineJob(job_set, optimized_dispatch_file)
    dispatch_offline_job.dispatch_offline_jobs()
    
    
    