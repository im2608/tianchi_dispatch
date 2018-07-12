'''
Created on Jun 22, 2018

@author: Heng.Zhang
'''

from ResMgr import *

def main():
    print(getCurrentTime(), 'running...')
    res_mgr = MachineResMgr()
    
    inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
    i = 0
    for each_inst in inst_app_csv:
        inst_id = int(each_inst[0])
        # 初始化时指定机器的 inst 已经在 MachineResMgr.init_deploying 中处理过了，这里跳过
        if (len(each_inst[2]) > 0):
            continue

        if (not res_mgr.dispatch_inst(inst_id, None)):
            break

        if (i % 100 == 0):
            print(getCurrentTime(), ' %d instances handled\r' % (i), end='')
        i += 1

    res_mgr.output_submition()

    return

def normal_app_resource():
    normal_app_output_file = open(r'%s\..\output\normal_app_resources.csv' % runningPath, 'w')
    
    app_res_csv = csv.reader(open(r'%s\..\output\app_resources.csv' % runningPath, 'r'))
    for each_app in app_res_csv:
        app_id = each_app[0]
        cpu = max(list(map(float, each_app[1].split('|')))) # max cpu slice, 保留 cpu 最大使用量
        mem = max(list(map(float, each_app[2].split('|')))) # max mem slice， 保留 mem 最大使用量
        disk = int(each_app[3]) # disk
        p = int(each_app[4]) # p
        m = int(each_app[5]) # m
        pm = int(each_app[6]) # pm
        
        normal_app_output_file.write('%s,%f,%f,%f,%f,%f,%f\n' % \
                                     (app_id,
                                      cpu / MAX_CPU,
                                      mem / MAX_MEM,
                                      disk / MAX_DISK,
                                      p / MAX_P,
                                      m / MAX_M,
                                      pm / MAX_PM))
        
        
    normal_app_output_file.close()
    return

def c(m, n):
    z = 1
    for i in range(m, m-n, -1):
        z*=i
    fenmu = 1
    for i in range(1, n+1):
        fenmu *= i
    return z/fenmu


def reverse_machine():

    machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources.csv' % runningPath, 'r'))
    output_file = open(r'%s\..\input\machine_resources_reverse.csv' % runningPath, 'w')

    machine_res_list = []
    for each_machine in machine_res_csv:
        machine_res_list.append(each_machine)

    for i in range(len(machine_res_list)-1, -1, -1):
        output_file.write('%s\n' %','.join(machine_res_list[i]))
        
    output_file.close()

def sum_cpu_slice():
    app_res_dict = {}
    app_res_csv = csv.reader(open(r'%s\..\input\app_resources.csv' % runningPath, 'r'))
    app_cpu_sum = np.array(np.zeros(98))
    app_mem_sum = np.array(np.zeros(98))
    app_disk_sum = 0
    app_p_sum = 0
    app_m_sum = 0
    app_pm_sum = 0
    for each_app in app_res_csv:
        
        app_cpu_sum += np.array(list(map(float, each_app[1].split('|'))))
        app_mem_sum += np.array(list(map(float, each_app[2].split('|'))))
        app_disk_sum += int(each_app[3])
        app_p_sum += int(each_app[4])
        app_m_sum += int(each_app[5])
        app_pm_sum += int(each_app[6])
    
    return app_cpu_sum


if __name__ == '__main__':
    main()
