'''
Created on Jun 22, 2018

@author: Heng.Zhang
'''

from ResMgr import *

def load_data():
    inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
    inst_app_dict = {}
    for each_inst in inst_app_csv:
        if (len(each_inst[2]) > 0):
            inst_id = each_inst[0]
            inst_app_dict[inst_id] = [each_inst[1], each_inst[2]]

    app_constraint_dict = {}
    app_cons_csv = csv.reader(open(r'%s\..\input\app_interference.csv' % runningPath, 'r'))
    for each_cons in app_cons_csv:
        app_constraint_dict[each_cons[0]] = (each_cons[1], each_cons[2])

    return


def main():
    res_mgr = MachineResMgr()
    
    inst_app_csv = csv.reader(open(r'%s\..\input\instance_deploy.csv' % runningPath, 'r'))
    i = 0
    for each_inst in inst_app_csv:
        inst_id = each_inst[0]
        if (not res_mgr.dispatch_inst(inst_id, None)):
            break

        if (i % 1000 == 0):
            print(getCurrentTime(), ' %d instances handled\r' % (i), end='')
        i += 1

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
if __name__ == '__main__':
    main()
