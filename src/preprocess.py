'''
Created on Jul 18, 2018

@author: Heng.Zhang
'''
from global_param import *
import csv

def app_classification():

    print(getCurrentTime(), 'loading app_resources.csv...')
    app_res_dict = {}
    app_res_csv = csv.reader(open(r'%s\..\input\%s\app_resources.csv' % (runningPath, data_set), 'r'))
    for each_app in app_res_csv:
        app_id = int(each_app[0])
        cpu_slice = np.round(np.array(list(map(float, each_app[1].split('|')))).mean(), 4)
        mem_slice = np.round(np.array(list(map(float, each_app[2].split('|')))).mean(), 4)
        disk = int(float(each_app[3]))
        p = int(each_app[4])
        m = int(each_app[5])
        pm = int(each_app[6])
        
        app_res_tup = (cpu_slice)#, mem_slice, disk, p, m, pm)
        
        if (app_res_tup not in app_res_dict):
            app_res_dict[app_res_tup] = 0

        app_res_dict[app_res_tup] += 1
        
    sorted_app_res_tup = np.sort(list(app_res_dict.keys()))
    
    with open(r'%s\..\log\app_classification_%s.txt' % (runningPath, data_set), 'w') as outputfile:
        for app_res_tup in sorted_app_res_tup:
            outputfile.write('%s,%d\n' % (app_res_tup, app_res_dict[app_res_tup]))
            
    return

def corss_big_small_machine():
    machine_res_csv = csv.reader(open(r'%s\..\input\machine_resources.csv' % runningPath, 'r'))
    
    machine_runing_info = []

    for each_machine in machine_res_csv:
        machine_runing_info.append(each_machine)
        
    output_file = open(r'%s\..\input\machine_resources_cross.csv' % (runningPath), 'w')

    for i in range(0, 3000):
        output_file.write(",".join(machine_runing_info[i]) + '\n')
        output_file.write(",".join(machine_runing_info[i + 3000]) + '\n')
        
    output_file.close()
         
    
if __name__ == '__main__':
    app_classification()