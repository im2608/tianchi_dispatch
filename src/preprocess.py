#coding=utf-8
'''
Created on Jul 18, 2018

@author: Heng.Zhang
'''
from global_param import *
import csv
from contextlib import contextmanager
import contextlib

def app_classification():

    print(getCurrentTime(), 'loading app_resources.csv...')
    app_res_dict = {}
    app_res_csv = csv.reader(open(r'%s\..\input\%s\app_resources.csv' % (runningPath, data_set), 'r'))
    for each_app in app_res_csv:
        app_id = int(each_app[0].split('_')[1])
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
         
@contextmanager
def mytimer():
    s = time.time()
    yield()
    e = time.time()
    return e-s

    
import multiprocessing
import os    
import threading
import time
import signal

def func_a(a, b):
    print('a is ', a, ' b is ', b, ' form pid %d, ppid %d' % (os.getpid(), os.getppid()))
    
h = {1:2, 2:3, 3:4}    
def test_mp():    
    p = multiprocessing.Process(target = func_a, args=(1, h))
    p.start() 
    
    print('main end from pid ', os.getpid())
    
    
pid_set = set()    
def chldHandler(signum,stackframe):
    while 1:
        try:
#             result = os.waitpid(-1,os.WNOHANG)
            result = os.wait()
        except:
            print('os.waitpid exception')
            break
        print('Reaped child process %d' % result[0])
        if (result[0] in pid_set):
            pid_set.remove(result[0])

def test_fork():
    number = 120
    cpu_cnt = multiprocessing.cpu_count()
    each_cnt = number // cpu_cnt
    print('cpu count %d, each count %d' % (cpu_cnt, each_cnt))
    start = 0
    main_print_once = False
    
#     signal.signal(signal.SIGCHLD, chldHandler)
    
    for sub_idx in range(0, cpu_cnt):
        start = sub_idx * each_cnt
        end = start + each_cnt
        if (sub_idx == cpu_cnt - 1): # last sub-process
            end = number

        pid = os.fork()
        is_main_process =True
        
        if (pid != 0): # parent, pid is child pid
            pid_set.add(pid)
            if (not main_print_once):
                start = 0
                end = start + each_cnt
                pid = os.getpid() 
                print('main process pid %d, start %d, end %d' % (pid, start, end))
                main_print_once = True
        else:
            is_main_process = False
            pid = os.getpid() 
            print('sub-process pid %d, start %d, end %d' % (pid, start, end))
            time.sleep(1)
            break

    if (is_main_process):
        while(len(pid_set) > 0):                
            try:
    #             result = os.waitpid(-1,os.WNOHANG)
                result = os.wait()
            except:
                print('os.waitpid exception')
            
            if (result[0] in pid_set):
                pid_set.remove(result[0])
                print('child process %d ended' % result[0])
                
            time.sleep(1)
    
    
if __name__ == '__main__':
#     test_fork()
    app_classification()