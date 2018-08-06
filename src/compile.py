'''
Created on Aug 1, 2018

@author: Heng.Zhang
'''

from global_param import *

import py_compile
# py_compile.compile(r'%s\cost_score.py' % runningPath, r'%s\..\src_pyc\cost_score.pyc' % runningPath)
# py_compile.compile(r'%s\ACS.py' % runningPath, r'%s\..\src_pyc\ACS.pyc' % runningPath)
# py_compile.compile(r'%s\Ant.py' % runningPath, r'%s\..\src_pyc\Ant.pyc' % runningPath)
# py_compile.compile(r'%s\AppRes.py' % runningPath, r'%s\..\src_pyc\AppRes.pyc' % runningPath)
# py_compile.compile(r'%s\global_param.py' % runningPath, r'%s\..\src_pyc\global_param.pyc' % runningPath)
# py_compile.compile(r'%s\MachineRes.py' % runningPath, r'%s\..\src_pyc\MachineRes.pyc' % runningPath)
# py_compile.compile(r'%s\MachineRunningInfo.py' % runningPath, r'%s\..\src_pyc\MachineRunningInfo.pyc' % runningPath)
# py_compile.compile(r'%s\ResMgr.py' % runningPath, r'%s\..\src_pyc\ResMgr.pyc' % runningPath)


def cal_time():
    a = [i for i in range(6000)]
    j = 0
    s = time.time()
    for i in a:
        j += 2
        
    e = time.time()
    
    print('used %d' % (e - s))
        
    
cal_time()        