天池调度算法比赛， 初赛 36， 复赛18

复赛代码分支： dispatch_semifinal

运行方法：
1. 创建目录： 
    Project/input/sf，  天池上下载的输入文件放在这里 
    Project/output/sf， 输出文件会生成在这里
    Project/log  日志目录
        
2. 源代码放到 Project/src 中    

3. 进入 src 目录， 运行：
    python cost_score.py set=X  X = [a,b,c,d,e]， 调度 online job
        得到各个 job set online job 的调度结果, 输出到 Project/output/sf中, 文件格式为 X_optimized_YYYYmmdd_HHMMSS.csv
        
    运行 topological_sort(job_set) (dispatch_sf.py) 函数， 会对 offline job 进行拓扑排序， 生成  sorted_job.X.csv, 该文件作为调度 offline job 的输入文件之一
        
    python DispatchOffline.py set=X file=X_optimized_YYYYMMDD_HHMMSS.csv   调度 offline job
    X = [a,b,c,d], file为前一步得到的 online job 调度结果， 在此基础上调度 offline job， 代码会到 output 目录中读取该文件
    
调度 online job 流程：
复赛题目中增加了一个要求, 即同一轮内所有的迁移同时进行，在迁移实例之前先在目标机器上创建实例的副本，创建成功后再在源机器上删除原实例， 这里边隐含了一个约束： 即使迁移完成后目标机器上的实例都符合约束，但迁移过程中在目标机器上创建被迁移实例的副本时，仍有可能与目标机器上正在运行的实例发生冲突。 例如：
inst_A， machine_A,  inst_A 运行在 machine_A 上
inst_B, machine_B   inst_B 运行在 machine_B 上
约束： <inst_A, inst_B, 0> 表示 inst_A 与 inst_B 不能同时运行在同一台机器上
现有迁移方案  
inst_A -> machine_B,  inst_A 迁移到  machine_B 
inst_B -> machine_C   inst_B 迁移到  machine_C
即使迁移完成后所有的实例都符合约束，但在迁移 inst_A的过程中，需先在 machine_B 上创建 inst_A 的副本， 而此时 inst_B还没有迁出， 这就会违反约束 <inst_A, inst_B, 0>， 导致迁移失败.
我对这个问题的解决方法是如果本轮在某机器上有实例迁出，那么本轮就不会向该机器上迁入实例, 这样就避免了这个问题。 函数 migrate_machine_dp 的第二个参数指明了这点。

调度 online job采用了 First Fit + 动态规划进行， 题目要求只能进行三轮调度， 所以三轮采用了 分散 -- 集中 -- 分散的策略
第一轮将重负载机器上的实例随机迁移到轻负载的机器上直到重负载的机器成为轻负载的。 该方法来自于我之前在一家电商工作时，有一次去库房参观，讯问如何尽快地将入库的商品上架，得到的回答是，“看哪里有合适的货架就放哪里", 翻译成代码的语言就是随机将实例迁移到符合条件的机器上。

第二轮使用动态规划的方法，采用集中的策略， 将轻负载机器上的实例尽量迁出，使其成为空闲机器。 使用动态规划算法搜索所有的可行迁移方案，但在本题目中，可行迁移方案的数量随着机器上实例的数量呈指数级增长，例如在某机器上有
inst1, inst2, inst3 三个实例，需要将它们迁移到其他机器上， 并且迁入目标机器后所增加的分数最小， inst1, inst2, inst3 分别有 100， 200， 100台可迁入的机器，那么可行的迁移方案共有 100*200*100 种，在可行时间内无法完成这样的搜索。 但是进一步研究发现， 在搜索迁移方案中，如果某两个方案的迁移代价（即将实例迁入后所增加的分数）相同，那么在这两个方案的基础上继续进行搜索所得到的其他方案的迁移代价也相同，例如：
inst1 有 2 种迁移方案（即有 2 台机器都可以将 inst1 迁入），但是这 2 种迁移方案的迁移代价相同， 那么在迁移 inst1 的基础上迁移 inst2， 不论将 inst2 迁入到这 2 台机器上的那一台，迁移 inst1， inst2 的最终代价都是相同的。 在此基础上，可行迁移方案的数量减少为原来的 10% 左右。 但即使如此，搜索空间仍然巨大，在有 3 个实例的情况下，搜索空间已经为千万级别。 经过综合考量， 我把运行实例数 <= 3 作为轻负载机器，尝试将他们迁移到其他机器上, 并且采 fork 多子进程的方式并行搜索。


第三轮同样使用动态规划的方法，采用分散的策略，将负载最重的机器上的 cpu 占用最高的的 3 个实例尝试迁移到其他机器中.

经过以上步骤， online 的分数为： a: 4865，  b: 4878, c: 6901, d: 6870, e: 9760 

调度 offline ：
调度 offline 采用贪心算法调度 offline job， 每台机器 cpu 的最小剩余量在 g_min_cpu_left_useage_per 中指定， 意为如果某台机器上的 cpu 的剩余容量小于该值，则不再往该机器上调度 offline job。 若现有的有负载机器都无法容纳 offline job， 则尝试使用一台空闲的机器。

offline 的分数为： a : 5285, b: 5467, c: 7193, d: 7125

final score = (5285+5467+7193+7125+9760)/5 = 6966 


其他：

ACS， Ant 为蚁群算法的实现. 可直接运行：
python ACS.py

