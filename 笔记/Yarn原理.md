# Yarn架构

![image-20210331223815524](.\图片\Yarn-架构.png)

## Yarn作业提交流程

![image-20210402204000976](.\图片\Yarn流程.png)

作业提交过程

​	作业提交：

​		1、Client调用job.waitForComplietion方法，向整个集群提交MapReduce作业。

​		2、Client向RM申请一个作业id。

​		3、RM向Client返回该job资源的提交路径和作业id。

​		4、Client提交Jar包、切片信息和配置文件到指定的资源提交路径

​		5、client提交完资源后，向RM申请运行MrAppMaster。

​	作业初始化：

​		6、当RM收到Client的请求后，将该Job添加到容量调度其中。

​		7、某一个空闲的NN领取到该Job。

​		8、该NM创建Container，并产生MRAppMaster。

​		9、下载Client提交的资源到本地。

​	任务分配：

​		10、MrAppMaster向RM申请运行多个MapTask任务资源。

​		11、RM将运行MapTask任务分配给另外两个NodeManger，另外两个NM领取任务创建容器。

​	任务运行：

​		12、MR向两个接收任务的NM发送程序启动脚本，这两个NM分别启动MapTask，MapTask对数据分区排序

​		13、MrAppMaster等待所有MapTask运行完毕，向RM申请容器，运行ReduceTask。

​		14、ReduceTask向MapTask获取对应分区数据

​		15、程序运行完成后，MR向RM申请注销自己。

​	

进度和状态更新，Yarn中的任务将其进度和状态返回给应用管理器，客户端每秒（通过mapreduce.client.progressmonitor.pollinterval设置）向应用管理器请求进度更新，展示给用户。

#  Yarn调度策略

FIFO、Capacity Scheduler、Fair Scheduler

##  FIFO

​	先进先出，按照任务到达时间排序。

​	![image-20210402210342890](.\图片\yarn-FIFO.png)

## 2、Capacity Scheduler

​	容量调度器，默认的调度策略，允许多个组织共享整个集群，多队列模式。

![image-20210402210508180](.\图片\yarn-capacity-scheduler.png)

## 3、Fair Scheduler

​	公平调度器。CDH版本的默认策略。

​	当A启动一个job而B没有任务是，A会获得全部集群资源，当B启动一个job后，A的job会继续运行，不一会两个任务共享集群资源各占一半。



# Yarn多租户资源隔离配置

​	Yarn集群资源设置为A、B两个队列。

​	A队列设置占用资源70%主要运行常规的定时任务。

​	B队列设置占用30%主要临时任务。

​	两个队列间可以相互资源共享，假如A队列资源占满，B队列资源比较充裕，A队列可以使用B队列的资源，使总体做到资源利用最大化。

​	使用Fair scheduler调度策略。

​	

 ## 具体配置

​	1、指定调度策略

​			<property>

​					<name>yarn.resourcemanager.scheduler.class</name>

​					<value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>

​			</property>

​	2、创建Fair-scheduler.xml文件

​		![image-20210402211506078](.\图片\Yarn公平调度配置.png)



# NameNode启动流程源码



# DataNode启动流程



# NameNode如何支持高并发访问

高并发的难点在于数据的多线程安全和操作效率。

**多线程安全**

​	1、写入数据到edits_log必须保证每条edits都有一个全局顺序递增的transactionId，这样可以标识出来 一条条的edits的先后顺序。

​	2、如果要保证每条 edits的txid是递增的，就必须得加同步锁。每个线程修改了元数据，都要写一条edits的时候，都必须按顺序排队获取锁后，才会生成递增的txid，代表这次要写的edits的序号。

**产生问题**

​	如果每次都是在一个加锁的代码块里，生成txid，然后写磁盘文件edits log，这种既有同步锁又有写磁盘操作非常耗时。

**HDFS优化解决方案**

​	1、串行化，使用分段锁

​	2、写磁盘，使用双缓冲

分段加锁机制

​	首先各个线程依次第一次获取锁，生成顺序递增的txid，然后将edits写入内存双缓冲的区域，接着就立马第一次释放锁了。趁着这个空隙，后面的线程可以立马第一次获取锁，然后立即写自己的edits到内存缓冲。



双缓冲机制

​	程序冲开辟两份一模一样的内存空间，一个名为bufCurrent，产生的数据直接写入到这个bufCurrent，而另一个是bufReady，在bufCurrent数据写入（达到一定标准）后，两片内存就会exchang（交换）。直接交换缓冲的区域1和区域2，保证客户端请求都是操作内存而不是同步写磁盘。

![image-20210403084834704](.\图片\NameNode的高并发支持.png)

# Hadoop3.x新特性

​	1、MapReduce基于内存+id+磁盘，共同处理数据

​	2、HDFS可擦出编码、多NameNode支持、MR Native Task优化、Yarn基于cgroup的内存和磁盘IO隔离、Yarn Container resizing等。



## Hadoop3.x新特性之Common改进





## yarn改进



## MapReduce改进

















