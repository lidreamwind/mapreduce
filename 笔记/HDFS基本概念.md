# 第一节 HDFS重要概念

## 典型的Master/Slave架构

HDFS的架构是典型的Master/Slave架构。

HDFS集群往往是一个NameNode(HA架构会有两个NameNode，联邦机制)+多个DataNode组成。

NameNode是集群的主节点，DataNode是集群的从节点。

## 分块存储

HDFS中的文件在物理上是分块存储(block)的，块的大小可以通过dfs.blocksize控制。

hadoop2.x默认块是128M。

## 命名空间

HDFS支持传统的层次性文件组织结构。用户或应用程序可以创建目录，然后将文件保存在目录里。

文件系统名字空间的层次结构和大多数现有的文件系统类似，用户可以创建、删除、移动或重命名。

NameNode辅助文件系统的名字空间，任何对文件系统名字空间或属性的修改都被NameNode记录下来。

HDFS提供给客户一个抽象目录树，访问形式：hdfs://namenode:port/

## NameNode元数据管理

目录结构以及文件分块的信息有NameNode维护。

NameNode记录每个文件所对应的block信息(block的id,Datanode信息)

## DataNode数据存储

文件的具体存储管理有DataNode节点承担。一个block会有多个节点维护存储。

DataNode定时向NameNode汇报持有的block信息。

## 副本机制

- 为了容错所有的block都会有副本，每个文件的block大小和副本系数都是可以配置的。
- 应用程序可以指定某个文件的副本数目。
- 副本系数可以在文件创建的时候指定，也可以在之后改变。
- 副本数量默认是3个。
- 副本数量取决于DataNode的数量。

![image-20210319182511514](.\图片\副本与DataNode关系.png)

## 一次写入多次读出

HDFS是设计成适应一次写入，多次读出的场景，不支持文件的随机修改。(支持追加写入，不支持随机更新)

适合做大数据分析的底层存储服务，并不适合做网盘等应用。



# 第二节 HDFS Shell命令-架构

## 架构图

![image-20210319175541597](.\图片\HDFS架构.png)

### NameNode

维护管理HDFS的命名空间(NameSpace)

维护副本策略

记录文件块的映射信息

负责处理客户端读写请求

### DataNode

保存实际的数据块

负责数据块的读写

### Client

上传文件到HDFS的时候，Client负责将文件切分成Block，然后上传

请求NameNode交互，获取文件的位置信息

读取或写入文件，与DataNode交互

Client可以使用一些命令来管理或者访问HDFS



## shell命令

hadoop fs  或者 hdfs dfs 

### 查看命令帮助

​		hadoop fs -help cat

### 创建目录

​	 	hadoop fs -mkdir -p /lagou/bigdata

### 从本地剪切hdfs

​		hadoop fs -moveFromLocal ./tt.txt /lagou/bigdata

### 从本地复制到hdfs

​		hadoop fs -copyFromLocal ./tt.txt /lagou/bigdata

​		hadoop fs -put./tt.txt /lagou/bigdata

### 从hdfs拷贝到本地

​		hadoop fs -copyToLocal  /lagou/bigdata/remote.txt ./

​		hadoop fs -get /lagou/bigdata/remote.txt ./

### 查看列表

​		hadoop fs -ls

### 追加

​		hadoop fs -appendToFile  ./local.txt /lagou/bigdata/remote.txt

### 查看文件

​		hadoop fs -cat /lagou/bigdata/remote.txt



### hdfs拷贝

​		hadoop fs -cp /lagou/bigdata/remote.txt /

### 移动

​		hadoop fs -mv /remote.txt /lagou/bigdata



### 删除

​		hadoop fs -rm /lagou/bigdata			

​		hadoop fs -rm -r -f /lagou/bigdata	

​		hadoop fs -rmdir /lagou/bigdata 		删除空目录



### 统计大小

​		hadoop fs -du -s -h /



### 改变副本数量

​		hadoop fs -setrep 10 /lagou/bigdata/tt.txt

![image-20210319182317396](.\图片\副本与DataNode关系.png)