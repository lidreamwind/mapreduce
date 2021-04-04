# HDFS读写

## HDFS读取数据流程

![image-20210322195209571](.\图片\HDFS读数据.png)

1.客户端通过Distributed FileSystem向NameNode请求下载文件，NameNode通过查询元数据，找到文件块所在的DataNode地址

2.挑选一台DataNode(就近原则，然后随机)服务器，请求读取数据。

3.DataNode开始传输数据给客户端，（从磁盘里面读取数据输入流，以Packet为单位来做校验）

4.客户端以Packet为单位接收，现在本地缓存，然后写入目标文件

## HDFS写数据流程

![image-20210322212746882](.\图片\HDFS写数据.png)

1、客户端通过Distributed FileSystem 模块向NameNode请求上传文件，NameNode检查目标文件是否存在，父目录是否存在。

2、NameNode返回是否可以上传

3、客户端请求第一个Block上传到哪几个DataNode服务器上

4、NameNode返回3个DataNode节点，dn1，dn2，dn3

5、客户端通过FSDataOutputStream模块请求dn1上传数据，dn1收到请求会继续调用dn2，然后dn2调用dn3，将这个通信管道建立完成。

6、dn1、dn2、dn3逐级应答客户端

7、客户端开始向dn1传第一个Block（先从磁盘读取数据放到一个本地内存缓存），以Packet为单位，dn1收到一个Packet就回传给dn2，dn2传给dn3，dn1没传入一个Packet会放入一个确认队列等待确认。

8、当一个Block传输完成后，客户端再次请求NameNode上传第二个Block的服务器。（重复执行3-7）

Packet大小是64kb。

# HDFS元数据管理机制之NN和2NN

**问题：NameNode如何管理和存储元数据**

计算机存储数据：内存或者磁盘。

元数据存储磁盘：不能满足客户端对元数据低延迟的响应，安全性高。

元数据存储内存：可以高校查询以及快速响应客户端的查询请求，数据易丢失。

解决方案：内存+磁盘。NameNode内存+FsImage的文件+edits文件

**问题：磁盘和内存中的数据如何划分**

一模一样：需要保证内存和文件中的数据一致性，操作FsImage文件操作效率不高。

两个合并=完整数据：NameNode引入了edits文件（日志文件：只能追加写入），edits文件记录的client的增删改操作。

​	引用2NN对edits和FsImage进行合并，不再让NameNode把数据dump出来形成FsImage文件。



## 元数据管理流程

![image-20210323212525969](.\图片\NameNode启动.png)

**第一阶段，NameNode启动**

- ​	第一次启动NameNode格式化后，会创建FsImage和Edits文件。如果不是第一次启动，会直接加载editis文件和镜像文件到内存
- ​	客户端对元数据进行增删改的请求
- ​	NameNode记录操作日志，更新滚动日志
- ​	NameNode在内存中对数据进行增删改

**第二阶段，Secondary NameNode工作**

​	辅助NameNode生成FsImage文件。

- ​	Secondary NameNode询问NameNode是否需要CheckPonit，直接带回NameNode是否执行检查点操作结果
- ​	Secondary NameNode请求执行CheckPoint
- ​    NameNode滚动正在写的edits日志

## FsImage和Edits文件解析

![image-20210323214357444](.\图片\HDFS-Image.png)

seen_txid记录了Fsimage的最新编号，为了管理edits文件。

VERSION文件如下图。

![image-20210323214943972](.\图片\VERSION.png)

### FsImage文件内容

​	hdfs oiv -p XML -i fsimage_000000001545  -o /root/image1545.xml

​	-p指定导出的文件类型 

​	-i指定导出哪一个fsImage

   ![image-20210323220302373](H:\拉勾-大数据系统资料与教程\hadoop\图片\FsImage内容.png)

在内存中有记录块所对应的datanode信息，但是Fsimage中就踢出了这个信息；HDFS集群启动的时候会加载image和edits文件，block没有记录。

集群启动时会有一个安全模式(safemode)，安全模式就是为了让datanode汇报自己所持有的block信息给NameNode来补全。后续DataNode会定时汇报block信息。

### Edits文件内容

​	hdfs oev -p XML -i edits_00000000001543-000000001231 -o /root/edits.xml

​	-p指定导出的文件类型 

​	-i指定导出哪一个fsImage

**确定那些edits需要合并**

fsImage文件后边的的编号，明确那些edits文件没有进行合并。



## HDFS元数据CheckPoint周期

[hdfs-site.xml]

<!-- 定时一小时 -->

<property>

​	<name>dfs.namenode.checkpoint.period</name>

​	<value>3600</value>

</property>

<!-- 一分钟检查一次操作数，当操作数达到一百万时，Secondary NameNode执行一次 -->

<property>

​	<name>dfs.namenode.checkpoint.txns</name>

​	<value>1000000</value>

​	<description>操作动作次数</description>

</property>

<property>

​	<name>dfs.namenode.checkpoint.check.period</name>

​	<value>60</value>

​	<description>1分钟检查一次操作次数</description>

</property>

# NN故障处理

NameNode故障后，HDFS集群就无法正常工作，因为HDFS文件系统的元数据需要NameNode来管理维护并与Client交互，如果元数据出现损坏和丢失同样会导致NameNode无法正常工作进而HDFS文件系统无法对外正常提供服务。

恢复方法：

 1、将2NN的元数据拷贝到NN的节点下，此方法会导致元数据丢失。

 2、搭建HDFS的HA集群，解决NN的单点故障问题。

# HDFS的限额、归档、安全模式

## 文件限额

​	    HDFS文件的限额配置允许我们以文件大小或者文件个数来限制我们在某个目录下上传的文件数量或文件内容总量，以便达到我们类似百度网盘等限制每个用户允许上传的最大的文件数量。

### 数量限额

​		hdfs dfsadmin -setQuota  数量 目录    允许目录上传文件数量是  数量-1

​		hdfs dfsadmin -setQuota 2  /lagou     允许/lagou目录最多上传1个文件

​	**清除**

​		hdfs dfsadmin -clrQuota /test	

### 空间大小限制

​	  hdfs dfsadmin -setSpaceQuota   大小 目录

 	 hdfs dfsadmin -setSpaceQuota 4k /test   设置/test目录允许上传最大4k的文件

​	**清除**

​	   hdfs dfsadmin -clrSpaceQuota  /test



## 安全模式

​		HDFS集群启动时需要加载FsImage和edits文件，而这两个文件都没有记录block对应的DataNode节点信息，。

​		安全模式是HDFS所处的一种特殊状态，在这种状态下文件系统只接受读数据请求，而不接受删除、修改等变更请求。在NameNode主节点启动时，DataNode在启动的时候汇报可以用的block等状态。当整个系统达到安全标准时，HDFS自动离开安全模式。如果HDFS处于安全模式下，则文件block不能进行任何副本复制操作。

​		HDFS集群刚启动的时候，默认30秒钟时间是出去安全期的，只有过了30S之后，脱离了安全期，，然后才可以对集群进行操作。

​		hdfs dfsadmin -safemode enter|leave |get | foreExit

## hadoop归档技术

​		由于大小小文件会占用NameNode内存，因此对于HDFS来说存储大量小文件造成NameNode内存资源的浪费。

​		Hadoop归档文件HAR，是一个高效的文件存档工具，HAR文件是由一组文件工作archive工具创建得来，不仅减少了NameNode的内存使用，而且对文件进行透明的访问，而且对外是一个独立的文件。

​		

**命令**

​	指定归档命令会启动MapReduce任务，需要启动yarn。

hadoop archive  <-archiveName <Name>.har>  <-p <parent path>> [-r <replication factor>] <src>*  <dest>

hadoop archive -archiveName input.har -p /wcinput /output/

**har文件查看**

hdfs dfs -lsr /output/input.har

hdfs dfs -ls -R /output.input.har

hdfs dfs -ls -R har:///output/input.har  查看原始的数据文件

hdfs dfs -cp har:///output/input.har/*  /wcoutput   提取inpu.har中的文件