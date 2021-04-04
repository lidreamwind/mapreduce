# MapReduce原理分析

## MapTask运行机制详解

![image-20210328092136442](.\图片\MapTask流程.png)

**多次溢写会有多个文件，，会有一个文件的合并(归并排序)**

**使用RecordReader对splits进行读取数据，将数据传给Map方法。通过换行符确定数据范围.**

切片是逻辑切分，HDFS的分块是物理切分。

​	1.读取数据组件InputFormat(默认是TextInputFormat)通过getSplits方法对输入目录中文件进行逻辑切片得到splits，有多少个splits就对应多少个MapTask。splits和block对应关系默认是一对一。

​	2.输出文件切分为splits后，RecordReader进行读取（默认是LineRecordReader）返回一行数据<key,value>。key是每行首字母偏移量，value是一行文本内容

​	3.将2步骤读取的<key,value>值放入自定义的mapper类中，调用map函数。

​	4.调用map函数，会通过上下文context将数据进行收集collect，先对其进行分区处理，默认使用HashPartitioner。

​	5.collect收集数据后会将数据写入内容—环形缓冲区，<key,value>在此都会被序列化成字节数组。

​		环形缓冲区是一个数组，里面放着key、value的序列化数据和key、value的元数据信息，包括partition、key的起始位置、value的起始位置以及value的长度。

​		缓冲区默认是100MB，当map 任务输出结果很多时，会发生溢写（spill）操作，将数据临时写入磁盘。溢写操作由单独的线程来完成。溢写操作触发有个环形缓冲区的比例—0.8。map的输出结果可以继续向剩下的0.2内存中写入数据。

​	6.溢写进程启动后，需要对0.8空间内的key做排序。

​	7.合并溢写文件。当map任务结束后会对每个map任务的溢写文件进行合并，最终保留一个文件。并提供一个索引文件一路每个reduce对应数据的偏移量。

### 参数设置

mapreduce.task.io.sort.factor=10，默认1mb

mapreduce.task.io.sortmb=100

mapreduce.map.sort.spill.percent=0.80

## MapTask并行度

**一个split分片对应一个MapTask任务**

split分片大小默认是block大小。splitSize = blockSize； BlockSize=128

**split设置为128M原因**

![image-20210328095818505](.\图片\split分片设置为128M原因.png)

**切片的计算方式**

​	按照文件逐个进行计算。

​	优先移动计算，而非移动数据。

## 切片机制源码解读

FileInputFormat -> getSplits

mapreduce.input.fileinputformat.split.minsize=0 默认值，，分片

mapreduce.input.fileinputformat.split.maxsize ---没有此属性的默认值。

![image-20210328101500916](.\图片\分片源码.png)

## ReduceTask工作原理

![image-20210328163650075](.\图片\ReduceTask流程.png)

1、Copy阶段，从各个MapTask上远程拷贝一片数据，若一片数据大小超过一定阈值，则写到磁盘上，否则放到内存中。

2、Merge阶段，从远程拷贝数据的同时，ReduceTask启动了两个后台线程对内存和磁盘上的文件进行合并，以防止内存食用过多或磁盘上的文件过多。

3、Sort阶段，按照MapReduce语义，用户编写reduce函数输入数据是按key进行聚集的一组数据。为了将key相同的数据聚集在一起，hadoop采用了基于排序的策略。ReduceTask只需对所有数据进行一次归并排序即可。

4、Reduce阶段，reduce函数将计算结果写到HDFS上。

## ReduceTask并行度

ReduceTask的并行度同样影响整个Job的执行并发度和执行效率，但与MapTask的并发数有切片决定不同，ReduceTask数量的决定是手动设置。

在 Driver设置。

​		job.setNumberReduceTask(4);

**注意事项**

​	1、ReduceTask=0，表示没有Reduce阶段，输出文件数和MapTask数量保持一致；

​	2、ReduceTask数量不设置默认就是一个，输出文件数量为1

​	3、如果数据分布不均匀，可能会产生数据倾斜

## Shuffle机制-和排序

![image-20210328170627626](.\图片\Shuffle流程.png)

### 排序

**排序是MapReduce框架中最重要的操作之一**

MapTask和ReduceTask均会对数据按照key进行排序，属于默认行为。默认排序是按照字典顺序排列，实现排序的方法是快速排序。

**MapTask**

- ​	将结果暂时放到环形缓冲区中，当缓冲区达到一定阈值后，对缓冲区数据进行一次快速排序，并将有序数据写入到磁盘
- ​	对溢写的文件进行归并排序

**ReduceTask**

​	当所有数据拷贝完成后，ReduceTask统一对内存和磁盘上的所有数据进行一次归并排序。

1、部分排序

​	MapReduce根据输入记录的键值对数据集排序，保证输出的每个文件内部有序。

2、全排序

​	最终输出结果只有一个文件，且文件内部有序。实现方式是只设置一个ReducerTask。效率低下。

3、辅助排序：GroupingComparator分组

​	在Reduce端对key进行分组。

​	应用于：在接收的key为bean对象是，想让一个或几个字段相同（全部字段比较不相同）的key进入到同一个reduce方式，可以采用分组排序。

4、二次排序

​	在自定义排序过程中，如果是compareTo中的判断条件为两个，，，即为二次排序。



需要实现WritableComparable接口

#### Reduce

```java
package com.lagou.writable.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SpeakBean, NullWritable,SpeakBean, NullWritable> {
    //reduce方法的调用是相同key的value组成一个集合调用一次
    /*
    java中如何判断两个对象是否相等？
    根据equals方法，比较还是地址值
     */
    @Override
    protected void reduce(SpeakBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //讨论按照总流量排序这件事情，还需要在reduce端处理吗？因为之前已经利用mr的shuffle对数据进行了排序
        //为了避免前面compareTo方法导致总流量相等被当成对象相等，而合并了key，所以遍历values获取每个key（bean对象）
        for (NullWritable value : values) { //遍历value同时，key也会随着遍历。
            context.write(key, value);
        }
    }

}
```

#### Mapper

```java
package com.lagou.writable.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,SpeakBean, NullWritable> {
    /*
   1 转换接收到的text数据为String
   2 按照制表符进行切分；得到自有内容时长，第三方内容时长，设备id,封装为SpeakBean
   3 直接输出：k-->设备id,value-->speakbean
    */
    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 转换接收到的text数据为String,并进行切分
        String[] fields = value.toString().split("\t");
        //设备id
        String serviceId = fields[1];
        //自有内容时长
        Long selfTime = Long.valueOf(fields[fields.length-3]);
        //第三方内容时长
        Long thirdTime = Long.valueOf(fields[fields.length-2]);
        SpeakBean speakBean = new SpeakBean(serviceId, selfTime, thirdTime);
        //  直接输出：k-->speakbean,value-->NullWritable
        this.text.set(serviceId);
        context.write(speakBean,NullWritable.get());
    }
}
```

#### Bean

```java
package com.lagou.writable.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//这个类型是map输出kv中key的类型，需要实现writable序列化接口
//若是Mapper中的key带有bean对象，那么也需要实现Comparable接口
public class SpeakBean implements WritableComparable<SpeakBean> {

    //定义属性
    private String serviceId; // 设备id
    private Long selfTime; //自有内容播放时长
    private Long thirdTime; // 第三方播放内容时长
    private Long totalTime;  //总时长

    //准备一个空参构造，反序列化时需要使用
    public SpeakBean() {
    }
    //有参构造， 用于创建对象
    public SpeakBean(String serviceId, Long selfTime, Long thirdTime) {
        this.serviceId = serviceId;
        this.selfTime = selfTime;
        this.thirdTime = thirdTime;
        this.totalTime = this.selfTime + this.thirdTime;
    }

    //序列化方法:就是把内容输出到网络或者文本中
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(serviceId);
        dataOutput.writeLong(selfTime);
        dataOutput.writeLong(thirdTime);
        dataOutput.writeLong(totalTime);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.serviceId = dataInput.readUTF();
        this.selfTime = dataInput.readLong();
        this.thirdTime = dataInput.readLong();
        this.totalTime = dataInput.readLong();
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public Long getSelfTime() {
        return selfTime;
    }

    public void setSelfTime(Long selfTime) {
        this.selfTime = selfTime;
    }

    public Long getThirdTime() {
        return thirdTime;
    }

    public void setThirdTime(Long thirdTime) {
        this.thirdTime = thirdTime;
    }

    public Long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(Long totalTime) {
        this.totalTime = totalTime;
    }
    //为了方便观察数据，重写toString()方法

    @Override
    public String toString() {
        return  "serviceId='" + serviceId +
                "\t selfTime=" + selfTime +
                "\t thirdTime=" + thirdTime +
                "\t totalTime=" + totalTime;
    }

    @Override
    public int compareTo(SpeakBean o) { // 返回值 ：0-相等 1-小于 -1：大于
        // 指定按照bean对象的总时长字段进行比较
        if(this.totalTime>o.totalTime){
            return -1;
        }else if(this.totalTime<this.totalTime){
            return 1;
        }else {
            return 0;
        }
    }
}
```

#### Job-Driver

```java
package com.lagou.writable.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"sortDriverTask");
        //设置jar包本地路径,设置Driver的类
        job.setJarByClass(SortJob.class);
        // 设置Mapper信息
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(SpeakBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置Reducer信息
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(SpeakBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置输出和输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 启动任务
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
```



## Shuffle的Combiner组件

Combiner预聚合的位置。

![image-20210328185314649](.\图片\Shuffle的Combiner位置解析.png)

1、Combiner是MR程序中Mapper和Reducer之外的一种组件

2、Combiner组件的父类是Reducer

3、Combiner和Reducer的区别在于运行的位置

4、Combiner是在每一个maptask所在的节点运行

5、Combiner是对每个maptask进行一个局部汇总，减少局部网络传输量

### Reducer

```java
package com.lagou.writable.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer number = 0;
        for (IntWritable value : values) {
            number = number+ value.get();
        }
        context.write(key,new IntWritable(number));
    }
}
```

### mapper

```java
package com.lagou.writable.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        Text rs = new Text();
        IntWritable num = new IntWritable(1);
        for (String word : words) {
            rs.set(word);
            context.write(rs, num);
        }
    }
}
```

### job-Driver

```java
package com.lagou.writable.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
         /*
        1. 获取配置文件对象，获取job对象实例
        2. 指定程序jar的本地路径
        3. 指定Mapper/Reducer类
        4. 指定Mapper输出的kv数据类型
        5. 指定最终输出的kv数据类型
        6. 指定job处理的原始数据路径
        7. 指定job输出结果路径
        8. 提交作业
         */
//        1. 获取配置文件对象，获取job对象实例
        Configuration conf = new Configuration();
        //获取Job实例
        Job job = Job.getInstance(conf, "WcCount");
        //2. 指定程序jar的本地路径-Driver
        job.setJarByClass(MyJob.class);
        //3. 指定Mapper/Reducer类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //设置使用combiner组件
        job.setCombinerClass(Combiner.class);

        //  6. 指定job读取数据路径, 指定读取数据的原始路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 7. 指定job输出结果路径, 指定结果数据输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //   8. 提交作业
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
```

### combiner

此种统计可以使用Reducer的类，可以通用有时候。

```java
package com.lagou.writable.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

// combiner组件的输入和输出与amp方法保持一致
public class Combiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 进行局部汇总
        Integer num = 0;
        for (IntWritable value : values) {
            num += value.get();
        }
        //输出结果
        IntWritable total = new IntWritable(num);
        context.write(key,total);
    }
}
```

# MapReduce分区与ReduceTask数量

分区：key的hashcode值%numReduceTasks

### 自定义分区

​	1、自定义类继承Partitioner，重写getPartition()方法

​	2、在Driver驱动中，指定使用自定义Partitioner

​	3、在Driver驱动中，要根据自定义的Partioner的逻辑设置相应的ReduceTask数量。

### Mapper

```java
package com.lagou.writable.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartionMapper extends Mapper<LongWritable, Text,Text,PartitionBean> {
    PartitionBean bean = new PartitionBean();
    Text keyt = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        String appkey = fields[2];
        bean.setIp(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppkey(fields[2]);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThridDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);
        keyt.set(appkey);
        context.write(keyt,bean);
    }
}
```

### Reducer

```java
package com.lagou.writable.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// reduce输入类型，Text,PartitionBean, 输出类型Text,PartitionBean
public class PartitionReducer extends Reducer<Text,PartitionBean,Text,PartitionBean> {
    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {
        //不需要进行聚合预算
        for (PartitionBean value : values) {
            context.write(key,value);
        }

    }
}
```

### Driver-Job

```java
package com.lagou.writable.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置文件
        Configuration conf = new Configuration();
        // 获取Job实例
        Job job = Job.getInstance(conf);
        // Driver类设置
        job.setJarByClass(PartitionJob.class);
        // 设置任务线管参数
        job.setMapperClass(PartionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        //设置输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);
        //设置自定义分区
        job.setPartitionerClass(CustomPartioner.class);
        //设置reduceTask数量和分区数量保持一致
        job.setNumReduceTasks(3);

        //输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
```

### Bean

```java
package com.lagou.writable.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionBean implements Writable {
    //定义属性
    private String id; //日志id
    private String deviceId; //设备Id
    private String appkey ;// 厂商id
    private String ip; // Ip地址
    private Long selfDuration; //自有内容播放时长
    private Long thridDuration; //第三方播放时长
    private String status; // 设备状态

    public PartitionBean() {
    }

    //序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(deviceId);
        out.writeUTF(appkey);
        out.writeUTF(ip);
        out.writeLong(selfDuration);
        out.writeLong(thridDuration);
        out.writeUTF(status);
    }

    // 反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.deviceId = in.readUTF();
        this.appkey = in.readUTF();
        this.ip = in.readUTF();
        this.selfDuration = in.readLong();
        this.thridDuration = in.readLong();
        this.status = in.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getAppkey() {
        return appkey;
    }

    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThridDuration() {
        return thridDuration;
    }

    public void setThridDuration(Long thridDuration) {
        this.thridDuration = thridDuration;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return
                "id='" + id +
                "\t deviceId='" + deviceId +
                "\t appkey='" + appkey +
                "\t ip='" + ip +
                "\t selfDuration=" + selfDuration +
                "\t thridDuration=" + thridDuration +
                "\t status='" + status;
    }
}
```

### CustomPartion

分区数量要和Recuder保持一致。若不分区，，那么所有的数据都会进入到同一个文件，而不是分门别类存放。



```java
package com.lagou.writable.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
//Partionner分区器是Map阶段的输出类型，map端的reduce聚合操作
public class CustomPartioner extends Partitioner<Text,PartitionBean> {
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int numPartitions) {
        Integer partition = 0;
        if(text.toString().equals("kar")){
            // 只要保证满足此if条件的数据可以获得同个分区编号集合
            partition = 0;
        }else if(text.toString().equals("pandora")){
            partition = 1;
        }else {
            partition = 2;
        }
        return partition;
    }
}
```

**ReduceTask数量小于分区数量**

![image-20210328184251238](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20210328184251238.png)







