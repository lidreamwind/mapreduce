# Map Reduce编程框架

分而治之思想。

Map Reduce任务过程分为两个处理阶段。

- ​	Map阶段：Map阶段的主要作用是“分”，即把复杂的任务分解为若干个“简单的任务”来并行处理。Map阶段的这些任务可以并行计算，彼此间 没有依赖关系。
- ​	Reduce阶段：Reduce阶段的主要作用是“合”，即对Map阶段的结果进行全局汇总。

![image-20210325194941399](.\图片\MapReduce-思想图.png)

## Word Count案例源码解析

​		Mapper类继承了org.apache.hadoop.mapreduce.Mapper类重写了其中的map方法，Reducer类继承了org.apache.hadoop.mapreduce.Reducer类重写了其中的reduce方法。

​		重写Map方法作用：map方法其中的逻辑是用户希望mr程序map阶段需要做的逻辑处理。

​		重写的Reduce方法作用：reduce方法其中的逻辑是用户希望mr程序reduce阶段如何处理的逻辑；

**map中的Mapper<Object, Text, Text, IntWritable> 前两个是Map的输入类型，后两个是Map的输出类型。**

**reduce中的Reducer<Text, IntWritable, Text, IntWritable> 前两个输入类型是map阶段的输出类型**

![image-20210325201615932](.\图片\MapReduce-WordCount源码分析.png)

​			Driver 

![image-20210325202407234](.\图片\MapReduce-workcount-job源码.png)

## Hadoop序列号

​		序列化主要是通过网络传输数据或者把对象持久化到文件，需要把对象序列化程二进制的结构。

​		序列化：数据更紧凑，充分利用网络带宽资源；序列化和反序列化开销尽量低一些。



# Map Reduce编程规范

### Mapper类

- ​	用户自定义一个Mapper类继承Hadoop的Mapper类
- ​	Mapper输入数据是Key-value形式(类型可以自定义)
- ​	Map阶段的业务逻辑定义在map()方法中
- ​	Mapper的输出数据是key-value形式(类型可以自定义)

**注意：map()方法是对输入的一个key-value调用一次**

### Reducer类

- ​	用户自定义Reducer类要继承Hadoop的Reducer类
- ​	Reducer的输入数据类型对应Mapper的输出数据类型（key-value）
- ​	Reducer的业务逻辑都写在reduce方法中

* **Reduce()方法是对相同Key的一组key-value键值对调用执行一次**



### Driver阶段

​		创建提交yarn集群运行的job对象，其中封装了Map Reduce程序运行所需要的的相关参数输入数据路径，输出数据路径等，也相当于是一个Yarn集群的客户端，主要作用就是提交Map Reduce程序运行。

## Word Count实现

### Mapper实现

```java
package com.lagou.mr;

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



### Reducer实现

![image-20210325213024512](.\图片\MapReduce-自定义Reducer.png)

```java
package com.lagou.mr;

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

​	

### Driver

![image-20210325213855932](.\图片\MapReduce-自定义Driver.png)

```java
package com.lagou.mr;

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



### Pom配置

```java
<build>
    <plugins>
        <!-- 跳过测试代码-->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <configuration>
                <skip>true</skip>
            </configuration>
        </plugin>
        <!-- 使用maven-compiler-plugin插件可以指定项目源码的jdk版本，
            编译后的jdk版本，以及编码。-->
        <plugin>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>3.6.0</version>
            <configuration>
                <source>1.8</source>
                <target>1.8</target>
            </configuration>
        </plugin>
        <!--     支持自定义的打包结构，也可以定制依赖项等。
        我们日常使用的以maven-assembly-plugin为最多，
        因为大数据项目中往往有很多shell脚本、SQL脚本、.properties及.xml配置项等，
        采用assembly插件可以让输出的结构清晰而标准化。 -->
        <plugin>
            <artifactId>maven-assembly-plugin </artifactId>
            <configuration>
                <descriptorRefs>
                    <descriptorRef>jar-with-dependencies</descriptorRef>
                </descriptorRefs>
                <!-- 指定主类 -->
                <archive>
                    <manifest>
                        <mainClass>com.lagou.mr.MyJob</mainClass>
                    </manifest>
                </archive>

            </configuration>
            <executions>
                <execution>
                    <id>make-assembly</id>
                    <phase>package</phase>
                    <goals>
                        <goal>single</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

### Yarn集群提交

hadoop jar hdfs-1.0-SNAPSHOT.jar com.lagou.mr.MyJob /input /output



# Writable序列化接口

​		基本序列化类型往往不能满足所有需求，比如Hadoop框架内部传递一个自定义的bean对象，那么该对象就要实现Writable序列化接口。

## 实现Writable序列化步骤如下

1. ​		实现Writable接口

2. ​		反序列化时，需要反射调用空参构造函数，所以必须有空参构造	

   ```java
   //空参构造函数
   public CustomBean(){
   	super();
   }
   ```

3. ​		重写序列化方法

   ```java
   @Override
   public void write(DataOutput out) throws IOException{
       ...
   }
   ```

4. ​		重写反序列化方法

   ```java
   @Override
   public void readFields(DataInput in) throws IOException{
   	...
   }
   ```

5. **反序列化的字段顺序和序列化的字段顺序必须完全一致**

6. **方便展示结果数据，需要重写bean的toString()方法，可以自定义分隔符**

7. 如果自定义Bean对象需要放在Mapper输出KV中的K，则该对象还需要实现Comparable接口，因为MapReduce框中的Shuffle过程要求对key必须能排序。

   ```java
   @Override
   public int compareTo(CustomBean o){
   	// 自定义排序规则
   	return this.num>o.getNum()>-1:1;
   }
   ```

   

## Writable接口案例

### 需求

​		统计每台智能音箱设备内容播放时长。

​		原始日志		

```text
001			001577c3		kar_890809			120.196.100.99			1116			954						200
日志id		设备id			appkey(合作硬件厂商)		网络ip			自有内容时长(秒)	第三方内容时长(秒)		  网络状态码
```

输出结果

```text
0001577c3			11160			9540			20700
设备id			自有内容时长(秒)	第三方内容时长(秒)   总时长
```

整体思路：

​	**Map阶段**：

​			1.读取一行文本数据，按照制表符 切分数据

​			2.抽取出自由内容时长，第三方内容时长设备id

   3. 输出 key-->设备id  value：封装一个bean对象，bean对象携带 自由时长，第三方内容时长，设备id

   4. 自定义bean对象作为value输出，需要实现Writable序列化接口

      **Reduce阶段**：

      ​		1.在reduce方法中直接遍历迭代器，，然后累加时长然后输出即可。

### 编写MapReduce程序

#### 创建SpeakBean对象

**需要实现Writable接口**

```java
package com.lagou.writable.wcWritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//这个类型是map输出kv中value的类型，需要实现writable序列化接口
//若是Mapper中的key带有bean对象，那么也需要实现Comparable接口
public class SpeakBean implements Writable {

    //定义属性
    private String serviceId;
    private Long selfTime;
    private Long thirdTime;
    private Long totalTime;

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
}
```

#### 创建Mapper

```java
package com.lagou.writable.wcWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeakMapper extends Mapper<LongWritable, Text,Text,SpeakBean> {
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
        //  直接输出：k-->设备id,value-->speakbean
        this.text.set(serviceId);
        context.write(text,speakBean);
    }
}
```

#### 创建Reducer

```java
package com.lagou.writable.wcWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakReducer extends Reducer<Text,SpeakBean,Text,SpeakBean> {
    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        //定义时长累加的初始值
        Long selfTimeTotal = 0L;
        Long thirdTimeTotal = 0L;
        //reduce方法的key:map输出的某一个key
        //reduce方法的value:map输出的kv对中相同key的value组成的一个集合
        //reduce 逻辑：遍历迭代器累加时长即可
        for (SpeakBean value : values) {
            selfTimeTotal += value.getSelfTime();
            thirdTimeTotal += value.getThirdTime();
        }
        //输出，封装成一个bean对象输出
        SpeakBean speakBean = new SpeakBean(key.toString(), selfTimeTotal, thirdTimeTotal);
        context.write(key,speakBean);
    }
}
```

#### 创建Driver-Job

```java
package com.lagou.writable.wcWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SpeakJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"speakDriverTask");
        //设置jar包本地路径,设置Driver的类
        job.setJarByClass(SpeakJob.class);
        // 设置Mapper信息
        job.setMapperClass(SpeakMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);
        // 设置Reducer信息
        job.setReducerClass(SpeakReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);
        // 设置输出和输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 启动任务
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
```