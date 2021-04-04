package com.lagou.mapreduce.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinJob {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

//        1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "MapJoinDriver");
//        2. 指定程序jar的本地路径
        job.setJarByClass(MapJoinJob.class);
//        3. 指定Mapper/Reducer类
        job.setMapperClass(MapJoinMapper.class);
//        4. 指定最终输出的kv数据类型 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//5. 指定job读取数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0])); //指定读取数据的原始路径
//        6. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //指定结果数据输出路径
        //设置加载缓存文件
        job.addCacheFile(new URI("file:///E:/data/input/MRjoin/map_join/cacheposition.txt"));
        //设置reducetask数量为0
        job.setNumReduceTasks(0);
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}
