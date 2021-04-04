package com.lagou.mapreduce.reducerJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ReduceJoin");

        job.setJarByClass(ReduceJoinJob.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReduceJoinBean.class);

        job.setOutputKeyClass(ReduceJoinBean.class);
        job.setOutputValueClass(NullWritable.class);


//        FileInputFormat.setInputPaths(job, new Path(args[0])); //指定读取数据的原始路径
        FileInputFormat.setInputPaths(job, new Path("E:\\data\\input\\MRjoin\\reduce_join\\input")); //指定读取数据的原始路径
//        7. 指定job输出结果路径
//        FileOutputFormat.setOutputPath(job, new Path(args[1])); //指定结果数据输出路径
        FileOutputFormat.setOutputPath(job, new Path("E:\\data\\output\\reduce_join\\output")); //指定结果数据输出路径
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}
