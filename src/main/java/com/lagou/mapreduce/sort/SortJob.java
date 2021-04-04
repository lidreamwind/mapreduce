package com.lagou.mapreduce.sort;

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
