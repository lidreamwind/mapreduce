package com.lagou.mapreduce.wcWritable;

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
