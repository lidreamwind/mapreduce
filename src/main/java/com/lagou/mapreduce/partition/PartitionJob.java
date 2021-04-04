package com.lagou.mapreduce.partition;

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
