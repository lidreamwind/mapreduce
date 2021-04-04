package com.lagou.taskOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskSortDriver {
    /**
     * 实现思路：文件较小，可以通过一个MR实现。
     *      1、MapTask实现对三个文件的聚集，将所有数据统一key进行输出，使用CombineInputFormat
     *      2、Reduce对接收所有数据，使用数组进行排序，数组下标 既是序号，数组元素既是值
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 0、判断参数个数是否正确
//        if(args.length != 2){
//            System.out.println("请确认输入数据文件路径和输出数据文件路径参数是否完整！");
//            System.exit(0);
//        }
        // 1、获取配置文件
        Configuration conf = new Configuration();
        // 2、获取Job对象
        Job job = Job.getInstance(conf);
        // 3、 配置Job的Driver类
        job.setJarByClass(TaskSortDriver.class);
        // 4、设置CombineTextInputFormat来合并小文件，生成一个Task
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        // 5、设置Mapper信息，map类，输出到 reduce的key和value类型
        job.setMapperClass(TaskSortMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 6、设置Reduce信息，reduce类、输出到文件的key和value类型
        job.setReducerClass(TaskSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 7、设置输入和输出路径参数
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileInputFormat.setInputPaths(job,new Path("E:\\data\\input\\taskOne"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\data\\output\\taskOne"));
        // 8、获取任务的执行状态
        boolean exitStatus = job.waitForCompletion(true);
        // 9、退出系统
        System.exit(exitStatus?0:1);

    }
}
