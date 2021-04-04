package com.lagou.taskOne;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaskSortMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 定义text唯一key，将所有数据聚合到一个reduce中
        Text text = new Text("1");
        // 将文本转换为Int类型，
        IntWritable intWritable = new IntWritable(Integer.parseInt(value.toString().trim()));
        context.write(text,intWritable);
    }
}
