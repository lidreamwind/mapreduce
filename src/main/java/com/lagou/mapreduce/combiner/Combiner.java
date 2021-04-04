package com.lagou.mapreduce.combiner;

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
