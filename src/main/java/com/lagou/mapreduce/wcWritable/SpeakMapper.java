package com.lagou.mapreduce.wcWritable;

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
