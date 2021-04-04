package com.lagou.mapreduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartionMapper extends Mapper<LongWritable, Text,Text,PartitionBean> {
    PartitionBean bean = new PartitionBean();
    Text keyt = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        String appkey = fields[2];
        bean.setIp(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppkey(fields[2]);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThridDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);
        keyt.set(appkey);
        context.write(keyt,bean);
    }
}
