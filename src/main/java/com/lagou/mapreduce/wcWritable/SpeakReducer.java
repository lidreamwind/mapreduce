package com.lagou.mapreduce.wcWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakReducer extends Reducer<Text,SpeakBean,Text,SpeakBean> {
    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        //定义时长累加的初始值
        Long selfTimeTotal = 0L;
        Long thirdTimeTotal = 0L;
        //reduce方法的key:map输出的某一个key
        //reduce方法的value:map输出的kv对中相同key的value组成的一个集合
        //reduce 逻辑：遍历迭代器累加时长即可
        for (SpeakBean value : values) {
            selfTimeTotal += value.getSelfTime();
            thirdTimeTotal += value.getThirdTime();
        }
        //输出，封装成一个bean对象输出
        SpeakBean speakBean = new SpeakBean(key.toString(), selfTimeTotal, thirdTimeTotal);
        context.write(key,speakBean);
    }
}
