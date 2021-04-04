package com.lagou.mapreduce.reducerJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//通过positionId将两张表的数据聚集在一起，Text就是positionId， ReduceJoinBean是相关数据
public class ReduceJoinMapper extends Mapper<LongWritable, Text,Text, ReduceJoinBean> {
    String textName = "";
    /**
     * Called once at the beginning of the task.
     * 通过setup获取文件的名字
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit)inputSplit;
        textName = fileSplit.getPath().getName();
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fiedls = value.toString().split("\t");
        ReduceJoinBean bean = new ReduceJoinBean();
        if(textName.startsWith("deliver_info")){
            bean.setDate(fiedls[2]);
            bean.setFlag("deliver");
            bean.setPositionId(fiedls[1]);
            bean.setUserId(fiedls[0]);
            bean.setPositionName("");
        }else{
            bean.setDate("");
            bean.setFlag("position");
            bean.setPositionName(fiedls[1]);
            bean.setPositionId(fiedls[0]);
            bean.setUserId("");
        }
        Text text = new Text();
        text.set(bean.getPositionId());
        context.write(text,bean);
    }
}
