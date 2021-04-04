package com.lagou.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// reduce输入类型，Text,PartitionBean, 输出类型Text,PartitionBean
public class PartitionReducer extends Reducer<Text,PartitionBean,Text,PartitionBean> {
    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {
        //不需要进行聚合预算
        for (PartitionBean value : values) {
            context.write(key,value);
        }

    }
}
