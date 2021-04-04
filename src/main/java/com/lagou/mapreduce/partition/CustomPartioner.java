package com.lagou.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
//Partionner分区器是Map阶段的输出类型，map端的reduce聚合操作
public class CustomPartioner extends Partitioner<Text,PartitionBean> {
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int numPartitions) {
        Integer partition = 0;
        if(text.toString().equals("kar")){
            // 只要保证满足此if条件的数据可以获得同个分区编号集合
            partition = 0;
        }else if(text.toString().equals("pandora")){
            partition = 1;
        }else {
            partition = 2;
        }
        return partition;
    }
}
