package com.lagou.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//这个类型是map输出kv中key的类型，需要实现writable序列化接口
//若是Mapper中的key带有bean对象，那么也需要实现Comparable接口
public class SpeakBean implements WritableComparable<SpeakBean> {

    //定义属性
    private String serviceId; // 设备id
    private Long selfTime; //自有内容播放时长
    private Long thirdTime; // 第三方播放内容时长
    private Long totalTime;  //总时长

    //准备一个空参构造，反序列化时需要使用
    public SpeakBean() {
    }
    //有参构造， 用于创建对象
    public SpeakBean(String serviceId, Long selfTime, Long thirdTime) {
        this.serviceId = serviceId;
        this.selfTime = selfTime;
        this.thirdTime = thirdTime;
        this.totalTime = this.selfTime + this.thirdTime;
    }

    //序列化方法:就是把内容输出到网络或者文本中
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(serviceId);
        dataOutput.writeLong(selfTime);
        dataOutput.writeLong(thirdTime);
        dataOutput.writeLong(totalTime);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.serviceId = dataInput.readUTF();
        this.selfTime = dataInput.readLong();
        this.thirdTime = dataInput.readLong();
        this.totalTime = dataInput.readLong();
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public Long getSelfTime() {
        return selfTime;
    }

    public void setSelfTime(Long selfTime) {
        this.selfTime = selfTime;
    }

    public Long getThirdTime() {
        return thirdTime;
    }

    public void setThirdTime(Long thirdTime) {
        this.thirdTime = thirdTime;
    }

    public Long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(Long totalTime) {
        this.totalTime = totalTime;
    }
    //为了方便观察数据，重写toString()方法

    @Override
    public String toString() {
        return  "serviceId='" + serviceId +
                "\t selfTime=" + selfTime +
                "\t thirdTime=" + thirdTime +
                "\t totalTime=" + totalTime;
    }

    @Override
    public int compareTo(SpeakBean o) { // 返回值 ：0-相等 1-小于 -1：大于
        // 指定按照bean对象的总时长字段进行比较
        if(this.totalTime>o.totalTime){
            return -1;
        }else if(this.totalTime<this.totalTime){
            return 1;
        }else {
            return 0;
        }
    }
}
