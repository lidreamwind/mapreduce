package com.lagou.mapreduce.wcWritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//这个类型是map输出kv中value的类型，需要实现writable序列化接口
//若是Mapper中的key带有bean对象，那么也需要实现Comparable接口
public class SpeakBean implements Writable {

    //定义属性
    private String serviceId;
    private Long selfTime;
    private Long thirdTime;
    private Long totalTime;

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
}
