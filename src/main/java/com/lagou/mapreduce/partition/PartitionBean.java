package com.lagou.mapreduce.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionBean implements Writable {
    //定义属性
    private String id; //日志id
    private String deviceId; //设备Id
    private String appkey ;// 厂商id
    private String ip; // Ip地址
    private Long selfDuration; //自有内容播放时长
    private Long thridDuration; //第三方播放时长
    private String status; // 设备状态

    public PartitionBean() {
    }

    //序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(deviceId);
        out.writeUTF(appkey);
        out.writeUTF(ip);
        out.writeLong(selfDuration);
        out.writeLong(thridDuration);
        out.writeUTF(status);
    }

    // 反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.deviceId = in.readUTF();
        this.appkey = in.readUTF();
        this.ip = in.readUTF();
        this.selfDuration = in.readLong();
        this.thridDuration = in.readLong();
        this.status = in.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getAppkey() {
        return appkey;
    }

    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThridDuration() {
        return thridDuration;
    }

    public void setThridDuration(Long thridDuration) {
        this.thridDuration = thridDuration;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return
                "id='" + id +
                "\t deviceId='" + deviceId +
                "\t appkey='" + appkey +
                "\t ip='" + ip +
                "\t selfDuration=" + selfDuration +
                "\t thridDuration=" + thridDuration +
                "\t status='" + status;
    }
}
