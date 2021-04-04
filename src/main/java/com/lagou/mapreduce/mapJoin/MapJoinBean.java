package com.lagou.mapreduce.mapJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapJoinBean implements Writable {
    //定义属性信息
    private String userId;
    private String positionId;
    private String date;
    private String positionName;
    //flag判断数据来自哪个,这个应该不是非常必要，在Reduce阶段可以通过HashMap完成
    private String flag;
    //反序列化方法需要无参构造
    public MapJoinBean() {
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.userId);
        out.writeUTF(this.positionId);
        out.writeUTF(this.date);
        out.writeUTF(positionName);
        out.writeUTF(flag);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId=in.readUTF();
        this.positionId = in.readUTF();
        this.date = in.readUTF();
        this.positionName = in.readUTF();
        this.flag = in.readUTF();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPositionId() {
        return positionId;
    }

    public void setPositionId(String positionId) {
        this.positionId = positionId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    // 格式化输出
    @Override
    public String toString() {
        return "userId=" + userId +
                "\t positionId=" + positionId +
                "\t date=" + date +
                "\t positionName=" + positionName+
                "\t flag=" + flag;
    }
}
