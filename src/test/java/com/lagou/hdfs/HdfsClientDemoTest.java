package com.lagou.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

class HdfsClientDemoTest {
    // 创建目录，，有重载方法是否删除目录
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取hadoop集群的configuration对象
        Configuration conf = new Configuration();
        // 2.根据configuration对象获取FileSystem对象
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
        // 3.使用FileSystem对象创建测试目录
        fs.mkdirs(new Path("/api_test"));
        // 4. 释放FileSystem对象
        fs.close();
    }

    @Test
    // 参考 MarkDown文档《HDFS之API》
    public void testMkdirs1() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取hadoop集群的configuration对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs","hdfs://linux121:9000");
        // 2.根据configuration对象获取FileSystem对象
        FileSystem fs = FileSystem.get(conf);
        // 3.使用FileSystem对象创建测试目录
        fs.mkdirs(new Path("/api_test1"));
        // 4. 释放FileSystem对象
        fs.close();
    }
}