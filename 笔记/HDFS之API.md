# HDFS之API

## 客户端环境准备

参考：https://blog.csdn.net/zxl646801924/article/details/83377230



相关代码参考hdfs工程。

## 基于自定义项目导入源码

在idea的terminal中执行： mvn dependency:resolve -Dclassifier=sources

然后choose sources -> 选择Hadoop的源码目录  -> download source即可。

参考：https://blog.csdn.net/weiguanpan3647/article/details/89884256



## 创建目录-权限问题

**创建目录，若目录已经存在则不予以创建。**

```java
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取hadoop集群的configuration对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs","hdfs://linux121:9000");
        // 2.根据configuration对象获取FileSystem对象
//        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
        FileSystem fs = FileSystem.get(conf);
        // 3.使用FileSystem对象创建测试目录
        fs.mkdirs(new Path("/api_test"));
        // 4. 释放FileSystem对象
        fs.close();
    }
```

![image-20210321220648441](.\图片\hsfs-权限问题.png)

如果linux用户使用hadoop命令创建一个文件，那么这个文件在HDFS中的owner就是linux用户。

解决方案

1. ​	指定用户信息获取FileSystem对象    FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
2. ​	关闭HDFS集群权限校验 		hdfs-site.xml 	dfs.permissions=true ,,分发配置并重启服务
3. ​    彻底修改HDFS的权限校验  hadoop fs -chmod -R 777 /
4. ​    生产环境可以考虑使用Kerberos和sentry框架或者 ranger来管理大数据集群安全。



## 参数的优先级 

configuration对象设置参数>resource配置文件的参数>hdfs-default.xml文件参数

## 上传下载

**若路径存在，则将数据存入目录中，目录必须存在**

**若路径不存在，则将目标目录创将为文件形式**

**参数的重载形式，表现为是否删除目录**

```java
   // 上传
	@Test
    public void copyFromLocalFileToHdfs() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取hadoop集群的configuration对象
        Configuration conf = new Configuration();
        // 2.根据configuration对象获取FileSystem对象
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
        // 3.使用FileSystem对象上传文件
        fs.copyFromLocalFile(new Path("E:\\娱乐\\李朋怀劳动合同.pdf"),new Path("/api_test"));
        // 4. 释放FileSystem对象
        fs.close();
    }
	// 下载
	@Test
    public void copyFromHdfsToLocal() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取hadoop集群的configuration对象
        Configuration conf = new Configuration();
        // 2.根据configuration对象获取FileSystem对象
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
        // 3.使用FileSystem对象下载文件
        fs.copyToLocalFile(true,new Path("/api_test/李朋怀劳动合同.pdf"),new Path("e:/"));
        // 4. 释放FileSystem对象
        fs.close();
    }
```

## 删除文件夹|文件

是否递归删除，，后边一个参数。

删除不存在的文件也不会报错。

```java
	@Test
    public void deleteFile() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取hadoop集群的configuration对象
        Configuration conf = new Configuration();
        // 2.根据configuration对象获取FileSystem对象
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
        // 3.使用FileSystem对象递归删除
        fs.delete(new Path("/api_test"),true);
        // 4. 释放FileSystem对象
        fs.close();
    }
```



## 根目录遍历

获取指定目录的权限、名称、大小。

可以选择是否递归。

listFiles可以进行递归，listStatus仅展示本级的数据信息。

```java
/**
 * 罗列集群上所有文件，权限、名称、大小、分组和所有者等等等
 */
@Test
public void listAllFiles() throws IOException {
    // 1.获取hadoop集群的configuration对象
    Configuration conf = new Configuration();
    // 2.根据configuration对象获取FileSystem对象
    FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
    // 3.使用FileSystem对象递归获取 文件路径数据
    // 不递归，默认仅显示本级信息
    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
    while (remoteIterator.hasNext()){
        LocatedFileStatus next = remoteIterator.next();
        // 文件名称
        String fileName = next.getPath().getName();
        // 文件权限
        FsPermission permission = next.getPermission();
        // 文件大小
        long blockSize = next.getBlockSize();
        // 文件所有者
        String owner = next.getOwner();
        // 文件所属组
        String group = next.getGroup();
        System.out.println("文件名称："+fileName + "  文件权限："+permission.toString() +" 文件大小："+blockSize+
                "  文件所有者："+owner+"   文件所属组："+group);
        BlockLocation[] blockLocations = next.getBlockLocations();
        for (BlockLocation blockLocation : blockLocations) {
            //还可以获得偏移量、块Id等信息
            String[] hosts = blockLocation.getHosts();
            for (String host : hosts) {
                System.out.println("所在机器："+host);
            }
        }
        System.out.println("-----------------------------");
    }
    // 4. 释放FileSystem对象
    fs.close();
}
```



## 文件夹和文件的判断

listStatus是进行状态的显示，但没有递归方法。可以通过listFiles实现递归。

```java
/**
 * 判断文件是否是文件夹等，listStatus不可递归，若要递归，，需要自己实现方法
 */
@Test
public void listFileStatusDirOrFile() throws IOException {
    // 1.获取hadoop集群的configuration对象
    Configuration conf = new Configuration();
    // 2.根据configuration对象获取FileSystem对象
    FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
    // 3.使用FileSystem对象递归获取目录下的状态信息
    	//和listFiles作对比
    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listLocatedStatus(new Path("/"));
    while (remoteIterator.hasNext()) {
        LocatedFileStatus next = remoteIterator.next();
        boolean file = next.isFile();
        if(file){
            System.out.println("这是一个文件："+next.getPath().getName());
        }
        if(next.isDirectory()){
            System.out.println("这是一个目录："+next.getPath().getName());
        }
    }
    // 4. 释放FileSystem对象
    fs.close();
}
```



##  IO流方式 上传下载

### IO流上传

IO流上传需要使用本地的输入流和HDFS的输出流。

**需要指定到集群上不存在的文件名称**

```java
/**
 * 流操作上传
 */
@Test
public void copyFromLocalToHdfsIo() throws IOException {
    // 1.获取hadoop集群的configuration对象
    Configuration conf = new Configuration();
    // 2.根据configuration对象获取FileSystem对象
    FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
    // 3.使用FileSystem对象递归获取目录下的状态信息
    
    FileInputStream fileInputStream = new FileInputStream(new File("E:\\娱乐\\唐山学院学生成绩明细[原始].xls"));
    // 需要指定到集群上不存在的文件名称
    FSDataOutputStream fsDataOutputStream = fs.create(new Path("/唐山学院.xls"));
    // 四个参数，，输入流，输出流，传输搬运的字节大小，是否关闭流
    //默认进行关闭流
    IOUtils.copyBytes(fileInputStream,fsDataOutputStream,conf);
    // 也可以进行关闭流
    IOUtils.closeStream(fsDataOutputStream);
    IOUtils.closeStream(fileInputStream);
    
    // 4. 释放FileSystem对象
    fs.close();
}
```

### IO流下载

IO流下载需要使用本地的输出流和HDFS的输入流。

```java
/**
 * 流操作读取和写入API，下载
 */
@Test
public void copyFromHdfsToLocalIO() throws IOException {
    // 1.获取hadoop集群的configuration对象
    Configuration conf = new Configuration();
    // 2.根据configuration对象获取FileSystem对象
    FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
    // 3.使用FileSystem对象递归获取目录下的状态信息
    
    //以流的方式打开集群上文件
    FSDataInputStream open = fs.open(new Path("/唐山学院.xls"));
    // 需要指定到本地不存在的文件名称
    FileOutputStream fileOutputStream = new FileOutputStream(new File("e:/hdfs.xml"));
    // 四个参数，，输入流，输出流，传输搬运的字节大小，是否关闭流
    //默认进行关闭流
    IOUtils.copyBytes(open,fileOutputStream,conf);
    
    // 4. 释放FileSystem对象
    fs.close();
}
```

### IO流Seek

```java
/**
 *  使用Seek对文件进行定位
 */
@Test
public void showContentSeek() throws IOException {
    // 1.获取hadoop集群的configuration对象
    Configuration conf = new Configuration();
    // 2.根据configuration对象获取FileSystem对象
    FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
    // 3.使用FileSystem对象递归获取目录下的状态信息
    
    
    FSDataInputStream open = fs.open(new Path("/passwd"));
    IOUtils.copyBytes(open,System.out,4096,false);
    open.seek(0);
    System.out.println("----------------------------------------");
    IOUtils.copyBytes(open,System.out,4096,true);
    
    // 4. 释放FileSystem对象
    fs.close();
}
```