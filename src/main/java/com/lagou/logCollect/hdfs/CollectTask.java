package com.lagou.logCollect.hdfs;

import com.lagou.logCollect.common.ConsistantVar;
import com.lagou.logCollect.utils.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

public class CollectTask extends TimerTask {
    @Override
    public void run() {
        //下面是采集的流程
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String today = simpleDateFormat.format(new Date());
        //获取文件属性
        Properties props = new PropertiesUtils().loadProp();
        //1、扫描指定目录，找到待上传的文件
        File file = new File(props.getProperty(ConsistantVar.LOCAL_LOG_DIR));
            // 查找要上传的文件，是否过滤器形式
        File[] logFiles = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.getName().startsWith(props.getProperty(ConsistantVar.LOG_PREFIX))) {
                    return true;
                }
                return false;
            }
        });
        //2、把待上传文件移动到临时目录
            // 判断临时目录是否存在
        File localDirTemp = new File(props.getProperty(ConsistantVar.LOG_TEMP_DIR) );
        if(!localDirTemp.exists()){
            localDirTemp.mkdirs();
        }
        for (File logFile : logFiles) {
            logFile.renameTo(new File(localDirTemp.getAbsolutePath()+File.separator+logFile.getName()));
        }
        // 判断备份目录是否存在
        File localDirBak = new File(props.getProperty(ConsistantVar.LOCAL_LOG_DIR_BAK)+File.separator+today );
        if(!localDirBak.exists()){
            localDirBak.mkdirs();
        }
        //3、将临时目录中的数据，上传到指定目录
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://linux121:9000");
        FileSystem fs = null;
        Path hdfsPath = new Path(props.getProperty(ConsistantVar.HDFS_LOG_DIR)+"/"+today);
        try {
            fs = FileSystem.get(new URI("hdfs://linux121:9000"), conf, "root");
            // 创建目录
            if(!fs.exists(hdfsPath)){
                fs.mkdirs(hdfsPath);
            }
            File[] files = localDirTemp.listFiles();
            for (File file1 : files) {
                //4、上传的文件备份到指定目录
                fs.copyFromLocalFile(new Path(file1.getPath()),new Path(hdfsPath.toString()+"/"+file1.getName()));
                // 文件转移到备份目录
                file1.renameTo(new File(localDirBak.getPath()+File.separator+file1.getName()));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
