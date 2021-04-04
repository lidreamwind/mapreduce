package com.lagou.logCollect.scheduler;

import com.lagou.logCollect.hdfs.CollectTask;
import com.lagou.logCollect.utils.PropertiesUtils;

import java.util.Properties;
import java.util.Timer;

public class Scheduler {
    public static void main(String[] args) {
         // 使用Timer做定时任务
        Timer timer = new Timer();
        // 延迟0没有，，  周期是一小时一次：3600*1000
        timer.schedule(new CollectTask(),0,3600*1000);
    }
}
