package com.lagou.logCollect.utils;

import java.io.*;
import java.util.Properties;

public class PropertiesUtils {
    private Properties props = null;

    public Properties loadProp(){
        try {
            if(props==null) {
                synchronized ("lock") {
                    if (props == null) {
                        props = new Properties();
                        // 方式一，在conf配置文件中进行配置，打包的时候，，会有更好的友好度
                        String path = new File(".").getAbsolutePath().replace(".","conf")+File.separator+"collect.properties";
                        InputStream in = new BufferedInputStream(new FileInputStream(path));
                        // 方拾二，使用classLoader进行加载
                        InputStream inputStream = PropertiesUtils.class.getClassLoader().getResourceAsStream("collect.properties");
                        props.load(in);
                    }
                }
            }
            return props;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        Properties properties = new PropertiesUtils().loadProp();
        System.out.println(properties.getProperty("LOCAL_LOG_DIR"));
    }
}
