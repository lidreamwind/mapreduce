package com.lagou.logCollect.utils;

import java.io.IOException;
import java.util.Properties;

public class PropertieyUtilsStastic {
    private static Properties props = null;

    static {
        try {
            props.load(PropertieyUtilsStastic.class.getClassLoader().getResourceAsStream("collect.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
