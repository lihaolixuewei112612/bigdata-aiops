package com.dtc.java.analytic.V1.configuration;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

public class Configuration {
    public static Properties getConf(String path) {
        Properties prop = new Properties();
        try {
            //读取属性文件a.properties
            InputStream in = Configuration.class.getClassLoader().getResourceAsStream(path);
            prop.load(in);     ///加载属性列表
            Iterator<String> it = prop.stringPropertyNames().iterator();
            while (it.hasNext()) {
                String key = it.next();
//                System.out.println(key + ":" + prop.getProperty(key));
            }
            in.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        return prop;
    }


}
