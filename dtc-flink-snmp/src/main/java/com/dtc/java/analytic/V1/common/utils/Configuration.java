package com.dtc.java.analytic.V1.common.utils;

/**
 * @Author : lihao
 * Created on : 2020-05-08
 * @Description : TODO描述类作用
 */

import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

public class Configuration {
    public static Properties getConf(String path) {
        Properties prop = new Properties();
        try {
            InputStream in = Configuration.class.getClassLoader().getResourceAsStream(path);
            prop.load(in);
            Iterator<String> it = prop.stringPropertyNames().iterator();
            String str=null;
            while (it.hasNext()) {
                str = it.next();
            }
            in.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        return prop;
    }
}
