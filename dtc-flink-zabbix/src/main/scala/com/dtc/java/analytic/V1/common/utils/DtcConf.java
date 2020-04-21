package com.dtc.java.analytic.V1.common.utils;

import com.dtc.java.analytic.V1.dtc.execption.DtcException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created on 2019-08-16
 *
 * @author :hao.li
 */
public class DtcConf {
    private static Logger logger = LoggerFactory.getLogger(DtcConf.class);
    private static Configuration conf = new Configuration();

    /**
     * Initial or refresh configuration & constants. Before you starting
     * applications on Hadoop/HBase, you should call this method at first. If you
     * are testing units, calling this method might not be necessary.
     */
    public static void setup() {
        InputStream inputStream = null;
        try {
            inputStream = DtcConf.class.getClassLoader().getResourceAsStream("dtc-flink.xml");
//            inputStream = ClassLoader.getSystemResourceAsStream("dtc-flink.xml");
            //local test
//            inputStream = new FileInputStream(new File("conf/dtc-flink.xml"));
            if (null != inputStream) {
//        conf.set("hadoop.tmp.dir", "/tmp");
//        conf.set("fs.defaultFS", "hdfs://10.3.6.7:9000");
                conf.addResource(inputStream);
                String kafka=conf.get("dtc.bootstrap.servers");
            } else {
                throw new IOException("Configure file not found: dtc-flink.xml");
            }
        } catch (FileNotFoundException e) {
            logger.error("An error was caught", e);
        } catch (IOException e) {
            throw new DtcException(e.getMessage());
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (Exception e) {
                logger.error("An error was caught", e);
            }
        }
    }


    public static Configuration getConf() {
        return conf;
    }
}
