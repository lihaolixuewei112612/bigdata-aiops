package com.dtc.dingding;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.File;
import java.io.IOException;

/**
 * @Author : lihao
 * Created on : 2020-07-03
 * @Description : TODO描述类作用
 */
public class Application {
    public static void main(String[] args) throws IOException {
        File file = new File("/Users/lixuewei/workspace/DTC/workspace/test/dtc_worker/conf/mysql-bin.000002");
        EventDeserializer eventDeserializer = new EventDeserializer();
//        eventDeserializer.setChecksumType(ChecksumType.CRC32);
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        BinaryLogFileReader reader = new BinaryLogFileReader(file, eventDeserializer);
        try {
            for (Event event; (event = reader.readEvent()) != null;) {
                System.out.println(event.toString());
                EventData data = event.getData();
                if (data != null && data.getClass().isAssignableFrom(RowsQueryEventData.class)) {
                    RowsQueryEventData dmlData = (RowsQueryEventData) data;
                    System.out.println(dmlData.getQuery());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }
}

