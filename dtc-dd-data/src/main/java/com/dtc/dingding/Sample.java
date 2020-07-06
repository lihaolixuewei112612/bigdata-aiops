package com.dtc.dingding;

/**
 * @Author : lihao
 * Created on : 2020-07-03
 * @Description : TODO描述类作用
 */
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.File;
import java.io.IOException;
class Sample {
    public static void main(String[] args) throws IOException {
        String filePath="/Users/lixuewei/workspace/DTC/workspace/test/dtc_worker/conf/mysql-bin.000002";
        File binlogFile = new File(filePath);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setChecksumType(ChecksumType.CRC32);
        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer);
        try {
            for (Event event; (event = reader.readEvent()) != null; ) {
                System.out.println(event.toString());
            }
        } finally {
            reader.close();
        }
    }
}
