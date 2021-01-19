package com.apex_ids.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @author linqz
 * @date 2017/12/29
 */
public class CustomHiveInputFormat extends TextInputFormat implements JobConfigurable {


    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new CustomRecordReader((FileSplit) genericSplit, job);
    }
}
