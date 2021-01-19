package com.apex_ids.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author linqz
 * @date 2017/12/29
 */
public class CustomRecordReader implements RecordReader<LongWritable, Text> {

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader lineReader;
    int maxLineLength;

    public CustomRecordReader(InputStream in, long offset, long endOffset,
                              int maxLineLength) {
        this.maxLineLength = maxLineLength;
        this.start = offset;
        this.lineReader = new LineReader(in);
        this.pos = offset;
        this.end = endOffset;
    }

    public CustomRecordReader(InputStream in, long offset, long endOffset,
                              Configuration job) throws IOException {
        this.maxLineLength = job.getInt(
                "mapred.mutilCharRecordReader.maxlength", Integer.MAX_VALUE);
        this.lineReader = new LineReader(in, job);
        this.start = offset;
        this.end = endOffset;
    }

    // 构造方法
    public CustomRecordReader(FileSplit inputSplit, Configuration job)
            throws IOException {
        maxLineLength = job.getInt("mapred.mutilCharRecordReader.maxlength",
                Integer.MAX_VALUE);
        start = inputSplit.getStart();
        end = start + inputSplit.getLength();
        final Path file = inputSplit.getPath();
        // 创建压缩器
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        // 打开文件系统
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);
        boolean skipFirstLine = false;

        if (codec != null) {
            lineReader = new LineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            lineReader = new LineReader(fileIn, job);
        }

        if (skipFirstLine) {
            start += lineReader.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    @Override
    public void close() throws IOException {
        if (lineReader != null) {
            lineReader.close();
        }
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
        while (pos < end) {
            key.set(pos);
            int newSize = lineReader.readLine(value, maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                            maxLineLength));
            // 把字符串中的">|"替换为hive的默认分割字符
            String strReplace = value.toString().replaceAll(">\\|", "\001");
            Text txtReplace = new Text();
            txtReplace.set(strReplace);
            value.set(txtReplace.getBytes(), 0, txtReplace.getLength());
            if (newSize == 0) {
                return false;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                return true;
            }
        }
        return false;
    }
}
