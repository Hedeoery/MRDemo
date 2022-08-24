package com.hedeoer.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    自定义一个OutputFormat类
    1.自定义一个类继承FileOutputFormat
    2.FileOutputFormat<K, V>
        K : reducer写出的key的类型
        V : reducer写出的value的类型
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    //
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new MyRecordWriter(job);
    }
}
