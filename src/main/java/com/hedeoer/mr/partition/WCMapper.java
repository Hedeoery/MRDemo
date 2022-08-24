package com.hedeoer.mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WCMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    private Text outKey = new Text();//创建key的对象
    //创建value对象
    private LongWritable outValue = new LongWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        for (String word : words) {
            outKey.set(word);
            outValue.set(1);
            context.write(outKey,outValue);
        }

    }
}
