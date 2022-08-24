package com.hedeoer.mr.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WCReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    private LongWritable outValue = new LongWritable();//封账的value


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;//累加value
        //1.遍历所有的value
        for (LongWritable value : values) {
            //2.将value进行累加
            //2.1将LongWritable转换成long
            sum += value.get();
        }
        //3.封装key,value
        outValue.set(sum);

        //4.将key,value写出去
        context.write(key,outValue);
    }
}
