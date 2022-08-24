package com.hedeoer.mr.comparable2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
13470253144	    180	    180	    360
13509468723	    7335	110349	117684
注意：把FlowBean作为key是因为只有key可以进行排序-而我们的需求是要对总流量进行排序
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean,Text> {
    private FlowBean outKey = new FlowBean();//创建的key的对象
    private Text outValue = new Text();//创建的value的对象
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将Text转成String并切割
        String[] phoneInfo = value.toString().split("\t");//13470253144,180,180,360
        //封装key,value
        //给key赋值
        outKey.setSumFlow(Long.parseLong(phoneInfo[3]));
        outKey.setDownFlow(Long.parseLong(phoneInfo[2]));
        outKey.setUpFlow(Long.parseLong(phoneInfo[1]));
        //给value赋值
        outValue.set(phoneInfo[0]);
        //将数据写出去
        context.write(outKey,outValue);
    }
}
