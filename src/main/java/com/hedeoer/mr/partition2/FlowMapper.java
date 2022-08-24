package com.hedeoer.mr.partition2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    private  Text outKey = new Text();//封装的key
    private FlowBean outValue = new FlowBean();//封装的value

    /*
        19 	13975057813	192.168.100.16	www.baidu.com	11058	48243	200
        20 	13768778790	192.168.100.17			        120	    120     200
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.切割数据
        String[] phoneInfo = value.toString().split("\t");

        //2.封装key,value
        //给key赋值
        outKey.set(phoneInfo[1]);
        //给value赋值
        outValue.setUpFlow(Long.parseLong(phoneInfo[phoneInfo.length-3]));
        outValue.setDownFlow(Long.parseLong(phoneInfo[phoneInfo.length-2]));

        outValue.setSumFlow(outValue.getUpFlow()+outValue.getDownFlow());

        //3.写出key,value
        context.write(outKey,outValue);

    }
}
