package com.hedeoer.mr.comparable2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    将key,value进行位置的交换。因为写出去的数据是:手机号 FlowBean
 */
public class FlowReducer extends Reducer<FlowBean,Text, Text, FlowBean> {
    /*
        直接将数据写出去即可
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
