package com.hedeoer.mr.partition2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    private FlowBean outValue = new FlowBean();//封装的value
    /*
            key                 value
        13568436656         100     200     300     ====>   13568436656 300 500 800
        13568436656         200     300     500
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;//总上行流量
        long sumDownFlow = 0;//总下行流量
        //1.遍历value - 求出总上行和总下行流量
        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        //2.封装key,value
        //给value赋值
        outValue.setUpFlow(sumUpFlow);
        outValue.setDownFlow(sumDownFlow);
        outValue.setSumFlow(outValue.getUpFlow() + outValue.getDownFlow());

        //3.将key,value写出去
        context.write(key,outValue);
    }
}
