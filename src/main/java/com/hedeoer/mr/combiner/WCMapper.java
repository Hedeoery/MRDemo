package com.hedeoer.mr.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WCMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    private Text outKey = new Text();//创建key的对象
    //创建value对象
    private LongWritable outValue = new LongWritable();

    /**
     * 在map方法中实现需要在MapTask中实现的功能
     * 注意：该方法在被循环调用调用一次传入一行数据
     *
     * @param key 偏移量
     * @param value 一行一行的内容
     * @param context 上下文（在这用来将key,value写出去）
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);不要去调
        //1.切割数据
        //1.1将Text转成String
        String line = value.toString(); //aaa  ccc
        //1.2切割数据
        String[] words = line.split(" "); // {aaa,ccc}

        //2.封装key,value
        for (String word : words) {// {aaa,ccc}
            //2.1封装key
            //给key赋值
            outKey.set(word);
            //给value赋值
            outValue.set(1);
            //3.将数据写出去
            context.write(outKey,outValue);
        }

    }
}
