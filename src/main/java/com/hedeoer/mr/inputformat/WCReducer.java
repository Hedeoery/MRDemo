package com.hedeoer.mr.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    1.该类用来实现ReduceTask中南非要实现的功能
    2. Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
            两组：
                第一组 ：KEYIN ： 读取的key的类型（mapper写出的key的类型）
                        VALUEIN ：读取的value的类型（mapper写出的value的类型）
                第二组：
                    KEYOUT ：写出的key的类型（在这是单词的类型）
                    VALUEOUT ：写出的value的类型（在这是单词数量的类型）
 */
public class WCReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    private LongWritable outValue = new LongWritable();//封账的value

    /**
     * 在该方法中用来实现需要在ReduceTask中实现的功能
     * 注意：该方法在被循环调用，每调用一次传入一组数据（key值相同为一组）
     * @param key 读进来的key
     * @param values 读进来的所有的value
     * @param context 上下文-在这用来将数据写出去
     * @throws IOException
     * @throws InterruptedException
     *
     * Text     LongWritable
     * aaa      1            ===>     aaa 2
     * aaa      1
     */
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
