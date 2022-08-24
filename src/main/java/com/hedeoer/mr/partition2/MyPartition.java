package com.hedeoer.mr.partition2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
    自定义分区类
    Partitioner<KEY, VALUE> :
        KEY : Mapper写出的key的类型
        VALUE : Mapper写出的value的类型
 */
public class MyPartition extends Partitioner<Text, FlowBean> {
    /**
     * 用来获取分区号
     * @param text map写出的key
     * @param flowBean map写出的value
     * @param numPartitions ReduceTask的数量
     * @return
     * 手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoneNumber = text.toString();//将Text转成String
        //判断手机号
        if (phoneNumber.startsWith("136")){
            return 0;
        }else if (phoneNumber.startsWith("137")){
            return 1;
        }else if (phoneNumber.startsWith("138")){
            return 2;
        }else if (phoneNumber.startsWith("139")){
            return 3;
        }else {
            return 4;
        }
    }
}
