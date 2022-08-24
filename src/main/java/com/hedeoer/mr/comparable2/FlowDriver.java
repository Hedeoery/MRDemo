package com.hedeoer.mr.comparable2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *  案列8
 *  一 . 分区加自定义排序
 *  二 . 需求
 *  输入数据格式
 *  手机号，上行流量，下行流量，本次访问网站使用的流量
    13470253144	    180	    180	    360
 *  13509468723	    7335	110349	117684
 *  输出的数据格式：
 *
 *  每个手机号分区， 每个手机号文件内按照总流量降序排序
 *  手机号， 上行流量，下行流量， 当月使用的总流量
 *  13509468723	    7335	110349	117684
 *  13470253144	    180	    180	    360
 */

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建Job实例
        Job job = Job.getInstance(new Configuration());

        //2.给Job赋值
        job.setJarByClass(FlowDriver.class);

        //设置使用自定义分区类
        job.setPartitionerClass(MyPartitioner.class);
        //设置ReduceTask的数量
        job.setNumReduceTasks(5);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);//最终输出的key的类型在这是reducer输出的key
        job.setOutputValueClass(FlowBean.class);//最终输出的value的类型在这是reducer输出的value

        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input3/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output7"));

        //3.提交Job
        job.waitForCompletion(true);
    }
}
