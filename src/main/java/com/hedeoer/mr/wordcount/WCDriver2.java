package com.hedeoer.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*

    -----案例2------

    一，Hadoop的集群运行

    二. 需求
    对/input1/hello.txt的hello.txt进行单词频次统计，每个单词以空格分割
    并将统计结果输出到HDFS指定的目录，比如/output1，但目录必须不存在

    在集群上运行WordCount
    1.将输入和输出路径修改成从main方法中读取
    2.打jar包
    3.运行命令
        hadoop jar jar包名字 jar包中的需要运行的main方法类名 输入路径 输出路径
        hadoop jar MRDemo-1.0-SNAPSHOT.jar com.hedeoer.mr.wordcount.WCDriver2 /input1 /output1

 */
public class WCDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //1.创建Job实例
        Job job = Job.getInstance(conf);

        //2.给Job赋值
        //2.1关联本程序中的jar---如果是本地运行不需要该设置
        job.setJarByClass(WCDriver2.class);
        //2.2关联Mapper和Reducer
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        //2.3设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //2.4设置最终输出的key,value的类型--在这是Reducer输出的key,value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //2.5设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //注意：输出路径一定不能存在否则报错
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //3.执行Job
        /*
            boolean waitForCompletion(boolean verbose)
            verbose : 是否打印执行的进度
            返回值 ：如果执行成功返回true
         */
        boolean b = job.waitForCompletion(true);
        System.out.println(b+"=========================");
    }
}
