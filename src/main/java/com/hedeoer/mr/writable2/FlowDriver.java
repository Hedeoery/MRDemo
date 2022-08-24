package com.hedeoer.mr.writable2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 *  -----案例4------
 *
 * 一 . Hadoop 的序列化机制
 * Java类型	 Hadoop Writable类型
 * Boolean	 BooleanWritable
 * Byte	     ByteWritable
 * Integer	 IntWritable
 * Float	 FloatWritable
 * Long	     LongWritable
 * Double	 DoubleWritable
 * String	 Text（特殊）
 * Map	     MapWritable
 * Array	 ArrayWritable
 * Null	     NullWritable（特殊）
 *
 * 二 . 需求
 *      对FlowBean类实现Hadoop的序列化， 继承Writable接口，重写readFields（）和write() 方法
 *      对src/main/resources/testFiles/input2/phone_data .txt中的手机流量统计，以一下格式输出
 *      到E:\Z_temp\TestFiles\output2.
 *      手机号  使用的上行流量 使用的下行流量  总流量
 *      13470253144 180	180	360
 *      13509468723	7335    110349	117684
 *      13560439638	918	4938	5856
 *      13568436656	3597	25635	29232
 *
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建Job实例
        Job job = Job.getInstance(new Configuration());

        //2.给Job赋值
        //2.1关联本程序中的jar---如果是本地运行不需要该设置
        job.setJarByClass(FlowDriver.class);
        //2.2关联Mapper和Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //2.3设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //2.4设置最终输出的key,value的类型--在这是Reducer输出的key,value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //2.5设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input2/phone_data .txt"));
        //注意：输出路径一定不能存在否则报错
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output2"));

        //3.执行Job
        job.waitForCompletion(true);
    }
}
