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

    -----案例3------

    一，在本地向集群提交Job (了解即可)

    二. 需求
    对/input1/hello.txt的hello.txt进行单词频次统计，每个单词以空格分割
    并将统计结果输出到HDFS指定的目录，比如/output2，但目录必须不存在

    三.
      1.设置参数
            设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
            conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
            指定MR运行在Yarn上
            conf.set("mapreduce.framework.name","yarn");
            指定MR可以在远程集群运行
            conf.set("mapreduce.app-submission.cross-platform", "true");
            指定yarn resourcemanager的位置
            conf.set("yarn.resourcemanager.hostname", "hadoop103");
       2.打包 （输入路径和输路径是从main方法读取的）
       3. 将job.setJarByClass(WCDriver3.class)注释掉
          添加job.setJar("jar包路径");
       4.
            VM Options : -DHADOOP_USER_NAME=atguigu
            Program Arguments : hdfs://hadoop102:8020/input hdfs://hadoop102:8020/output2

 */
public class WCDriver3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //指定MR运行在Yarn上
        conf.set("mapreduce.framework.name","yarn");
        //指定MR可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname", "hadoop103");


        Job job = Job.getInstance(conf);

        //2.给Job赋值
        //2.1关联本程序中的jar---如果是本地运行不需要该设置
       // job.setJarByClass(WCDriver3.class);
        job.setJar("D:\\Course\\courseToMapReduce\\MRDemo\\target\\MRDemo-1.0-SNAPSHOT.jar");

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.out.println(b+"=========================");
    }
}
