package com.hedeoer.mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 ----案例 10  ----
    一. MapReduce 中Combiner，
       Combiner的意义就是进行对来自环形缓冲区的数据局部汇总，用来减少网络传输量，并减轻
       ReduceTask的压力

     二. 需求
        对src/main/resources/testFiles/input1/hello.txt单词统计，对比使用Combiner前后的情况

        （1）未使用前
        Map-Reduce Framework
		Combine input records=0
		Combine output records=0
		Reduce input groups=7
		Reduce shuffle bytes=161
		Reduce input records=10
		Reduce output records=7

        （2）使用后

        Combine input records=10
		Combine output records=7
		Reduce input groups=7
		Reduce shuffle bytes=116
		Reduce input records=7
		Reduce output records=7

 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置使用Combiner类
//        job.setCombinerClass(WCCombiner.class);
        // 此处可以使用WCReducer类的逻辑处理Combiner的逻辑
        job.setCombinerClass(WCReducer.class);

        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input1/hello.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\ouput12"));


        boolean b = job.waitForCompletion(true);
        System.out.println(b+"=========================");
    }
}
