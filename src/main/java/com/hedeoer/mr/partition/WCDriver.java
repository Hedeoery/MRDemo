package com.hedeoer.mr.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*

    -----案例5------

    一. MapReduce 的默认分区和自定义分区
    二.
    (1)如果ReduceTask的数量  > getPartition的结果数，则会多产生几个空的输出文件part-r-000xx;
    (2)如果1 < ReduceTask的数量 < getPartition的结果数，则有一部分分区数据无处安放，会Exception;
    (3）如果ReduceTask的数量 = 1，则不管MapTask端输出多少个分区文件，最终结果都交给这一个ReduceTask，最终也就只会产生一个结果文件part-r-00000;
    (4）分区号必须从零开始，逐一累加。

    三. 案例
        调整job.setNumReduceTasks(1);
        对单词统计结果分区，观察最终的结果文件数量

        结果： 1. 当只设置reducetask的为2或者更多， 则将会按照默认的分区规则，即以传入maptask的key和设置的
                reducetask的数量共同决定每条数据的分区号；结果中将会出现以partitionId出现在2个或者更多的
                文件中。
               2. 设置reducetask的数量和自定义分区配合使用




 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置ReduceTask的数量
        //通过设置ReduceTask的数量那么就会影响"默认的分区"的数量
//        job.setNumReduceTasks(1);
        job.setNumReduceTasks(2);


        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input1/hello.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output4"));


        boolean b = job.waitForCompletion(true);
        System.out.println(b+"=========================");
    }
}
