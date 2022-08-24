package com.hedeoer.mr.partition2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
        -----案例6------

        一. MapReduce 的默认分区和自定义分区

        二.
        (1)如果ReduceTask的数量  > getPartition的结果数，则会多产生几个空的输出文件part-r-000xx;
        (2)如果1 < ReduceTask的数量 < getPartition的结果数，则有一部分分区数据无处安放，会Exception;
        (3）如果ReduceTask的数量 = 1，则不管MapTask端输出多少个分区文件，最终结果都交给这一个ReduceTask，最终也就只会产生一个结果文件part-r-00000;
        (4）分区号必须从零开始，逐一累加。

        三. 需求对 phone_date.txt ，统计每个手机号的流量使用情况，写出结果为每个手机号一个文件

 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        /*
            1.如果是默认分区有几个ReduceTask就有几个分区

            2.自定义分区的情况下：分区的数量和ReduceTask数量的关系？
             默认的 ： 分区的数量 = ReduceTask的数量
                      分区的数量 > ReduceTask的数量 ： 抛异常
                      分区的数量 < ReduceTask的数量 ： 可以但是浪费资源
         */
        //设置ReduceTask的数量 ---- 会影响默认分区的数量 : key.hashCode() % ReduceTask的数量
        job.setNumReduceTasks(5);
        // 异常
//        job.setNumReduceTasks(4);
        // 浪费资源
//        job.setNumReduceTasks(6);


        //设置使用自定义分区类
        job.setPartitionerClass(MyPartition.class);

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input2/phone_data .txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output5"));

        job.waitForCompletion(true);
    }
}
