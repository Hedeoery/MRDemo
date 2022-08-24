package com.hedeoer.mr.comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 案例7
 一. MapReduce 自定义排序

 二. 对自定义类 FlowBean实现可排序，即FlowBean implements WritableComparable<FlowBean>
        -- public interface WritableComparable<T> extends Writable, Comparable<T>
        此时，FlowBean即可序列化，又可实例排序

 三.  对 案例4 按照总流量倒序排序
         @Override
         public int compareTo(FlowBean o) {
         //按照总流量进行排序
         return -Long.compare(this.sumFlow,o.sumFlow);
         }
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建Job实例
        Job job = Job.getInstance(new Configuration());

        //2.给Job赋值
        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);//最终输出的key的类型在这是reducer输出的key
        job.setOutputValueClass(FlowBean.class);//最终输出的value的类型在这是reducer输出的value

        // 案例4 的结果文件
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input3/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output6"));

        //3.提交Job
        job.waitForCompletion(true);
    }
}
