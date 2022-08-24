package com.hedeoer.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**

  ----案例 11  ----
     一. MapReduce 中OutputFormat


      二. 需求
      对atguigu.log日志，进行过滤，对包含atguigu字样的记录放在atguigu.txt,
      对其他记录放在other.txt中

      输入数据格式：
         http://www.baidu.com
         http://www.google.com
         http://cn.bing.com
         http://www.atguigu.com
         http://www.atguiguhome.com
         http://www.atguigu.cn
        输出的数据格式：
 atguigu.txt中
         http://www.atguigu.com
         http://www.atguiguhome.com
         http://www.atguigu.cn

 other.txt中
         http://www.baidu.com
         http://www.google.com
         http://cn.bing.com
 */

public class AddressDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //1.创建Job实例
        Job job = Job.getInstance(conf);

        //2.给job赋值
        job.setJarByClass(AddressDriver.class);
        //设置OutputFormat类如果不设置默认使用的是TextOutputFormat
        job.setOutputFormatClass(MyOutputFormat.class);

        job.setMapperClass(AddressMapper.class);
        job.setReducerClass(AddressReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input5/log.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFilesoutput12"));

        //3.job提交
        job.waitForCompletion(true);
    }
}
