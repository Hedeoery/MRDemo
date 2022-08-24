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

    -----案例1------

    一，Hadoop的本地运行

    二. 需求
    对src/main/resources/testFiles/input1/hello.txt的hello.txt进行单词频次统计，每个单词以空格分割
    并将统计结果输出到指定的目录，比如E:\Z_temp\TestFiles\output，但目录必须不存在

    三. 常犯的错误：

        1.导包错误：ClassCastException (比如import javax.xml.soap.Text;)
        2.调用父类中的方法 （super.map(key, value, context)）
        3.全部给key或者value赋值
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputKeyClass(LongWritable.class);
        4.输出的目录中没有内容或者文件（一定是报错了，具体情况看错误信息）
        5.如果报NativeIOException
               5.1先排除是否是代码错误（可以使用龙哥的代码测试）
               5.2确定不是代码问题--使用NativeIO这个类
               5.3更换hadoop环境。如果3.0更换成3.1
               5.4查看是否windows用户名是中文或者有空格（重新创建新的用户-改名无效）
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //1.创建Job实例
        Job job = Job.getInstance(conf);

        //2.给Job赋值
        //2.1关联本程序中的jar---如果是本地运行不需要该设置
        job.setJarByClass(WCDriver.class);

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
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input1/hello.txt"));
        //注意：输出路径一定不能存在否则报错
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output"));

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
