package com.hedeoer.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    ----案例 9 ----
    一. MapReduce 中CombineTextInputFormat，
       CombineTextInputFormat :用于小文件过多的场景，它可以将多个小文件从逻辑上规划到一个切片中，
       这样，多个小文件就可以交给一个MapTask处理。 先虚拟存储， 再切片

     二. maptask 的数量
        Math.max(minSize, Math.min(maxSize, blockSize));
        可通过调整miniSize和blockSize的大小，调整切片的大小，从而影响切片的数量， --》 maptask数量

     三. 需求
        对src/main/resources/testFiles/input4下的四个1K小文件，使用CombineTextInputFormat规划切片
        再传递给maptask

        结果：
        （1）未使用CombineTextInputFormat,则MapRuduce默认使用FileInputFormat，按文件为单位，读入数据，
            针对四个1K大小的文件，将会产生4个maptask， 即有4个切片
            INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4

        （2） 使用了 CombineTextInputFormat
              若虚拟存储切片最大值设置为4M，按照内部虚拟存储再切片，则会使用1个maptask
              INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1


 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置虚拟存储切片最大值
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);//4m
        //设置使用CombineTextInputFormat(如果不设置默认是TextInputFormat)
        job.setInputFormatClass(CombineTextInputFormat.class);

        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // input目录下存在 hello.txt拷贝的其他三个副本，大小都为1K
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input4"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output9"));


        boolean b = job.waitForCompletion(true);
        System.out.println(b+"=========================");
    }
}
