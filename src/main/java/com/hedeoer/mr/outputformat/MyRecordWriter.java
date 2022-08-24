package com.hedeoer.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    自定义RecordWriter类
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atuigu;
    private FSDataOutputStream other;
    /*
        创建流
     */
    public MyRecordWriter(TaskAttemptContext job){
        try {
            //1.创建文件系统对象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //2.创建流
            //2.1获取输出路径
            Path outputPath = FileOutputFormat.getOutputPath(job);
            //2.2创建流的对象
            atuigu =
                    fs.create(new Path(outputPath,"atguigu.txt"));
            other =
                    fs.create(new Path(outputPath,"other.txt"));
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }

    }
    /**
     * 将数据写出去
     * @param key reducer写出的key (网址)
     * @param value reducer写出的value
     * @throws IOException
     * @throws InterruptedException
     * 注意： 该方法在被循环调用，调用一次传入一对数据
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //1.将Text转成String
        String address = key.toString() + "\n";
        //2.判断是否包含atguigu
        if (address.contains("atguigu")){
            //写到atguigu.txt
            atuigu.write(address.getBytes());//getBytes() : 将字符串转成byte[]
        }else{
            //写到other.txt
            other.write(address.getBytes());
        }
    }

    /*
        关闭资源 ：该方法在上面的write方法全部执行完后再进行调用
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (atuigu != null){
            atuigu.close();
        }
        if (other != null){
            other.close();
        }
    }
}
