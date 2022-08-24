package com.hedeoer.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
----案例 12  ----
     一. MapReduce 中MapJoin
         Map Join适用于一张表十分小、一张表很大的场景，在Map端缓存多张表，
         提前处理业务逻辑，这样增加Map端业务，
         减少Reduce端数据的压力，尽可能的减少数据倾斜。

     二. 使用步骤
        （1）job.setNumReduceTasks(0);
            设置RuduceTask的数量为0，则map后的所有流程终止，以map的处理结果输出
        （2）job.addCacheFile()
            添加需要缓存的文件，文件按不宜过大
        （3）重写Mapper类中setUp和map，cleanUp方法
            setUp（）方法中将小表db.txt的数据加载到缓存中
            map对order.txt 的每条数据封装数据

      三. 需求
        对order.txt和db.txt进行Join

        输入：
            order.txt: id	pid	amount
                        1001	01	1
                        1002	02	2
                        1003	03	3
                        1004	01	4
                        1005	02	5
                        1006	03	6

            db.txt: pid	pname
                    01	小米
                    02	华为
                    03	格力

        输出：
                    id	pname	amount
                    1001	小米	1
                    1004	小米	4
                    1002	华为	2
                    1005	华为	5
                    1003	格力	3
                    1006	格力	6



 */
public class MJDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        //1.创建Job实例
        Job job = Job.getInstance(new Configuration());

        //2.给Job赋值
        job.setJarByClass(MJDriver.class);
        job.setMapperClass(MJMapper.class);
        //因为没有Reducer -- 一旦将ReduceTask设置为0那么不再排序（可以认为不走Shuffle）
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);//mapper输出的key
        job.setOutputValueClass(NullWritable.class);//mapper输出的value

        //添加缓存路径 file:///D:/io/input9/pd.txt
        job.addCacheFile(new URI("src/main/resources/testFiles/input6/pd.txt"));
        //map方法读取的文件的路径
        FileInputFormat.setInputPaths(job,new Path("src/main/resources/testFiles/input6/dir/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Z_temp\\TestFiles\\output14"));

        //提交Job
        job.waitForCompletion(true);
    }
}
