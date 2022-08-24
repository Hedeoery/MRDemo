package com.hedeoer.mr.mapjoin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //用来缓存pd.txt中的数据。key是pid  value是pname
    private Map<String,String> map = new HashMap<String,String >();
    //封装的key
    private Text outKey = new Text();
    /*
        缓存数据
        该方法在任务开始时候调用一次。在map方法前调用
        作用：初始化
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = null;
        FileSystem fs = null;
        try {
            //1.创建文件系统对象
            fs = FileSystem.get(context.getConfiguration());
            //2.创建流
            //2.1获取缓存路径
            URI[] cacheFiles = context.getCacheFiles();
            //2.2创建流---因为要一行一行读数据所以使用BufferedReader
            //因为FSDataInputStream是字节流所以还要使用转换流InputStreamReader
            br =
                    new BufferedReader(
                            new InputStreamReader(
                                    fs.open(new Path(cacheFiles[0])),"UTF-8"));
            //3.读取数据
            String line = "";
            while ((line = br.readLine()) != null) {
                //4.切割数据并将数据放到map中
                String[] split = line.split("\t"); //01	小米
                map.put(split[0], split[1]);
            }
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }finally {
            if (br != null){
                br.close();
            }
            if (fs != null){
                fs.close();
            }
        }
    }

    /*
        在任务结束的时候调用一次此方法。在map方法执行后执行.
        作用 ： 关闭资源
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }

    /*
                1.读取order.txt中的内容1001	01	1
                2.和缓存中的数据(pd.txt  01	小米)进行join
                3.将数据写出去
             */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1001	01	1
        String[] line = value.toString().split("\t");
        //通过pid获取对应的pname -- 通过map获取
        String pname = map.get(line[1]);
        //进行字符串拼接
        String info = line[0] + "\t" + pname + "\t" + line[2];
        //将String转成Text(key,value封装)
        outKey.set(info);
        //将key,value写出去
        context.write(outKey,NullWritable.get());
    }
}
