package com.bigdata.wash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Locale;

/**
 * 本地运行模式，设置NatiIO。java中的access为true
 */
public class wash {
    public static String inputpath = "file:///H:\\hadoopData\\wash\\input";
    public static String outputpath = "file:///H:\\hadoopData\\wash\\output";
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir","D:\\Develop\\hadoop-2.6.1\\hadoop-2.6.1" );
        //定义一个job
        Job job = Job.getInstance(conf);
        //指定job所在的jarbao
        job.setJarByClass(wash.class);
        //指定mapper reduce对象
        //job.setReducerClass(washReduce.class);
        job.setMapperClass(washMapper.class);
        //指定mapp的输出和value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce的输出key和value的类型
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Flowbean.class);

        //设置所需的切片数目
        //job.setNumReduceTasks(5);
        //设置分区方法
        //job.setPartitionerClass(flowCountPartition.class);

        //设置要处理的数据在hdfs上的位置
        FileInputFormat.setInputPaths(job,new Path(inputpath));
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(new Path(outputpath)))
        {
            fileSystem.delete(new Path(outputpath), true);
        }

        FileOutputFormat.setOutputPath(job,new Path(outputpath));

        //向yarn提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

}
class  washMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    Text k = new Text();
    NullWritable v = NullWritable.get();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        accessBean bean = WashUtil.wash(line);
        if(bean.isValid() || bean.getTimeStamp() == null){
            return;
        }
        k.set(bean.toString());
        context.write(k,v);
    }
}

////reducer
//class washReduce extends Reducer<Text,Flowbean,Text,Flowbean> {
//    @Override
//    protected void reduce(Text key, Iterable<Flowbean> values, Context context) throws IOException, InterruptedException {
//
//    }
//}

class WashUtil {
    public static accessBean wash(String line){
        accessBean ab = new accessBean();
        String[] split = line.split(" ");
        if (split.length > 11) {
            ab.setTimeStamp(parserTime(split[3].substring(1)));
            ab.setIPAddress(split[0]);
            ab.setRequestURL(split[6]);
            ab.setReferal(split[10]);
            ab.setStatus(split[8]);

            if (Integer.parseInt(ab.getStatus()) >= 400) {// 大于400，HTTP错误
                ab.setValid(false);
            }

        }else {
            ab.setValid(false);
        }
        return ab;
    }
    static SimpleDateFormat sd1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    static SimpleDateFormat sd2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);

    public static String parserTime(String time) {
        String newtime = null;
        try {
            newtime = sd2.format(sd1.parse(time));
            return newtime;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return "";
    }

}
