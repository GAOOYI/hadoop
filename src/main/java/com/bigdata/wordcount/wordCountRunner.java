package com.bigdata.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class wordCountRunner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //定义一个job
        Job job = Job.getInstance(conf);
        //指定job所在的jarbao
        job.setJarByClass(wordCountRunner.class);
        //指定mapper reduce对象
        job.setReducerClass(wordCountReduce.class);
        job.setMapperClass(wordCountMapper.class);
        //指定mapp的输出和value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce的输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置要处理的数据在hdfs上的位置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //向yarn提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
