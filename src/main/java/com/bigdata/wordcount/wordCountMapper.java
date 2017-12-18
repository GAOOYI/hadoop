package com.bigdata.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//手动指定泛型
//keyin:  LongWritable    valuein: Text
//keyout: Text            valueout:IntWritable
public class wordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    //map方法的生命周期：  框架每传一行数据就被调用一次
    //key :  这一行的起始点在文件中的偏移量
    //value: 这一行的内容
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] words = s.split(" |\\,");
        for(String word:words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
