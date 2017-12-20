package com.bigdata.flowCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class flowCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //定义一个job
        Job job = Job.getInstance(conf);
        //指定job所在的jarbao
        job.setJarByClass(flowCount.class);
        //指定mapper reduce对象
        job.setReducerClass(flowCountReduce.class);
        job.setMapperClass(flowCountMapper.class);
        //指定mapp的输出和value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flowbean.class);

        //设置reduce的输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        //设置所需的切片数目
        job.setNumReduceTasks(5);
        //设置分区方法
        job.setPartitionerClass(flowCountPartition.class);

        //设置要处理的数据在hdfs上的位置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //向yarn提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
class flowCountMapper extends Mapper<LongWritable,Text,Text,Flowbean>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] keys = line.split("\t");
        Flowbean flowbean = new Flowbean(Long.parseLong(keys[keys.length - 3]), Long.parseLong(keys[keys.length - 2]));
        context.write(new Text(keys[1]),flowbean);
    }
}

class flowCountReduce extends Reducer<Text,Flowbean,Text,Flowbean> {
    @Override
    protected void reduce(Text key, Iterable<Flowbean> values, Context context) throws IOException, InterruptedException {
        long upFlowTotal = 0;
        long downFlowTotal = 0;
        for (Flowbean f:values
             ) {
            upFlowTotal += f.getUpFlow();
            downFlowTotal += f.getDownFlow();
        }
        Flowbean flowbean = new Flowbean(upFlowTotal, downFlowTotal);
        context.write(key,flowbean);
    }
}

class flowCountPartition extends Partitioner<Text,Flowbean>{
    private static Map<String,Integer> Dict = new HashMap<String, Integer>();
    static {
        Dict.put("135",0);
        Dict.put("136",1);
        Dict.put("137",2);
        Dict.put("138",3);
    }

    public int getPartition(Text text, Flowbean flowbean, int i) {
        String priex = text.toString().substring(0,3);
        Integer Id = Dict.get(priex);
        return Id == null?4:Id;
    }
}
