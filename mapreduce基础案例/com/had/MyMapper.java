package com.had;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    private LongWritable one = new LongWritable(1);

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        // value -> "hello world java python"
        String[] split = value.toString().split(" ");
        // split -> ["hello","world","java","python"]

        for (String s:split){
            // set key and value
            context.write(new Text(s),one);
        }
    }

}
