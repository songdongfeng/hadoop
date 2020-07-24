package com.had;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, LongWritable,Text, LongWritable> {

    public void reduce(Text key,Iterable<LongWritable> values, Context context) throws IOException,InterruptedException {
        int sum = 0;
        // "hello"->[1,1,3,1,1]
        for (LongWritable val:values){
            sum+=val.get();
        }
        // "hello"->7
        context.write(key,new LongWritable(sum));

    }

}
