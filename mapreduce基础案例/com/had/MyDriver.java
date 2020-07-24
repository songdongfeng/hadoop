package com.had;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MyDriver {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        if (args.length != 2){
            System.out.println("useage jarfilename args[0] args[1]");
            System.exit(-1);
        }

        FileSystem fs = FileSystem.get(conf);

        boolean isExists = fs.exists(new Path(args[1]));

        // delete output directory
        if (isExists){
            fs.delete(new Path(args[1]),true);
        }

        Job job = Job.getInstance(conf,"word count");

        job.setJarByClass(com.had.MyDriver.class);

        job.setMapperClass(com.had.MyMapper.class);

        job.setReducerClass(com.had.MyReducer.class);

        job.setCombinerClass(com.had.MyCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setPartitionerClass(com.had.MyPartitioner.class);
        job.setNumReduceTasks(4);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
