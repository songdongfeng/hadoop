package com.had;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner  extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {

        switch (text.toString().length()%4){
            case 0:
                return 0;
            case 1:
                return 1;
            case 2:
                return 2;
            case 3:
                return 3;
        }
        return 0;
    }
}
