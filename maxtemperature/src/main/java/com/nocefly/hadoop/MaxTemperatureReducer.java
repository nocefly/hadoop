package com.nocefly.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        for(IntWritable item:values){
            max = Math.max(max,item.get());
        }
        context.write(key,new IntWritable(max));
    }
}
