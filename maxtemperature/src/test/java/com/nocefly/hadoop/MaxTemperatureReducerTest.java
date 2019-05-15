package com.nocefly.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class MaxTemperatureReducerTest {
    private ReduceDriver reduceDriver;

    @Before
    public void setUp(){
        MaxTemperatureReducer reducer = new MaxTemperatureReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> IntWritableList = Lists.newArrayList();
        IntWritableList.add(new IntWritable(340));
        IntWritableList.add(new IntWritable(240));
        IntWritableList.add(new IntWritable(320));
        IntWritableList.add(new IntWritable(330));
        IntWritableList.add(new IntWritable(310));
        reduceDriver.withInput(new Text("2016"), IntWritableList);
        reduceDriver.withOutput(new Text("2016"), new IntWritable(340));
        reduceDriver.runTest();
    }
}
