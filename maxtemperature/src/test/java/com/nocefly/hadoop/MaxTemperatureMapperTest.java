package com.nocefly.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MaxTemperatureMapperTest {
    private MapDriver mapDriver;

    @Before
    public void setUp(){
        MaxTemperatureMapper mapper = new MaxTemperatureMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        Text text = new Text("0029029070999991901122906004+64333+023450FM-12+000599999V0209991C000019999999N0000001N9-01281+99999101121ADDGF106991999999999999999999\n");
        mapDriver.withInput(new LongWritable(),text);
        mapDriver.withOutput(new Text("1901"),new IntWritable(-128));
        mapDriver.runTest();
    }

}
