package com.ple2025.edges;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EdgesReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text OUT_VALUE = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        long wins = 0;

        for (Text v : values) {
            String[] parts = v.toString().split(";");
            count += Long.parseLong(parts[0]);
            wins += Long.parseLong(parts[1]);
        }

        OUT_VALUE.set(count + ";" + wins);
        context.write(key, OUT_VALUE);
    }
}
