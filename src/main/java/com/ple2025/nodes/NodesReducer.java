package com.ple2025.nodes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NodesReducer extends Reducer<Text, Text, Text, NullWritable> {

    private static final Text OUT_KEY = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        long win = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(";");
            if (parts.length != 2) continue;

            try {
                count += Long.parseLong(parts[0]);
                win += Long.parseLong(parts[1]);
            } catch (NumberFormatException ignored) {}
        }

        OUT_KEY.set(key.toString() + ";" + count + ";" + win);
        context.write(OUT_KEY, NullWritable.get());
    }
}
