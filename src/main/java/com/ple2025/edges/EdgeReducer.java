package com.ple2025.edges;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EdgeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long count = 0, wins = 0;
        for (Text v : values) {
            String[] parts = v.toString().split(",");
            if (parts.length == 2) {
                count += Long.parseLong(parts[0]);
                wins += Long.parseLong(parts[1]);
            }
        }
        // Optional: skip edges with count < 2 (for stability)
        if (count >= 1) {
            context.write(key, new Text(count + ";" + wins));
        }
    }
}
