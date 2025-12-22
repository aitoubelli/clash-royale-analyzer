package com.ple2025.cleaning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DedupReducer extends Reducer<Text, Text, Text, org.apache.hadoop.io.NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Emit only the first occurrence (any one is fine)
        Text first = values.iterator().next();
        context.write(first, org.apache.hadoop.io.NullWritable.get());
    }
}
