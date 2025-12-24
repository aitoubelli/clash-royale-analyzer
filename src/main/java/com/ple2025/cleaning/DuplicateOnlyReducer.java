package com.ple2025.cleaning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DuplicateOnlyReducer extends Reducer<Text, Text, Text, org.apache.hadoop.io.NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<Text> copies = new ArrayList<>();
        for (Text val : values) {
            // Make a copy because Hadoop reuses the Text object
            copies.add(new Text(val));
        }

        // Only output if duplicate (2 or more occurrences)
        if (copies.size() >= 2) {
            for (Text line : copies) {
                context.write(line, org.apache.hadoop.io.NullWritable.get());
            }
        }
    }
}
