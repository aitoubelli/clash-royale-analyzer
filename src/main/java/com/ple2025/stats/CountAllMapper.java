package com.ple2025.stats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountAllMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains("\t")) {
            String[] parts = line.split("\t");
            if (parts.length >= 2) {
                String[] countWin = parts[1].split(";");
                if (countWin.length >= 1) {
                    try {
                        long count = Long.parseLong(countWin[0]);
                        context.write(new Text("total"), new LongWritable(count));
                    } catch (NumberFormatException ignored) {}
                }
            }
        }
    }
}
