package com.ple2025.nodes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Pass2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        long wins = 0;
        for (Text v : values) {
            String[] parts = v.toString().split(",");
            if (parts.length == 2) {
                count += Long.parseLong(parts[0]);
                wins += Long.parseLong(parts[1]);
            }
        }
        // Optional: re-apply min-support (redundant if whitelist is correct, but safe)
        int deckSize = key.getLength() / 2;
        long minSupport = (deckSize == 8) ? 1 : 1;
        if (count >= minSupport) {
            context.write(new Text(key + ";" + count + ";" + wins), new Text(""));
        }
    }
}
