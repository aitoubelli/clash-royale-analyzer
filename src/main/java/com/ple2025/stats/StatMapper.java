package com.ple2025.stats;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StatMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, Long> nodeCounts = new HashMap<>();
    private long N_all;

    @Override
    protected void setup(Context context) throws IOException {
        // Load N_all from configuration
        N_all = context.getConfiguration().getLong("N_ALL", 1);

        // Load nodes from DistributedCache
        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (localCacheFiles != null && localCacheFiles.length > 0) {
            try (BufferedReader reader = new BufferedReader(new FileReader(localCacheFiles[0].toString()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(";");
                    if (parts.length >= 2) {
                        nodeCounts.put(parts[0], Long.parseLong(parts[1]));
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || !line.contains("\t")) return;

        String[] edgeParts = line.split("\t");
        if (edgeParts.length < 2) return;

        String[] srcDst = edgeParts[0].split(";");
        String[] countWin = edgeParts[1].split(";");

        if (srcDst.length != 2 || countWin.length < 2) return;

        String src = srcDst[0];
        String dst = srcDst[1];
        long obsCount = Long.parseLong(countWin[0]);
        long obsWin = Long.parseLong(countWin[1]);

        Long srcCount = nodeCounts.get(src);
        Long dstCount = nodeCounts.get(dst);

        if (srcCount == null || dstCount == null) {
            // Skip if node not found (shouldn't happen if consistent)
            return;
        }

        // Compute expected = (srcCount * dstCount) / N_all
        double expected = (double) srcCount * dstCount / N_all;

        // Format output as requested
        String output = String.format("%s;%s;%d;%d;%d,%d;%.1f",
            src, dst, obsCount, obsWin, srcCount, dstCount, expected);

        context.write(new Text(output), new Text(""));
    }
}
