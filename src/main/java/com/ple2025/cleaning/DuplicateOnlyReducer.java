package com.ple2025.cleaning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DuplicateOnlyReducer extends Reducer<Text, Text, Text, org.apache.hadoop.io.NullWritable> {

    private static class TimedLine implements Comparable<TimedLine> {
        long timestamp;
        String line;

        TimedLine(long timestamp, String line) {
            this.timestamp = timestamp;
            this.line = line;
        }

        @Override
        public int compareTo(TimedLine o) {
            int cmp = Long.compare(this.timestamp, o.timestamp);
            if (cmp == 0) {
                return this.line.compareTo(o.line);
            }
            return cmp;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<TimedLine> timedLines = new ArrayList<>();
        for (Text val : values) {
            String valStr = val.toString();
            int firstPipe = valStr.indexOf('|');
            if (firstPipe != -1) {
                try {
                    long t = Long.parseLong(valStr.substring(0, firstPipe));
                    String line = valStr.substring(firstPipe + 1);
                    timedLines.add(new TimedLine(t, line));
                } catch (NumberFormatException ignored) {}
            }
        }

        if (timedLines.isEmpty()) return;

        Collections.sort(timedLines);

        // We want to output only those lines that are duplicates
        // A line is a duplicate if:
        // 1. It is an exact match of a previous line in this group
        // 2. It represents the "same game" (within 5s) but is not the FIRST occurrence of that game

        if (!timedLines.isEmpty()) {
            TimedLine currentFirstOfGame = timedLines.get(0);
            Set<String> seenLinesInCurrentGame = new HashSet<>();
            seenLinesInCurrentGame.add(currentFirstOfGame.line);

            for (int i = 1; i < timedLines.size(); i++) {
                TimedLine next = timedLines.get(i);

                boolean isExactMatch = seenLinesInCurrentGame.contains(next.line);
                boolean isSameGame = Math.abs(next.timestamp - currentFirstOfGame.timestamp) <= 5;

                if (isExactMatch) {
                    context.getCounter(CleanMapper.Validation.EXACT_DUPLICATE).increment(1);
                    context.write(new Text(next.line), org.apache.hadoop.io.NullWritable.get());
                } else if (isSameGame) {
                    context.getCounter(CleanMapper.Validation.SAME_GAME_DUPLICATE).increment(1);
                    context.write(new Text(next.line), org.apache.hadoop.io.NullWritable.get());
                    seenLinesInCurrentGame.add(next.line);
                } else {
                    // New game
                    currentFirstOfGame = next;
                    seenLinesInCurrentGame.clear();
                    seenLinesInCurrentGame.add(currentFirstOfGame.line);
                }
            }
        }
    }
}
