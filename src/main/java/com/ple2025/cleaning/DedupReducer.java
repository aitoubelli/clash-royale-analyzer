package com.ple2025.cleaning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DedupReducer extends Reducer<Text, Text, Text, org.apache.hadoop.io.NullWritable> {

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

        // Deduplication logic:
        // 1. Exact matches: same line
        // 2. Same game duplicates: within 5 seconds

        List<TimedLine> uniqueGames = new ArrayList<>();
        if (!timedLines.isEmpty()) {
            TimedLine current = timedLines.get(0);
            uniqueGames.add(current);

            Set<String> seenLinesInCurrentGame = new HashSet<>();
            seenLinesInCurrentGame.add(current.line);

            for (int i = 1; i < timedLines.size(); i++) {
                TimedLine next = timedLines.get(i);

                // If it's an exact match of a line we've seen in THIS game group
                if (seenLinesInCurrentGame.contains(next.line)) {
                    context.getCounter(CleanMapper.Validation.EXACT_DUPLICATE).increment(1);
                    continue;
                }

                // If it's within 5 seconds of the current game's first record, it's a "same game" duplicate
                if (Math.abs(next.timestamp - current.timestamp) <= 5) {
                    context.getCounter(CleanMapper.Validation.SAME_GAME_DUPLICATE).increment(1);
                    seenLinesInCurrentGame.add(next.line); // Still mark the line as seen for exact match check
                } else {
                    // New game
                    current = next;
                    uniqueGames.add(current);
                    seenLinesInCurrentGame.clear();
                    seenLinesInCurrentGame.add(current.line);
                }
            }
        }

        for (TimedLine game : uniqueGames) {
            context.write(new Text(game.line), org.apache.hadoop.io.NullWritable.get());
        }
    }
}
