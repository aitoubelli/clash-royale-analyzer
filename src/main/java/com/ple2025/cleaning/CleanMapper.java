package com.ple2025.cleaning;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.ple2025.model.GameRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Gson GSON = new Gson();
    private static final DateTimeFormatter ISO_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    // Static method for local testing
    public static String buildDedupKey(String jsonLine) {
        if (jsonLine == null || jsonLine.trim().isEmpty()) return null;

        GameRecord game;
        try {
            game = GSON.fromJson(jsonLine.trim(), GameRecord.class);
        } catch (JsonSyntaxException e) {
            return null;
        }

        if (game == null || game.getPlayers() == null || game.getPlayers().size() != 2) {
            return null;
        }

        var players = game.getPlayers();
        var p1 = players.get(0);
        var p2 = players.get(1);

        if (!isValidDeck(p1.getDeck()) || !isValidDeck(p2.getDeck())) {
            return null;
        }

        String utag1 = p1.getUtag();
        String utag2 = p2.getUtag();
        if (utag1 == null || utag2 == null) return null;

        if (utag1.compareTo(utag2) > 0) {
            String tmp = utag1;
            utag1 = utag2;
            utag2 = tmp;
        }

        long roundedTimestamp;
        try {
            Instant instant = Instant.from(ISO_FORMAT.parse(game.getDate()));
            roundedTimestamp = (instant.getEpochSecond() / 5) * 5;
        } catch (DateTimeParseException e) {
            return null;
        }

        return utag1 + "|" + utag2 + "|" + roundedTimestamp + "|" + game.getRound();
    }

    // Public static deck validator
    public static boolean isValidDeck(String deck) {
        if (deck == null || deck.length() != 16) return false;
        for (int i = 0; i < 16; i++) {
            char c = deck.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    // Original Hadoop map method (reuses static logic)
    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String line = value.toString();
        String dedupKey = buildDedupKey(line);
        if (dedupKey != null) {
            try {
                context.write(new Text(dedupKey), new Text(line));
            } catch (Exception ignored) {}
        }
    }
}
