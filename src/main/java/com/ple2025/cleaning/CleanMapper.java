package com.ple2025.cleaning;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.ple2025.model.GameRecord;
import com.ple2025.model.Player;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Gson GSON = new Gson();
    private static final DateTimeFormatter ISO_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    // Counter enum
    public enum Validation {
        TOTAL_LINES,
        VALID,
        INVALID_JSON,
        INVALID_PLAYERS,
        INVALID_DECK,
        INVALID_DATE,
        INVALID_UTAG,
        OTHER
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(Validation.TOTAL_LINES).increment(1);

        String line = value.toString().trim();
        if (line.isEmpty()) {
            context.getCounter(Validation.OTHER).increment(1);
            return;
        }

        GameRecord game;
        try {
            game = GSON.fromJson(line, GameRecord.class);
        } catch (JsonSyntaxException e) {
            context.getCounter(Validation.INVALID_JSON).increment(1);
            logError(context, "JSON", line);
            return;
        }

        if (game == null || game.getPlayers() == null || game.getPlayers().size() != 2) {
            context.getCounter(Validation.INVALID_PLAYERS).increment(1);
            logError(context, "PLAYERS", line);
            return;
        }

        List<Player> players = game.getPlayers();
        Player p1 = players.get(0);
        Player p2 = players.get(1);

        if (!isValidDeck(p1.getDeck()) || !isValidDeck(p2.getDeck())) {
            context.getCounter(Validation.INVALID_DECK).increment(1);
            logError(context, "DECK", line);
            return;
        }

        String utag1 = p1.getUtag();
        String utag2 = p2.getUtag();
        if (utag1 == null || utag2 == null) {
            context.getCounter(Validation.INVALID_UTAG).increment(1);
            logError(context, "UTAG", line);
            return;
        }

        // Normalize player order
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
            context.getCounter(Validation.INVALID_DATE).increment(1);
            logError(context, "DATE", line);
            return;
        }

        String dedupKey = utag1 + "|" + utag2 + "|" + roundedTimestamp + "|" + game.getRound();
        context.getCounter(Validation.VALID).increment(1);
        context.write(new Text(dedupKey), new Text(line));
    }

    private static long errorCount = 0;
    private static final long MAX_ERRORS_TO_LOG = 5;

    private void logError(Context context, String errorType, String line) {
        if (errorCount < MAX_ERRORS_TO_LOG) {
            try {
                context.write(new Text("__ERROR__ [" + errorType + "]"), new Text(line));
                errorCount++;
            } catch (Exception ignored) {}
        }
    }

    // Keep static methods for local testing
    public static String buildDedupKey(String line) {
        if (line == null || line.trim().isEmpty()) return null;

        GameRecord game;
        try {
            game = GSON.fromJson(line, GameRecord.class);
        } catch (JsonSyntaxException e) {
            return null;
        }

        if (game == null || game.getPlayers() == null || game.getPlayers().size() != 2) {
            return null;
        }

        List<Player> players = game.getPlayers();
        Player p1 = players.get(0);
        Player p2 = players.get(1);

        if (!isValidDeck(p1.getDeck()) || !isValidDeck(p2.getDeck())) {
            return null;
        }

        String utag1 = p1.getUtag();
        String utag2 = p2.getUtag();
        if (utag1 == null || utag2 == null) {
            return null;
        }

        // Normalize player order
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
}
