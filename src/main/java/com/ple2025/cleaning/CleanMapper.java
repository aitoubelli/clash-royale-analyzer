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
import java.util.List;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Gson GSON = new Gson();
    private static final DateTimeFormatter ISO_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        GameRecord game;
        try {
            game = GSON.fromJson(line, GameRecord.class);
        } catch (JsonSyntaxException e) {
            return; // skip malformed JSON
        }

        if (game == null || game.getPlayers() == null || game.getPlayers().size() != 2) {
            return;
        }

        List<com.ple2025.model.Player> players = game.getPlayers();
        com.ple2025.model.Player p1 = players.get(0);
        com.ple2025.model.Player p2 = players.get(1);

        // Validate decks: must be non-null, 16 hex chars = 8 cards
        if (!isValidDeck(p1.getDeck()) || !isValidDeck(p2.getDeck())) {
            return;
        }

        String utag1 = p1.getUtag();
        String utag2 = p2.getUtag();
        if (utag1 == null || utag2 == null) return;

        // Normalize player order (lexicographic)
        if (utag1.compareTo(utag2) > 0) {
            String tmp = utag1;
            utag1 = utag2;
            utag2 = tmp;
        }

        // Parse and round timestamp to nearest 5 seconds
        long roundedTimestamp;
        try {
            Instant instant = Instant.from(ISO_FORMAT.parse(game.getDate()));
            long epochSec = instant.getEpochSecond();
            roundedTimestamp = (epochSec / 5) * 5; // floor to 5s
        } catch (DateTimeParseException e) {
            return; // invalid date format
        }

        // Build deduplication key: player1|player2|rounded_time|round
        String dedupKey = utag1 + "|" + utag2 + "|" + roundedTimestamp + "|" + game.getRound();

        try {
            context.write(new Text(dedupKey), new Text(line));
        } catch (Exception e) {
            // skip on write error
        }
    }

    private boolean isValidDeck(String deck) {
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
