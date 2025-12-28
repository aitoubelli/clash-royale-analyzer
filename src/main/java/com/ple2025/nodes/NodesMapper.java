package com.ple2025.nodes;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.ple2025.model.GameRecord;
import com.ple2025.model.Player;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class NodesMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Gson GSON = new Gson();
    private static final Text OUT_KEY = new Text();
    private static final Text OUT_VALUE = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        GameRecord game;
        try {
            game = GSON.fromJson(line, GameRecord.class);
        } catch (JsonSyntaxException e) {
            return;
        }

        if (game == null || game.getPlayers() == null || game.getPlayers().size() != 2) {
            return;
        }

        int winnerIndex = game.getWinner();

        List<Player> players = game.getPlayers();

        for (int i = 0; i < players.size(); i++) {

            Player p = players.get(i);

            // 🔹 USTAWIAMY WINNERA
            p.setWinner(i == winnerIndex);

            String deckStr = p.getDeck();
            if (deckStr == null || deckStr.length() != 16) continue;

            // 🔹 DECK → LISTA 8 KART
            List<String> cards = new ArrayList<>(8);
            for (int j = 0; j < 16; j += 2) {
                cards.add(deckStr.substring(j, j + 2));
            }

            if (cards.size() != 8) continue;

            Collections.sort(cards);

            int win = p.isWinner() ? 1 : 0;

            List<String> archetypes = generateSubDecks(cards);

            for (String archetype : archetypes) {
                OUT_KEY.set(archetype);
                OUT_VALUE.set("1;" + win);
                context.write(OUT_KEY, OUT_VALUE);
            }
        }
    }

    private List<String> generateSubDecks(List<String> cards) {
        List<String> result = new ArrayList<>();
        int n = cards.size();

        for (int k = 1; k <= n; k++) {
            generateCombinations(cards, k, 0, new ArrayList<>(), result);
        }
        return result;
    }

    private void generateCombinations(List<String> cards,
                                      int k,
                                      int start,
                                      List<String> current,
                                      List<String> result) {

        if (current.size() == k) {
            result.add(String.join("", current));
            return;
        }

        for (int i = start; i < cards.size(); i++) {
            current.add(cards.get(i));
            generateCombinations(cards, k, i + 1, current, result);
            current.remove(current.size() - 1);
        }
    }
}
