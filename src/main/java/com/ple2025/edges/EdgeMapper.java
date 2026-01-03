package com.ple2025.edges;

import com.google.gson.Gson;
import com.ple2025.model.GameRecord;
import com.ple2025.model.Player;
import com.ple2025.utils.DeckUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class EdgeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Gson GSON = new Gson();
    private Set<String> frequentArchetypes = new HashSet<>();
    private Map<Integer, Set<String>> frequentBySize = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(cacheFiles[0].toString()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String arch = line.trim();
                    frequentArchetypes.add(arch);
                    int k = arch.length() / 2;
                    frequentBySize.computeIfAbsent(k, k1 -> new HashSet<>()).add(arch);
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        GameRecord game;
        try {
            game = GSON.fromJson(line, GameRecord.class);
        } catch (Exception e) { return; }

        if (game == null || game.getPlayers().size() != 2) return;

        Player p1 = game.getPlayers().get(0);
        Player p2 = game.getPlayers().get(1);

        if (!isValidDeck(p1.getDeck()) || !isValidDeck(p2.getDeck())) return;

        // Get frequent archetypes per player, grouped by size
        Map<Integer, List<String>> archs1 = getFrequentArchetypesBySize(p1.getDeck());
        Map<Integer, List<String>> archs2 = getFrequentArchetypesBySize(p2.getDeck());

        // Emit pairs only for k=8 (full decks) - one pair per match
        int k = 8;
        if (archs1.containsKey(k) && archs2.containsKey(k)) {
            // Each deck should have exactly one k=8 archetype (the full deck)
            String a1 = archs1.get(k).get(0);
            String a2 = archs2.get(k).get(0);
            // Canonical ordering
            String src = a1.compareTo(a2) <= 0 ? a1 : a2;
            String dst = a1.compareTo(a2) <= 0 ? a2 : a1;
            boolean winBySrc = (a1.compareTo(a2) <= 0) ? p1.isWon() : p2.isWon();
            context.write(
                new Text(src + ";" + dst),
                new Text("1," + (winBySrc ? "1" : "0"))
            );
        }
    }

    private Map<Integer, List<String>> getFrequentArchetypesBySize(String deck) {
        Map<Integer, List<String>> result = new HashMap<>();
        List<String> allArchs = DeckUtils.getArchetypesForPass1(deck); // k=4,5,6,8 only
        for (String arch : allArchs) {
            if (frequentArchetypes.contains(arch)) {
                int k = arch.length() / 2;
                result.computeIfAbsent(k, k1 -> new ArrayList<>()).add(arch);
            }
        }
        return result;
    }

    private boolean isValidDeck(String d) {
        if (d == null || d.length() != 16) return false;
        for (int i = 0; i < 16; i++) {
            char c = d.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }
}
