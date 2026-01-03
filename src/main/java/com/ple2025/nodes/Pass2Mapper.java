package com.ple2025.nodes;

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
import java.nio.file.FileSystems;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Pass2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Gson GSON = new Gson();
    private Set<String> frequentArchetypes = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = new Path(cacheFiles[0]);
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(path.toString()))) {
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    frequentArchetypes.add(line.trim());
                    count++;
                }
                System.err.println("Loaded " + count + " archetypes into whitelist");
            }
        } else {
            System.err.println("NO CACHE FILES LOADED!"); // ← This might be your issue
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty())
            return;

        GameRecord game;
        try {
            game = GSON.fromJson(line, GameRecord.class);
        } catch (Exception e) {
            return;
        }

        if (game == null || game.getPlayers() == null || game.getPlayers().size() != 2)
            return;

        for (Player p : game.getPlayers()) {
            if (p.getDeck() != null && isValidDeck(p.getDeck())) {
                // Get only frequent archetypes
                List<String> archetypes = DeckUtils.getArchetypes(p.getDeck(), frequentArchetypes);
                for (String arch : archetypes) {
                    String winFlag = p.isWon() ? "1" : "0";
                    context.write(new Text(arch), new Text("1," + winFlag));
                }
            }
        }
    }

    private boolean isValidDeck(String deck) {
        if (deck == null || deck.length() != 16)
            return false;
        for (int i = 0; i < 16; i++) {
            char c = deck.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }
}
