package com.ple2025.nodes;

import com.google.gson.Gson;
import com.ple2025.model.GameRecord;
import com.ple2025.model.Player;
import com.ple2025.utils.DeckUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class Pass1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final Gson GSON = new Gson();

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
                List<String> archetypes = DeckUtils.getArchetypesForPass1(p.getDeck());

                for (String arch : archetypes) {
                    context.write(new Text(arch), new LongWritable(1));
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
