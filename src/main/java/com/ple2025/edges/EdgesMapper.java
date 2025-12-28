package com.ple2025.edges;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.ple2025.model.GameRecord;
import com.ple2025.model.Player;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EdgesMapper extends Mapper<LongWritable, Text, Text, Text> {

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

        Player p0 = game.getPlayers().get(0);
        Player p1 = game.getPlayers().get(1);

        if (p0.getDeck() == null || p1.getDeck() == null) return;
        if (p0.getDeck().length() != 16 || p1.getDeck().length() != 16) return;

        int winner = game.getWinner();

        // p0 → p1
        OUT_KEY.set(p0.getDeck() + ";" + p1.getDeck());
        OUT_VALUE.set("1;" + (winner == 0 ? 1 : 0));
        context.write(OUT_KEY, OUT_VALUE);

        // p1 → p0
        OUT_KEY.set(p1.getDeck() + ";" + p0.getDeck());
        OUT_VALUE.set("1;" + (winner == 1 ? 1 : 0));
        context.write(OUT_KEY, OUT_VALUE);
    }
}
