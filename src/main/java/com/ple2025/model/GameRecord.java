package com.ple2025.model;

import java.util.List;

public class GameRecord {
    private String date;
    private int round;
    private List<Player> players;

    public String getDate() {
        return date;
    }

    public int getRound() {
        return round;
    }

    public List<Player> getPlayers() {
        return players;
    }

    public GameRecord() {}
}
