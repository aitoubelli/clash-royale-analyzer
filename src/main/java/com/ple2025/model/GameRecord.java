package com.ple2025.model;

import java.util.List;

public class GameRecord {
    private String date;
    private int round;
    private List<Player> players;
    private int winner;

    public String getDate() {
        return date;
    }

    public int getRound() {
        return round;
    }
    public List<Player> getPlayers() {
        return players;
    }
    
    public int getWinner() {
        return winner;
    }

    public GameRecord() {}
}
