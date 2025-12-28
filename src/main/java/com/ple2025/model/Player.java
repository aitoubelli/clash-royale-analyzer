package com.ple2025.model;

public class Player {
    private String utag;
    private String deck;
    private boolean winner;

    public String getUtag() {
        return utag;
    }

    public String getDeck() {
        return deck;
    }
    public boolean isWinner() {
        return winner;
    }

    public void setWinner(boolean winner) {
        this.winner = winner;
    }

    public Player() {}
}
