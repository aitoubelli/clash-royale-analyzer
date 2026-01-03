package com.ple2025.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DeckUtils {

    public static String canonicalizeDeck(String deck) {
        if (deck == null || deck.length() != 16)
            return "";
        List<Integer> cards = new ArrayList<>();
        try {
            for (int i = 0; i < 16; i += 2) {
                String hex = deck.substring(i, i + 2);
                cards.add(Integer.parseInt(hex, 16));
            }
        } catch (NumberFormatException e) {
            return "";
        }
        Collections.sort(cards);
        StringBuilder sb = new StringBuilder();
        for (int card : cards) {
            sb.append(String.format("%02x", card));
        }
        return sb.toString();
    }

    public static List<String> getArchetypes(String deckHex) {
        return getArchetypes(deckHex, null);
    }

    public static List<String> getArchetypes(String deckHex, Set<String> whitelist) {
        if (deckHex == null || deckHex.length() != 16)
            return Collections.emptyList();

        // Parse deck into list of byte (or int) card IDs
        List<Integer> cards = new ArrayList<>(8);
        try {
            for (int i = 0; i < 16; i += 2) {
                String cardHex = deckHex.substring(i, i + 2);
                cards.add(Integer.parseInt(cardHex, 16));
            }
        } catch (NumberFormatException e) {
            return Collections.emptyList();
        }
        Collections.sort(cards); // canonical order

        Set<String> result = new HashSet<>();
        // Generate all non-empty subsets (1 to 8 cards)
        for (int k = 1; k <= 8; k++) {
            generateSubsets(cards, 0, new ArrayList<>(), k, result);
        }

        if (whitelist != null) {
            result.retainAll(whitelist);
        }
        return new ArrayList<>(result);
    }

    public static List<String> getArchetypesForPass1(String deckHex) {
        if (deckHex == null || deckHex.length() != 16)
            return Collections.emptyList();

        // Parse and sort cards
        List<Integer> cards = new ArrayList<>(8);
        for (int i = 0; i < 16; i += 2) {
            cards.add(Integer.parseInt(deckHex.substring(i, i + 2), 16));
        }
        Collections.sort(cards);

        Set<String> result = new HashSet<>();
        // ONLY these k values: 4,5,6,8 (skip 1,2,3,7)
        int[] kValues = {4, 5, 6, 8};
        for (int k : kValues) {
            generateCombinations(cards, k, result);
        }
        return new ArrayList<>(result);
    }

    private static void generateSubsets(List<Integer> cards, int start, List<Integer> current,
            int k, Set<String> result) {
        if (current.size() == k) {
            StringBuilder sb = new StringBuilder();
            for (int card : current) {
                sb.append(String.format("%02x", card));
            }
            result.add(sb.toString());
            return;
        }
        for (int i = start; i < cards.size(); i++) {
            current.add(cards.get(i));
            generateSubsets(cards, i + 1, current, k, result);
            current.remove(current.size() - 1);
        }
    }

    // Explicit combination generator for clarity
    private static void generateCombinations(List<Integer> cards, int k, Set<String> result) {
        int[] indices = new int[k];
        for (int i = 0; i < k; i++) {
            indices[i] = i;
        }

        while (true) {
            // Build archetype from current combination
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < k; i++) {
                sb.append(String.format("%02x", cards.get(indices[i])));
            }
            result.add(sb.toString());

            // Generate next combination
            int target = k - 1;
            while (target >= 0 && indices[target] == cards.size() - k + target) {
                target--;
            }
            if (target < 0) break;

            indices[target]++;
            for (int i = target + 1; i < k; i++) {
                indices[i] = indices[i - 1] + 1;
            }
        }
    }
}
