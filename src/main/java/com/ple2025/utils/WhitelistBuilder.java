package com.ple2025.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class WhitelistBuilder {
    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage: WhitelistBuilder <input> <output> <full_deck_threshold> <sub_archetype_threshold>");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        int fullDeckThreshold = Integer.parseInt(args[2]);      // for k=8 (16 hex chars)
        int subArchetypeThreshold = Integer.parseInt(args[3]);  // for k=4,5,6 (8,10,12 hex chars)

        try (BufferedReader reader = new BufferedReader(new FileReader(input));
             BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String archetype = parts[0];
                    int count = Integer.parseInt(parts[1]);

                    // Determine archetype size (each card = 2 hex chars)
                    int deckSize = archetype.length() / 2;
                    int threshold;

                    // Only consider valid sizes: k=4,5,6,8 (as per your pruning)
                    if (deckSize == 8) {
                        threshold = fullDeckThreshold;
                    } else if (deckSize == 4 || deckSize == 5 || deckSize == 6) {
                        threshold = subArchetypeThreshold;
                    } else {
                        // Skip invalid sizes (k=1,2,3,7)
                        continue;
                    }

                    if (count >= threshold) {
                        writer.write(archetype);
                        writer.newLine();
                    }
                }
            }
        }
    }
}
