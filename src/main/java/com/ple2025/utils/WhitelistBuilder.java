package com.ple2025.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class WhitelistBuilder {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: WhitelistBuilder <input> <output> <threshold>");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        int threshold = Integer.parseInt(args[2]);

        try (BufferedReader reader = new BufferedReader(new FileReader(input));
             BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String archetype = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    if (count >= threshold) {
                        writer.write(archetype);
                        writer.newLine();
                    }
                }
            }
        }
    }
}
