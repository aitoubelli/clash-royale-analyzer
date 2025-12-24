package com.ple2025;

import com.ple2025.cleaning.CleanMapper;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class LocalTest {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java LocalTest <input.json> <duplicates_output.txt>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Map: dedupKey -> list of original JSON lines
        Map<String, List<String>> groups = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputPath))) {
            String line;
            long lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                String key = CleanMapper.buildDedupKey(line);
                if (key != null) {
                    groups.computeIfAbsent(key, k -> new ArrayList<>()).add(line);
                }
                // Optional: progress
                if (lineNum % 50000 == 0) {
                    System.out.println("Processed " + lineNum + " lines...");
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading input: " + e.getMessage());
            System.exit(1);
        }

        // Write only duplicates (2+ occurrences)
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputPath)))) {
            int totalDupLines = 0;
            for (List<String> group : groups.values()) {
                if (group.size() >= 2) {
                    for (String jsonLine : group) {
                        writer.println(jsonLine);
                        totalDupLines++;
                    }
                }
            }
            System.out.println("✅ Done.");
            System.out.println("Total duplicate lines written: " + totalDupLines);
            System.out.println("Output file: " + outputPath);
        } catch (IOException e) {
            System.err.println("Error writing output: " + e.getMessage());
            System.exit(1);
        }
    }
}
