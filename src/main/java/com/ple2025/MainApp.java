package com.ple2025;

import com.ple2025.cleaning.DataCleaningJob;
import com.ple2025.nodes.Pass1Job;
import com.ple2025.nodes.Pass2Job;
import com.ple2025.utils.WhitelistBuilder;

public class MainApp {
    public static void main(String[] args) {
        if (args.length == 0 || args[0].equals("--help")) {
            printHelp();
            return;
        }

        String command = args[0];
        String[] subArgs = new String[args.length - 1];
        System.arraycopy(args, 1, subArgs, 0, subArgs.length);

        try {
            switch (command) {
                case "clean":
                    DataCleaningJob.main(subArgs);
                    break;
                case "pass1":
                    Pass1Job.main(subArgs);
                    break;
                case "pass2":
                    Pass2Job.main(subArgs);
                    break;
                case "whitelist":
                    WhitelistBuilder.main(subArgs);
                    break;
                default:
                    System.err.println("Unknown command: " + command);
                    printHelp();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printHelp() {
        System.out.println("Usage: MainApp <command> [args...]");
        System.out.println("Commands:");
        System.out.println("  clean <input> <output> <mode>    - Run data cleaning. Mode: 'clean' or 'duplicates'");
        System.out.println("  pass1 <input> <output>           - Run Pass 1: Archetype Frequencies");
        System.out.println("  pass2 <input> <whitelist> <output> - Run Pass 2: Filtered Nodes");
        System.out.println("  whitelist <input> <output> <threshold> - Generate whitelist");
        System.out.println("  --help                           - Show this help");
    }
}
