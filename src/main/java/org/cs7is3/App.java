package org.cs7is3;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
// TODO: Implement your main application class
// This class should handle command-line arguments and coordinate between Indexer and Searcher
// 
// Required command-line interface:
//   java -jar your-jar.jar index --docs "Assignment Two" --index index
//   java -jar your-jar.jar search --index index --topics topics --output runs/student.run --numDocs 1000
//
// The GitHub Actions workflow expects:
// 1. Maven build to succeed (mvn clean package)
// 2. Index command to create an index from the dataset
// 3. Search command to produce a TREC-format run file with exactly 1000 results per topic
// 4. Output format: "topic_id Q0 docno rank score run_tag"

public class App {

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            return;
        }

        String mode = args[0]; // "index" 或 "search"

        Path docsPath = null;
        Path indexPath = null;
        Path topicsPath = null;
        Path outputPath = null;
        int numDocs = 1000;

        // 解析命令行参数
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--docs":
                    docsPath = Paths.get(args[++i]);
                    break;
                case "--index":
                    indexPath = Paths.get(args[++i]);
                    break;
                case "--topics":
                    topicsPath = Paths.get(args[++i]);
                    break;
                case "--output":
                    outputPath = Paths.get(args[++i]);
                    break;
                case "--numDocs":
                    numDocs = Integer.parseInt(args[++i]);
                    break;
                default:
                    System.err.println("Unknown argument: " + arg);
            }
        }

        try {
            if ("index".equalsIgnoreCase(mode)) {
                if (docsPath == null || indexPath == null) {
                    System.err.println("Missing --docs or --index for index mode.");
                    printUsage();
                    return;
                }
                Indexer indexer = new Indexer();
                indexer.buildIndex(docsPath, indexPath);

            } else if ("search".equalsIgnoreCase(mode)) {
                if (indexPath == null || topicsPath == null || outputPath == null) {
                    System.err.println("Missing --index or --topics or --output for search mode.");
                    printUsage();
                    return;
                }
                Searcher searcher = new Searcher();
                searcher.searchTopics(indexPath, topicsPath, outputPath, numDocs);

            } else {
                System.err.println("Unknown mode: " + mode);
                printUsage();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar target/cs7is3-search-1.0.0.jar index "
                + "--docs \"Assignment Two\" --index index");
        System.out.println("  java -jar target/cs7is3-search-1.0.0.jar search "
                + "--index index --topics topics --output runs/student.run --numDocs 1000");
    }
}
