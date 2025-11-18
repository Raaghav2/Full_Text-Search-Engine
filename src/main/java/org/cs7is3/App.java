package org.cs7is3;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import java.io.IOException;

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

    private static String getArgValue(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equalsIgnoreCase(flag)) {
                return args[i + 1];
            }
        }
        return null;
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java -jar your-jar.jar <command> [options]");
            System.err.println("Commands: index, search");
            return;
        }

        String command = args[0].toLowerCase();

        try {
            if ("index".equals(command)) {
                // Command: java -jar your-jar.jar index --docs "Assignment Two" --index index
                
                String docsPathStr = getArgValue(args, "--docs");
                String indexPathStr = getArgValue(args, "--index");
                
                if (docsPathStr == null || indexPathStr == null) {
                    System.err.println("Error: 'index' command requires --docs and --index arguments.");
                    return;
                }
                
                Path docsPath = Paths.get(docsPathStr);
                Path indexPath = Paths.get(indexPathStr);
                
                System.out.println("--- Starting Indexing Process ---");
                System.out.println("Document Collection: " + docsPath.toAbsolutePath());
                System.out.println("Index Directory: " + indexPath.toAbsolutePath());
                
                Indexer indexer = new Indexer(new EnglishAnalyzer());
                indexer.buildIndex(docsPath, indexPath);
                System.out.println("--- Indexing Complete ---");

            } else if ("search".equals(command)) {
                // Command: java -jar your-jar.jar search --index index --topics topics --output runs/student.run --numDocs 1000
                
                String indexPathStr = getArgValue(args, "--index");
                String topicsPathStr = getArgValue(args, "--topics");
                String outputPathStr = getArgValue(args, "--output");
                String numDocsStr = getArgValue(args, "--numDocs");
                
                if (indexPathStr == null || topicsPathStr == null || outputPathStr == null || numDocsStr == null) {
                    System.err.println("Error: 'search' command requires --index, --topics, --output, and --numDocs arguments.");
                    return;
                }
                
                Path indexPath = Paths.get(indexPathStr);
                Path topicsPath = Paths.get(topicsPathStr);
                Path outputPath = Paths.get(outputPathStr);
                int numDocs = Integer.parseInt(numDocsStr);

                System.out.println("--- Starting Search Process ---");
                System.out.println("Index Path: " + indexPath.toAbsolutePath());
                System.out.println("Topics File: " + topicsPath.toAbsolutePath());
                System.out.println("Output File: " + outputPath.toAbsolutePath());
                System.out.println("Retrieving Top K: " + numDocs);
                

                Searcher searcher = new Searcher();
                searcher.searchTopics(indexPath, topicsPath, outputPath, numDocs);
                
            } else {
                System.err.println("Unknown command: " + args[0]);
                System.err.println("Available commands are 'index' and 'search'.");
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: --numDocs must be an integer.");
        } catch (IOException e) {
            System.err.println("An IO Error occurred: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}