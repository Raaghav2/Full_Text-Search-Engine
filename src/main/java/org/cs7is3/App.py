package org.cs7is3;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Main application class for CS7IS3 Search Engine
 * Handles command-line arguments and coordinates between Indexer and Searcher
 */
public class App {
    
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }
        
        try {
            String command = args[0];
            
            if ("index".equals(command)) {
                handleIndexCommand(args);
            } else if ("search".equals(command)) {
                handleSearchCommand(args);
            } else {
                System.err.println("Unknown command: " + command);
                printUsage();
                System.exit(1);
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void handleIndexCommand(String[] args) throws Exception {
        String docsPath = null;
        String indexPath = null;
        
        // Parse arguments: index --docs "Assignment Two" --index index
        for (int i = 1; i < args.length; i++) {
            if ("--docs".equals(args[i]) && i + 1 < args.length) {
                docsPath = args[++i];
            } else if ("--index".equals(args[i]) && i + 1 < args.length) {
                indexPath = args[++i];
            }
        }
        
        if (docsPath == null || indexPath == null) {
            System.err.println("Missing required arguments for index command");
            System.err.println("Usage: java -jar app.jar index --docs <docs_path> --index <index_path>");
            System.exit(1);
        }
        
        System.out.println("Starting indexing...");
        System.out.println("Documents: " + docsPath);
        System.out.println("Index: " + indexPath);
        
        Indexer indexer = new Indexer();
        indexer.buildIndex(Paths.get(docsPath), Paths.get(indexPath));
        
        System.out.println("Indexing completed successfully!");
    }
    
    private static void handleSearchCommand(String[] args) throws Exception {
        String indexPath = null;
        String topicsPath = null;
        String outputPath = null;
        int numDocs = 1000; // default
        
        // Parse arguments: search --index index --topics topics --output runs/student.run --numDocs 1000
        for (int i = 1; i < args.length; i++) {
            if ("--index".equals(args[i]) && i + 1 < args.length) {
                indexPath = args[++i];
            } else if ("--topics".equals(args[i]) && i + 1 < args.length) {
                topicsPath = args[++i];
            } else if ("--output".equals(args[i]) && i + 1 < args.length) {
                outputPath = args[++i];
            } else if ("--numDocs".equals(args[i]) && i + 1 < args.length) {
                numDocs = Integer.parseInt(args[++i]);
            }
        }
        
        if (indexPath == null || topicsPath == null || outputPath == null) {
            System.err.println("Missing required arguments for search command");
            System.err.println("Usage: java -jar app.jar search --index <index_path> --topics <topics_path> --output <output_path> --numDocs <num_docs>");
            System.exit(1);
        }
        
        System.out.println("Starting search...");
        System.out.println("Index: " + indexPath);
        System.out.println("Topics: " + topicsPath);
        System.out.println("Output: " + outputPath);
        System.out.println("Number of docs: " + numDocs);
        
        Searcher searcher = new Searcher();
        searcher.searchTopics(Paths.get(indexPath), Paths.get(topicsPath), 
                             Paths.get(outputPath), numDocs);
        
        System.out.println("Search completed successfully!");
    }
    
    private static void printUsage() {
        System.out.println("CS7IS3 Search Engine");
        System.out.println("Usage:");
        System.out.println("  java -jar app.jar index --docs <docs_path> --index <index_path>");
        System.out.println("  java -jar app.jar search --index <index_path> --topics <topics_path> --output <output_path> --numDocs <num_docs>");
        System.out.println("");
        System.out.println("Examples:");
        System.out.println("  java -jar app.jar index --docs \"Assignment Two\" --index index");
        System.out.println("  java -jar app.jar search --index index --topics topics --output runs/student.run --numDocs 1000");
    }
}
 