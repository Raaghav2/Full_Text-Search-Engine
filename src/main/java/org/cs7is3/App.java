package org.cs7is3;
 
import java.nio.file.Path;
import java.nio.file.Paths;
 
public class App {
    public static void main(String[] args) {
        if (args.length == 0) {
            usage();
            System.exit(1);
        }
 
        String command = args[0];
        try {
            if ("index".equalsIgnoreCase(command)) {
                Path docsPath = Paths.get("Assignment Two");
                Path indexPath = Paths.get("index");
 
                for (int i = 1; i < args.length; i++) {
                    if ("--docs".equals(args[i]) && i + 1 < args.length) {
                        docsPath = Paths.get(args[++i]);
                    } else if ("--index".equals(args[i]) && i + 1 < args.length) {
                        indexPath = Paths.get(args[++i]);
                    }
                }
 
                System.out.println("Indexing docs from: " + docsPath + " -> index: " + indexPath);
                new Indexer().buildIndex(docsPath, indexPath);
                System.out.println("Indexing completed.");
 
            } else if ("search".equalsIgnoreCase(command)) {
                Path indexPath = Paths.get("index");
                Path topicsPath = Paths.get("topics");
                Path outputPath = Paths.get("runs/student.run");
                int numDocs = 1000;
 
                for (int i = 1; i < args.length; i++) {
                    if ("--index".equals(args[i]) && i + 1 < args.length) {
                        indexPath = Paths.get(args[++i]);
                    } else if ("--topics".equals(args[i]) && i + 1 < args.length) {
                        topicsPath = Paths.get(args[++i]);
                    } else if ("--output".equals(args[i]) && i + 1 < args.length) {
                        outputPath = Paths.get(args[++i]);
                    } else if ("--numDocs".equals(args[i]) && i + 1 < args.length) {
                        numDocs = Integer.parseInt(args[++i]);
                    }
                }
 
                System.out.println("Searching topics: " + topicsPath + " using index: " + indexPath);
                new Searcher().searchTopics(indexPath, topicsPath, outputPath, numDocs);
                System.out.println("Searching completed. Results written to " + outputPath);
 
            } else {
                usage();
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }
 
    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  java -jar target/cs7is3-search-1.0.0.jar index --docs \"Assignment Two\" --index index");
        System.out.println("  java -jar target/cs7is3-search-1.0.0.jar search --index index --topics topics --output runs/student.run --numDocs 1000");
    }
}