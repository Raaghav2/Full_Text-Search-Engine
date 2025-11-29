package org.cs7is3;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lucene indexer for CS7IS3 Assignment 2 document collection
 * Supports FBIS, Financial Times, LA Times, and Federal Register documents
 */
public class Indexer {
    
    private static final Pattern DOCNO_PATTERN = Pattern.compile("<DOCNO>\\s*([^<]+?)\\s*</DOCNO>");
    private static final Pattern HEADLINE_PATTERN = Pattern.compile("<HEADLINE>\\s*(.*?)\\s*</HEADLINE>", Pattern.DOTALL);
    private static final Pattern TEXT_PATTERN = Pattern.compile("<TEXT>\\s*(.*?)\\s*</TEXT>", Pattern.DOTALL);
    private static final Pattern DATE_PATTERN = Pattern.compile("<DATE>\\s*([^<]+?)\\s*</DATE>");
    
    public void buildIndex(Path docsPath, Path indexPath) throws IOException {
        System.out.println("Building Lucene index...");
        System.out.println("Documents path: " + docsPath);
        System.out.println("Index path: " + indexPath);
        
        // Create index directory if it doesn't exist
        Files.createDirectories(indexPath);
        
        // Initialize Lucene IndexWriter with English analyzer
        EnglishAnalyzer analyzer = new EnglishAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        
        try (IndexWriter writer = new IndexWriter(FSDirectory.open(indexPath), config)) {
            
            int totalDocs = 0;
            
            // Index FBIS documents
            Path fbisPath = docsPath.resolve("fbis");
            if (Files.exists(fbisPath)) {
                totalDocs += indexDirectory(writer, fbisPath, "FBIS");
            }
            
            // Index Financial Times documents
            Path ftPath = docsPath.resolve("ft");
            if (Files.exists(ftPath)) {
                totalDocs += indexFinancialTimes(writer, ftPath);
            }
            
            // Index LA Times documents
            Path latimesPath = docsPath.resolve("latimes");
            if (Files.exists(latimesPath)) {
                totalDocs += indexDirectory(writer, latimesPath, "LATIMES");
            }
            
            // Index Federal Register documents
            Path fr94Path = docsPath.resolve("fr94");
            if (Files.exists(fr94Path)) {
                totalDocs += indexDirectory(writer, fr94Path, "FR94");
            }
            
            writer.commit();
            System.out.println("Indexing completed! Total documents indexed: " + totalDocs);
        }
    }
    
    private int indexDirectory(IndexWriter writer, Path directory, String source) throws IOException {
        System.out.println("Indexing " + source + " documents from: " + directory);
        int count = 0;
        
        if (Files.isDirectory(directory)) {
            Files.walk(directory)
                .filter(Files::isRegularFile)
                .filter(path -> !path.getFileName().toString().startsWith("read"))
                .forEach(file -> {
                    try {
                        String content = Files.readString(file);
                        int docsInFile = parseAndIndexDocuments(writer, content, source);
                        System.out.println("Indexed " + docsInFile + " documents from " + file.getFileName());
                    } catch (Exception e) {
                        System.err.println("Error processing file " + file + ": " + e.getMessage());
                    }
                });
        }
        
        return count;
    }
    
    private int indexFinancialTimes(IndexWriter writer, Path ftPath) throws IOException {
        System.out.println("Indexing Financial Times documents from: " + ftPath);
        int totalCount = 0;
        
        if (Files.isDirectory(ftPath)) {
            Files.walk(ftPath)
                .filter(Files::isDirectory)
                .filter(path -> path.getFileName().toString().startsWith("ft"))
                .forEach(yearDir -> {
                    try {
                        Files.walk(yearDir)
                            .filter(Files::isRegularFile)
                            .filter(path -> !path.getFileName().toString().startsWith("read"))
                            .forEach(file -> {
                                try {
                                    String content = Files.readString(file);
                                    int docsInFile = parseAndIndexDocuments(writer, content, "FT");
                                    System.out.println("Indexed " + docsInFile + " documents from " + file.getFileName());
                                } catch (Exception e) {
                                    System.err.println("Error processing FT file " + file + ": " + e.getMessage());
                                }
                            });
                    } catch (Exception e) {
                        System.err.println("Error processing FT directory " + yearDir + ": " + e.getMessage());
                    }
                });
        }
        
        return totalCount;
    }
    
    private int parseAndIndexDocuments(IndexWriter writer, String content, String source) throws IOException {
        int count = 0;
        
        // Split content by <DOC> tags
        String[] docs = content.split("(?=<DOC>)");
        
        for (String docContent : docs) {
            if (docContent.trim().startsWith("<DOC>")) {
                try {
                    Document doc = parseDocument(docContent, source);
                    if (doc != null) {
                        writer.addDocument(doc);
                        count++;
                    }
                } catch (Exception e) {
                    System.err.println("Error parsing document: " + e.getMessage());
                }
            }
        }
        
        return count;
    }
    
    private Document parseDocument(String docContent, String source) {
        // Extract DOCNO (required)
        Matcher docnoMatcher = DOCNO_PATTERN.matcher(docContent);
        if (!docnoMatcher.find()) {
            return null; // Skip documents without DOCNO
        }
        String docno = docnoMatcher.group(1).trim();
        
        // Extract HEADLINE/TITLE
        String headline = "";
        Matcher headlineMatcher = HEADLINE_PATTERN.matcher(docContent);
        if (headlineMatcher.find()) {
            headline = cleanText(headlineMatcher.group(1));
        }
        
        // Extract TEXT
        String text = "";
        Matcher textMatcher = TEXT_PATTERN.matcher(docContent);
        if (textMatcher.find()) {
            text = cleanText(textMatcher.group(1));
        }
        
        // Extract DATE
        String date = "";
        Matcher dateMatcher = DATE_PATTERN.matcher(docContent);
        if (dateMatcher.find()) {
            date = dateMatcher.group(1).trim();
        }
        
        // Create Lucene document
        Document document = new Document();
        
        // Add fields
        document.add(new StoredField("DOCNO", docno));
        document.add(new TextField("DOCNO", docno, Field.Store.YES));
        
        if (!headline.isEmpty()) {
            document.add(new TextField("TITLE", headline, Field.Store.YES));
        }
        
        if (!text.isEmpty()) {
            document.add(new TextField("TEXT", text, Field.Store.NO));
        }
        
        if (!date.isEmpty()) {
            document.add(new StoredField("DATE", date));
        }
        
        document.add(new StoredField("SOURCE", source));
        
        // Combined content for search (title is more important)
        String allContent = headline + " " + text;
        if (!allContent.trim().isEmpty()) {
            document.add(new TextField("CONTENT", allContent, Field.Store.NO));
        }
        
        return document;
    }
    
    private String cleanText(String text) {
        if (text == null) return "";
        
        // Remove extra whitespace and newlines
        text = text.replaceAll("\\s+", " ");
        // Remove some common XML artifacts
        text = text.replaceAll("&amp;", "&");
        text = text.replaceAll("&lt;", "<");
        text = text.replaceAll("&gt;", ">");
        
        return text.trim();
    }
}