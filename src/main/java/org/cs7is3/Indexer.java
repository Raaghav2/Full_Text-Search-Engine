package org.cs7is3;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List; 

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.Parsers.FBISParser;
import org.cs7is3.Parsers.FR94Parser;
import org.cs7is3.Parsers.FTParser;
import org.cs7is3.Parsers.LATimesParser;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

public class Indexer {

    // Force the use of CustomAnalyzer
    public Analyzer analyzer;

    public Indexer(Analyzer ignoredAnalyzer) {

        this.analyzer = new EnglishAnalyzer();
    }

    public void buildIndex(Path docsPath, Path indexPath) throws java.io.IOException {
        Directory directory = FSDirectory.open(Paths.get(indexPath.toString()));
        

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        
        config.setOpenMode(OpenMode.CREATE);
        config.setRAMBufferSizeMB(256.0); 
        
        IndexWriter writer = new IndexWriter(directory, config);

        System.out.println("Starting incremental parsing with CustomAnalyzer (KStem)...");
        long totalDocs = 0;
        
        // --- FBIS ---
        Path fbisPath = docsPath.resolve("fbis");
        System.out.println("Processing FBIS documents...");
        List<Document> fbisDocs = FBISParser.parseFBIS(fbisPath.toString());
        writer.addDocuments(fbisDocs);
        totalDocs += fbisDocs.size();
        System.out.printf("-> Indexed %d FBIS docs.%n", fbisDocs.size());
        fbisDocs.clear(); fbisDocs = null; System.gc(); 

        // --- FR94 ---
        Path fr94Path = docsPath.resolve("fr94");
        System.out.println("Processing FR94 documents...");
        List<Document> fr94Docs = FR94Parser.parseFR94(fr94Path.toString());
        writer.addDocuments(fr94Docs);
        totalDocs += fr94Docs.size();
        System.out.printf("-> Indexed %d FR94 docs.%n", fr94Docs.size());
        fr94Docs.clear(); fr94Docs = null; System.gc(); 

        // --- FT ---
        Path ftPath = docsPath.resolve("ft");
        System.out.println("Processing FT documents...");
        List<Document> ftDocs = FTParser.parseFT(ftPath.toString());
        writer.addDocuments(ftDocs);
        totalDocs += ftDocs.size();
        System.out.printf("-> Indexed %d FT docs.%n", ftDocs.size());
        ftDocs.clear(); ftDocs = null; System.gc(); 

        // --- LA Times ---
        Path latimesPath = docsPath.resolve("latimes");
        System.out.println("Processing LA Times documents...");
        List<Document> latimesDocs = LATimesParser.parseLATimes(latimesPath.toString());
        writer.addDocuments(latimesDocs);
        totalDocs += latimesDocs.size();
        System.out.printf("-> Indexed %d LA Times docs.%n", latimesDocs.size());
        latimesDocs.clear(); latimesDocs = null; System.gc(); 

        System.out.printf("%nTotal documents indexed: %d%n", totalDocs);
        
        System.out.println("Merging segments...");
        writer.forceMerge(1); 
        
        writer.close();
        directory.close();
        System.out.println("Indexing complete. Index created at: " + indexPath);
    }
}