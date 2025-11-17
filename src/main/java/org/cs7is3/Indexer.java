package org.cs7is3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.Parsers.FBISParser;
import org.cs7is3.Parsers.FR94Parser;
import org.cs7is3.Parsers.FTParser;
import org.cs7is3.Parsers.LATimesParser;

// TODO: Implement your Lucene indexer
// This class should build a Lucene index from the document collection
//
// Requirements:
// 1. Parse documents from the "Assignment Two" dataset
// 2. Extract relevant fields (DOCNO, TITLE, TEXT, etc.)
// 3. Create a Lucene index with appropriate analyzers
// 4. Handle document parsing errors gracefully
//
// The GitHub Actions workflow will call:
//   indexer.buildIndex(Path docsPath, Path indexPath)

public class Indexer {

    //default analyzer in case none is provided
    public Analyzer analyzer = new EnglishAnalyzer();

    //constructor to set analyzer
    public Indexer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void buildIndex(Path docsPath, Path indexPath) throws java.io.IOException {;
        //create index writer
        Directory directory = FSDirectory.open(Paths.get(indexPath.toString()));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        OpenMode mode = OpenMode.CREATE;
        config.setOpenMode(mode);
        IndexWriter writer = new IndexWriter(directory, config);

        ArrayList<Document> documentsToIndex = new ArrayList<>();
        
        System.out.println("Starting direct multi-corpus parsing...");
        long totalDocs = 0;
        
        Path fbisPath = docsPath.resolve("fbis");
        System.out.println("Processing FBIS documents at: " + fbisPath);
        new FBISParser();
        ArrayList<Document> fbisDocs = FBISParser.parseFBIS(fbisPath.toString()); 
        documentsToIndex.addAll(fbisDocs);
        totalDocs += fbisDocs.size();
        System.out.printf("-> %d FBIS documents parsed.%n", fbisDocs.size());

        Path fr94Path = docsPath.resolve("fr94");
        System.out.println("Processing FR94 documents at: " + fr94Path);
        new FR94Parser();
        ArrayList<Document> fr94Docs = FR94Parser.parseFR94(fr94Path.toString()); 
        documentsToIndex.addAll(fr94Docs);
        totalDocs += fr94Docs.size();
        System.out.printf("-> %d FR94 documents parsed.%n", fr94Docs.size());

        Path ftPath = docsPath.resolve("ft");
        System.out.println("Processing FT documents at: " + ftPath);
        new FTParser();
        ArrayList<Document> ftDocs = FTParser.parseFT(ftPath.toString()); 
        documentsToIndex.addAll(ftDocs);
        totalDocs += ftDocs.size();
        System.out.printf("-> %d FT documents parsed.%n", ftDocs.size());

        Path latimesPath = docsPath.resolve("latimes");
        System.out.println("Processing LA Times documents at: " + latimesPath);
        new LATimesParser();
        ArrayList<Document> latimesDocs = LATimesParser.parseLATimes(latimesPath.toString()); 
        documentsToIndex.addAll(latimesDocs);
        totalDocs += latimesDocs.size();
        System.out.printf("-> %d LA Times documents parsed.%n", latimesDocs.size());

        System.out.printf("%nTotal documents to index: %d%n", totalDocs);
        writer.addDocuments(documentsToIndex);

        writer.close();
        directory.close();
        System.out.println("Indexing complete. Index created at: " + indexPath);


    }
}
