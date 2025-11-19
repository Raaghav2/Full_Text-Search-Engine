package org.cs7is3;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

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

import org.cs7is3.SMJAnalyzer;

public class Indexer {

    public Analyzer analyzer = new SMJAnalyzer();

    // constructor to set analyzer
    public Indexer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void buildIndex(Path docsPath, Path indexPath) throws java.io.IOException {
        // create index writer
        Directory directory = FSDirectory.open(Paths.get(indexPath.toString()));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(directory, config);

        final int COMMIT_INTERVAL = 10000; // commit every 10k documents
        System.out.println("Starting direct multi-corpus parsing...");
        long totalDocs = 0;

        // FBIS
        Path fbisPath = docsPath.resolve("fbis");
        System.out.println("Processing FBIS documents at: " + fbisPath);
        new FBISParser();
        ArrayList<Document> fbisDocs = FBISParser.parseFBIS(fbisPath.toString());
        System.out.printf("-> %d FBIS documents parsed.%n", fbisDocs.size());
        for (Document d : fbisDocs) {
            writer.addDocument(d);
            totalDocs++;
            if (totalDocs % COMMIT_INTERVAL == 0) writer.commit();
        }
        fbisDocs.clear();

        // FR94
        Path fr94Path = docsPath.resolve("fr94");
        System.out.println("Processing FR94 documents at: " + fr94Path);
        new FR94Parser();
        ArrayList<Document> fr94Docs = FR94Parser.parseFR94(fr94Path.toString());
        System.out.printf("-> %d FR94 documents parsed.%n", fr94Docs.size());
        for (Document d : fr94Docs) {
            writer.addDocument(d);
            totalDocs++;
            if (totalDocs % COMMIT_INTERVAL == 0) writer.commit();
        }
        fr94Docs.clear();

        // FT
        Path ftPath = docsPath.resolve("ft");
        System.out.println("Processing FT documents at: " + ftPath);
        new FTParser();
        ArrayList<Document> ftDocs = FTParser.parseFT(ftPath.toString());
        System.out.printf("-> %d FT documents parsed.%n", ftDocs.size());
        for (Document d : ftDocs) {
            writer.addDocument(d);
            totalDocs++;
            if (totalDocs % COMMIT_INTERVAL == 0) writer.commit();
        }
        ftDocs.clear();

        // LA Times
        Path latimesPath = docsPath.resolve("latimes");
        System.out.println("Processing LA Times documents at: " + latimesPath);
        new LATimesParser();
        ArrayList<Document> latimesDocs = LATimesParser.parseLATimes(latimesPath.toString());
        System.out.printf("-> %d LA Times documents parsed.%n", latimesDocs.size());
        for (Document d : latimesDocs) {
            writer.addDocument(d);
            totalDocs++;
            if (totalDocs % COMMIT_INTERVAL == 0) writer.commit();
        }
        latimesDocs.clear();

        System.out.printf("%nTotal documents indexed: %d%n", totalDocs);
        writer.commit();
        writer.close();
        directory.close();
        System.out.println("Indexing complete. Index created at: " + indexPath);
    }
}
