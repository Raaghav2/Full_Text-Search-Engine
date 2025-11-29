package org.cs7is3;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List; 
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer; // STANDARD ANALYZER
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

public class Indexer {

    public Analyzer analyzer = new EnglishAnalyzer();

    public Indexer(Analyzer ignored) {
        this.analyzer = new EnglishAnalyzer();
    }

    public void buildIndex(Path docsPath, Path indexPath) throws java.io.IOException {
        Directory directory = FSDirectory.open(Paths.get(indexPath.toString()));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        
        config.setOpenMode(OpenMode.CREATE);
        
        // --- KEY SPEED OPTIMIZATION ---
        // 1. Prioritize large RAM buffer for indexing speed (512MB)
        config.setRAMBufferSizeMB(512.0); 
        // 2. We allow Lucene to decide when to flush automatically
        // ------------------------------

        IndexWriter writer = new IndexWriter(directory, config);

        System.out.println("Indexing with EnglishAnalyzer (HIGH SPEED MODE)...");
        
        // We call indexCorpus sequentially, allowing Lucene to flush only when the 
        // 512MB buffer is full, not after every single newspaper corpus.
        indexCorpus(writer, docsPath.resolve("fbis"), "FBIS");
        indexCorpus(writer, docsPath.resolve("fr94"), "FR94");
        indexCorpus(writer, docsPath.resolve("ft"), "FT");
        indexCorpus(writer, docsPath.resolve("latimes"), "LATimes");

        // NOTE: We skip the final slow writer.forceMerge(1) to save time.
        
        writer.close(); // Flushes final segment and closes
        directory.close();
        System.out.println("Indexing complete.");
    }

    private void indexCorpus(IndexWriter writer, Path path, String name) throws java.io.IOException {
        System.out.println("Processing " + name + "...");
        List<Document> docs;
        // The code already indexes sequentially by newspaper, and clears memory afterward.
        if (name.equals("FBIS")) docs = FBISParser.parseFBIS(path.toString());
        else if (name.equals("FR94")) docs = FR94Parser.parseFR94(path.toString());
        else if (name.equals("FT")) docs = FTParser.parseFT(path.toString());
        else docs = LATimesParser.parseLATimes(path.toString());
        
        // ADD DOCUMENTS ONE BY ONE - Allows Lucene to decide when to flush
        for (Document doc : docs) writer.addDocument(doc);
        
        // We REMOVE writer.commit() to rely on the fast RAM buffer.
        docs.clear(); docs = null; System.gc();
    }
}