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

public class Indexer {

    public Analyzer analyzer = new CustomAnalyzer();

    public Indexer(Analyzer ignored) {
        this.analyzer = new CustomAnalyzer();
    }

    public void buildIndex(Path docsPath, Path indexPath) throws java.io.IOException {
        Directory directory = FSDirectory.open(Paths.get(indexPath.toString()));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE);
        config.setMaxBufferedDocs(2000); 
        
        IndexWriter writer = new IndexWriter(directory, config);

        System.out.println("Indexing with KStem...");
        
        indexCorpus(writer, docsPath.resolve("fbis"), "FBIS");
        indexCorpus(writer, docsPath.resolve("fr94"), "FR94");
        indexCorpus(writer, docsPath.resolve("ft"), "FT");
        indexCorpus(writer, docsPath.resolve("latimes"), "LATimes");

        writer.forceMerge(1); 
        writer.close();
        directory.close();
    }

    private void indexCorpus(IndexWriter writer, Path path, String name) throws java.io.IOException {
        System.out.println("Processing " + name + "...");
        List<Document> docs;
        if (name.equals("FBIS")) docs = FBISParser.parseFBIS(path.toString());
        else if (name.equals("FR94")) docs = FR94Parser.parseFR94(path.toString());
        else if (name.equals("FT")) docs = FTParser.parseFT(path.toString());
        else docs = LATimesParser.parseLATimes(path.toString());
        
        for (Document doc : docs) writer.addDocument(doc);
        writer.commit();
        docs.clear(); docs = null; System.gc();
    }
}