package org.cs7is3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
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
    public String docsPath = "";
    public String indexPath = "";
    public Analyzer analyzer;

    public void buildIndex() throws java.io.IOException {
        String targetPath = indexPath + "/" + analyzer.getClass().getSimpleName();
        Directory directory = FSDirectory.open(Paths.get(targetPath));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        OpenMode mode = OpenMode.CREATE;
        config.setOpenMode(mode);
        IndexWriter writer = new IndexWriter(directory, config);

        FileInputStream file = new FileInputStream(docsPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(file));

        String currentLine = "";
        String currentField = "";
        StringBuilder currentContent = new StringBuilder();
        ArrayList<Document> documents = new ArrayList<>();

        Document doc = new Document();

        currentLine = reader.readLine();
        while (currentLine != null) {
            if(currentLine.startsWith(".I")){
                if(currentField != ""){
                    doc.add(new TextField(currentField, currentContent.toString().trim(), TextField.Store.YES));
                    documents.add(doc);
                    currentContent.setLength(0);
                }
                doc = new Document();
                currentField = "id";
                doc.add(new TextField(currentField, currentLine.substring(3).trim(), TextField.Store.YES));
            }else if(currentLine.startsWith(".T")){
                currentField = "title";
            }else if(currentLine.startsWith(".A")){
                if(currentField != "author"){
                    doc.add(new TextField(currentField, currentContent.toString().trim(), TextField.Store.YES));
                    currentContent.setLength(0);
                }
                currentField = "author";
            }else if(currentLine.startsWith(".B")){
                if(currentField != "bibliography"){
                    doc.add(new TextField(currentField, currentContent.toString().trim(), TextField.Store.YES));
                    currentContent.setLength(0);
                }
                currentField = "bibliography";
            }else if(currentLine.startsWith(".W")){
                if(currentField != "content"){
                    doc.add(new TextField(currentField, currentContent.toString().trim(), TextField.Store.YES));
                    currentContent.setLength(0);
                }
                currentField = "content";
            }else{
                currentContent.append(currentLine).append(" ");
            }
            currentLine = reader.readLine();
        }
        documents.add(doc);

        writer.addDocuments(documents);

        reader.close();
        writer.close();
        file.close();
        directory.close();
        System.out.printf("Indexing completed. Indeexed %d documents.\n", documents.size());
    }
}
