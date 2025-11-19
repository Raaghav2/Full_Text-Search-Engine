package org.cs7is3;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

    public void buildIndex(Path docsPath, Path indexPath) throws IOException {
        Analyzer analyzer = new EnglishAnalyzer(); 

        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        try (IndexWriter writer = new IndexWriter(dir, config)) {
            Files.walk(docsPath)
                    .filter(Files::isRegularFile)
                    .forEach(path -> {
                        try {
                            List<Document> docs = parseDocumentsFromFile(path);
                            for (Document doc : docs) {
                                writer.addDocument(doc);
                            }
                        } catch (IOException e) {
                            System.err.println("Failed to parse file: " + path);
                            e.printStackTrace();
                        }
                    });

            writer.commit();
        }
    }


    private List<Document> parseDocumentsFromFile(Path file) throws IOException {
        List<Document> docs = new ArrayList<>();
        List<String> lines = Files.readAllLines(file, StandardCharsets.ISO_8859_1);

        StringBuilder current = new StringBuilder();
        boolean inDoc = false;

        for (String line : lines) {
            String trim = line.trim();
            if (trim.equalsIgnoreCase("<DOC>")) {
                inDoc = true;
                current.setLength(0);
            } else if (trim.equalsIgnoreCase("</DOC>")) {
                inDoc = false;
                Document doc = buildDocumentFromRaw(current.toString());
                if (doc != null) {
                    docs.add(doc);
                }
            } else if (inDoc) {
                current.append(line).append('\n');
            }
        }

        return docs;
    }


    private Document buildDocumentFromRaw(String raw) {
        String docno = extractTag(raw, "DOCNO");
        String title = extractTag(raw, "TITLE");
        String text  = extractTag(raw, "TEXT");
        // String date  = extractTag(raw, "DATE");
        // String source = extractTag(raw, "SOURCE");

        if (docno == null || docno.isEmpty()) {
            return null;
        }

        Document doc = new Document();

        doc.add(new StringField("docno", docno, Field.Store.YES));

        if (title != null && !title.isEmpty()) {
            doc.add(new TextField("title", title, Field.Store.YES));
        }
        if (text != null && !text.isEmpty()) {
            doc.add(new TextField("text", text, Field.Store.YES));
        }


        StringBuilder contents = new StringBuilder();
        if (title != null) contents.append(title).append(' ');
        if (text != null) contents.append(text);

        doc.add(new TextField("contents", contents.toString(), Field.Store.NO));

        return doc;
    }


    private String extractTag(String raw, String tag) {
        String open = "<" + tag + ">";
        String close = "</" + tag + ">";
        int start = raw.indexOf(open);
        int end = raw.indexOf(close);
        if (start >= 0 && end > start) {
            return raw.substring(start + open.length(), end).trim();
        }
        return null;
    }
}
