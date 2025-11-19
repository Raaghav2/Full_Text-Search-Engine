package org.cs7is3;
 
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
 
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
public class Indexer {
 
    private static final Pattern DOC_SPLIT = Pattern.compile("(?i)<doc>(.*?)</doc>", Pattern.DOTALL);
    private static final Pattern DOCNO_PAT = Pattern.compile("(?i)<docno>\\s*(.*?)\\s*</docno>");
    private static final Pattern TITLE_PAT = Pattern.compile("(?i)<title>\\s*(.*?)\\s*</title>", Pattern.DOTALL);
    private static final Pattern HEADLINE_PAT = Pattern.compile("(?i)<headline>\\s*(.*?)\\s*</headline>", Pattern.DOTALL);
    private static final Pattern TEXT_PAT = Pattern.compile("(?i)<text>\\s*(.*?)\\s*</text>", Pattern.DOTALL);
    private static final Pattern DATE_PAT = Pattern.compile("(?i)<date>\\s*(.*?)\\s*</date>", Pattern.DOTALL);
    private static final Pattern SOURCE_PAT = Pattern.compile("(?i)<source>\\s*(.*?)\\s*</source>", Pattern.DOTALL);
    private static final Pattern PUB_PAT = Pattern.compile("(?i)<pub>\\s*(.*?)\\s*</pub>", Pattern.DOTALL);
 
    public void buildIndex(Path docsPath, Path indexPath) throws IOException {
        if (!Files.exists(docsPath)) {
            throw new IOException("Documents path does not exist: " + docsPath.toAbsolutePath());
        }
        if (!Files.exists(indexPath)) {
            Files.createDirectories(indexPath);
        }
 
        Analyzer analyzer = new EnglishAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
 
        try (Directory dir = FSDirectory.open(indexPath);
             IndexWriter writer = new IndexWriter(dir, iwc)) {
 
            Files.walk(docsPath)
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        try {
                            String raw = Files.readString(file, StandardCharsets.UTF_8);
                            boolean foundDocTags = false;
 
                            Matcher docMatcher = DOC_SPLIT.matcher(raw);
                            while (docMatcher.find()) {
                                foundDocTags = true;
                                String docBlock = docMatcher.group(1);
                                indexDocBlock(docBlock, writer);
                            }
 
                            if (!foundDocTags) {
                                indexDocBlock(raw, writer);
                            }
                        } catch (Exception e) {
                            System.err.println("Skipping file due to error: " + file + " -> " + e.getMessage());
                        }
                    });
 
            writer.commit();
            System.out.println("Indexed successfully. Total docs: " + writer.getDocStats().numDocs);
        }
    }
 
    private void indexDocBlock(String block, IndexWriter writer) {
        try {
            String docno = extractFirst(DOCNO_PAT, block);
            String title = extractFirst(TITLE_PAT, block);
            if (title == null || title.isBlank()) {
                title = extractFirst(HEADLINE_PAT, block);
            }
            String text = extractFirst(TEXT_PAT, block);
            String date = extractFirst(DATE_PAT, block);
            String source = extractFirst(SOURCE_PAT, block);
            if (source == null || source.isBlank()) {
                source = extractFirst(PUB_PAT, block);
            }
 
            if (docno == null || docno.isBlank()) {
                docno = "DOC_" + Math.abs(block.hashCode());
            }
 
            if (text == null || text.isBlank()) {
                text = block;
            }
 
            Document doc = new Document();
            doc.add(new StringField("docno", docno, Field.Store.YES));
            if (title != null && !title.isBlank()) doc.add(new TextField("title", title, Field.Store.YES));
            doc.add(new TextField("text", text, Field.Store.YES));
            if (date != null && !date.isBlank()) doc.add(new StringField("date", date, Field.Store.YES));
            if (source != null && !source.isBlank()) doc.add(new StringField("source", source, Field.Store.YES));
 
            doc.add(new IntPoint("body_length", text.length()));
            doc.add(new StoredField("body_length", text.length()));
 
            writer.addDocument(doc);
        } catch (Exception e) {
            System.err.println("Failed to index doc block: " + e.getMessage());
        }
    }
 
    private String extractFirst(Pattern p, String s) {
        if (s == null) return null;
        Matcher m = p.matcher(s);
        if (m.find()) return m.group(1).trim();
        return null;
    }
}