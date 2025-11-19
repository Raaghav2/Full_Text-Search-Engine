package org.cs7is3;
 
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
 
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
public class Searcher {
 
    private static final Pattern TOP_SPLIT = Pattern.compile("(?i)<top>(.*?)</top>", Pattern.DOTALL);
 
    private static final Pattern NUM_CLOSING = Pattern.compile("(?i)<num>\\s*(?:Number:)?\\s*(\\d+)\\s*</num>", Pattern.DOTALL);
    private static final Pattern NUM_UPTO_NEXT = Pattern.compile("(?i)<num>\\s*(?:Number:)?\\s*(\\d+)", Pattern.DOTALL);
    private static final Pattern NUM_INLINE = Pattern.compile("(?i)Number:\\s*(\\d+)", Pattern.DOTALL);
 
    private static final Pattern TITLE_CLOSING = Pattern.compile("(?i)<title>\\s*(.*?)\\s*</title>", Pattern.DOTALL);
    private static final Pattern DESC_CLOSING = Pattern.compile("(?i)<desc>\\s*(.*?)\\s*</desc>", Pattern.DOTALL);
    private static final Pattern NARR_CLOSING = Pattern.compile("(?i)<narr>\\s*(.*?)\\s*</narr>", Pattern.DOTALL);
 
    private static final Pattern TITLE_UPTO_NEXT = Pattern.compile("(?i)<title>\\s*(.*?)\\s*(?=<desc>|<narr>|</top>)", Pattern.DOTALL);
    private static final Pattern DESC_UPTO_NEXT = Pattern.compile("(?i)<desc>\\s*(.*?)\\s*(?=<narr>|</top>)", Pattern.DOTALL);
    private static final Pattern NARR_UPTO_NEXT = Pattern.compile("(?i)<narr>\\s*(.*?)\\s*(?=</top>)", Pattern.DOTALL);
 
    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        if (!Files.exists(indexPath)) throw new IOException("Index path not found: " + indexPath);
        if (!Files.exists(topicsPath)) throw new IOException("Topics file not found: " + topicsPath);
 
        Analyzer analyzer = new EnglishAnalyzer();
 
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(new BM25Similarity());
 
            String topicsRaw = Files.readString(topicsPath, StandardCharsets.UTF_8);
            List<Topic> topics = parseTopics(topicsRaw);
 
            System.out.println("Parsed " + topics.size() + " topics.");
 
            // Ensure parent directory exists (e.g., runs/)
            Path parent = outputRun.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
 
            try (BufferedWriter writer = Files.newBufferedWriter(outputRun, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
 
                MultiFieldQueryParser parser = new MultiFieldQueryParser(
                        new String[]{"title", "text"},
                        analyzer,
                        Map.of("title", 2.0f, "text", 1.0f)
                );
 
                for (Topic t : topics) {
                    String queryText = (t.title + " " + t.desc + " " + t.narr).trim();
                    if (queryText.isBlank()) {
                        System.err.println("Topic " + t.id + " produced empty query; skipping.");
                        continue;
                    }
 
                    Query q;
                    try {
                        q = parser.parse(MultiFieldQueryParser.escape(queryText));
                    } catch (Exception e) {
                        q = new TermQuery(new org.apache.lucene.index.Term("text", queryText));
                    }
 
                    TopDocs topDocs = searcher.search(q, numDocs);
                    ScoreDoc[] hits = topDocs.scoreDocs;
 
                    List<Integer> returnedDocIds = new ArrayList<>();
                    for (ScoreDoc sd : hits) returnedDocIds.add(sd.doc);
 
                    if (hits.length < numDocs) {
                        TopDocs all = searcher.search(new MatchAllDocsQuery(), numDocs);
                        List<ScoreDoc> padding = new ArrayList<>();
                        for (ScoreDoc sd : all.scoreDocs) {
                            if (returnedDocIds.contains(sd.doc)) continue;
                            padding.add(sd);
                            if (hits.length + padding.size() >= numDocs) break;
                        }
                        ScoreDoc[] combined = new ScoreDoc[hits.length + padding.size()];
                        System.arraycopy(hits, 0, combined, 0, hits.length);
                        for (int i = 0; i < padding.size(); i++) combined[hits.length + i] = padding.get(i);
                        hits = combined;
                    }
 
                    int toWrite = Math.min(numDocs, hits.length);
                    for (int rank = 0; rank < toWrite; rank++) {
                        ScoreDoc sd = hits[rank];
                        Document d = searcher.doc(sd.doc);
                        String docno = d.get("docno");
                        float score = sd.score;
                        String runTag = "cs7is3";
                        String line = String.format("%s Q0 %s %d %.6f %s", t.id, docno, rank + 1, score, runTag);
                        writer.write(line);
                        writer.newLine();
                    }
 
                    if (hits.length < numDocs) {
                        String fallbackDocno = hits.length > 0 ? searcher.doc(hits[hits.length - 1].doc).get("docno") : "DUMMY";
                        for (int rank = hits.length; rank < numDocs; rank++) {
                            String line = String.format("%s Q0 %s %d %.6f %s", t.id, fallbackDocno, rank + 1, 0.0, "cs7is3");
                            writer.write(line);
                            writer.newLine();
                        }
                    }
                }
                writer.flush();
            }
        }
    }
 
    private List<Topic> parseTopics(String raw) {
        List<Topic> out = new ArrayList<>();
        Matcher topMatcher = TOP_SPLIT.matcher(raw);
        while (topMatcher.find()) {
            String block = topMatcher.group(1);
 
            String id = extractFirst(NUM_CLOSING, block);
            if (id == null) id = extractFirst(NUM_UPTO_NEXT, block);
            if (id == null) id = extractFirst(NUM_INLINE, block);
            if (id == null) {
                Matcher m = Pattern.compile("\\b(\\d{3,6})\\b").matcher(block);
                if (m.find()) id = m.group(1);
            }
 
            String title = extractFirst(TITLE_CLOSING, block);
            if (isBlank(title)) title = extractFirst(TITLE_UPTO_NEXT, block);
 
            String desc = extractFirst(DESC_CLOSING, block);
            if (isBlank(desc)) desc = extractFirst(DESC_UPTO_NEXT, block);
 
            String narr = extractFirst(NARR_CLOSING, block);
            if (isBlank(narr)) narr = extractFirst(NARR_UPTO_NEXT, block);
 
            if (id == null) {
                System.err.println("Skipping topic block with no id. Block snippet: " + (block.length() > 80 ? block.substring(0, 80) + "..." : block));
                continue;
            }
 
            out.add(new Topic(id, safe(title), safe(desc), safe(narr)));
        }
        return out;
    }
 
    private String extractFirst(Pattern p, String s) {
        if (s == null) return null;
        Matcher m = p.matcher(s);
        if (m.find()) return m.group(1).trim();
        return null;
    }
 
    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
 
    private String safe(String s) {
        return s == null ? "" : s;
    }
 
    private static class Topic {
        String id;
        String title;
        String desc;
        String narr;
 
        Topic(String id, String title, String desc, String narr) {
            this.id = id;
            this.title = title;
            this.desc = desc;
            this.narr = narr;
        }
    }
}