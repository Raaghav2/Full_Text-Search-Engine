package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

public class Searcher {

    // MUST match the Analyzer used in Indexer.java
    private final Analyzer analyzer = new CustomAnalyzer(); 
    
    private static final String RUN_TAG = "CS7IS3_Bare_BM25";
    
    // We search both fields, but treat them equally
    private static final String[] SEARCH_FIELDS = {"TITLE", "TEXT"};

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);

        // ===========================================================================
        // 1. SIMILARITY SETUP: Pure BM25
        // ===========================================================================
        // No fancy wrappers. Just the industry standard probabilistic model.
        searcher.setSimilarity(new BM25Similarity());

        // ===========================================================================
        // 2. PARSER SETUP: Standard Multi-Field
        // ===========================================================================
        // No phrase slop, no strictness. Just finds words in either field.
        MultiFieldQueryParser queryParser = new MultiFieldQueryParser(SEARCH_FIELDS, analyzer);

        TopicParser topicParser = new TopicParser();
        List<Topic> topics = topicParser.parse(topicsPath);
        
        int totalResultsWritten = 0; 
        
        if (outputRun.getParent() != null) {
            outputRun.getParent().toFile().mkdirs();
        }

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                
                try {

                    String cleanNarrative = filterNegativeNarrative(topic.narrative);
                    
                    String queryString = topic.title + " " + topic.description + " " + cleanNarrative;
                    
                    Query query = queryParser.parse(QueryParser.escape(queryString));
                    
                    // ===================================================================
                    // 4. EXECUTE SEARCH
                    // ===================================================================
                    ScoreDoc[] hits = searcher.search(query, numDocs).scoreDocs;

                    for (int rank = 0; rank < hits.length; rank++) {
                        ScoreDoc hit = hits[rank];
                        Document doc = searcher.doc(hit.doc);
                        
                        String docNo = doc.get("DOCNO");
                        if (docNo == null || docNo.isEmpty()) continue; 
                        
                        String trecLine = String.format(
                            "%s Q0 %s %d %.4f %s", 
                            topic.number, docNo, rank + 1, hit.score, RUN_TAG
                        );
                        writer.println(trecLine);
                        totalResultsWritten++;
                    }
                    writer.flush(); 

                } catch (Exception e) {
                    System.err.println("Error on topic " + topic.number + ": " + e.getMessage());
                }
            }
            System.out.println("Finished searching. Wrote " + totalResultsWritten + " results.");
            System.out.println("Results saved to: " + outputRun.toAbsolutePath());
            
        } finally {
            reader.close(); 
        }
    }

    private String filterNegativeNarrative(String narrative) {
        if (narrative == null) return "";
        StringBuilder cleanText = new StringBuilder();
        String[] sentences = narrative.split("[\\.\\;\\n]");
        for (String sentence : sentences) {
            String lower = sentence.toLowerCase();
            if (lower.contains("not relevant") || 
                lower.contains("irrelevant") || 
                lower.contains("unless")) {
                continue; 
            }
            cleanText.append(sentence).append(" ");
        }
        return cleanText.toString();
    }
}