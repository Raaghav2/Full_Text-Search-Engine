package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashMap; 
import java.util.List;
import java.util.Map;     

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;

// Imports for Advanced Querying
import org.apache.lucene.queryparser.classic.QueryParser; 
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

// Imports for Mixed Similarity
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.DistributionSPL; 
import org.apache.lucene.search.similarities.LambdaDF;        
import org.apache.lucene.search.similarities.NormalizationH2; 

public class Searcher {

    // Use our new CustomAnalyzer
    private final Analyzer analyzer = new CustomAnalyzer(); 
    private static final String RUN_TAG = "CS7IS3_Ultimate_Boosted_Phrase_KStem";

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);

        // ===========================================================================
        // 1. MIXED SIMILARITY SETUP
        // ===========================================================================
        final Map<String, Similarity> fieldSims = new HashMap<>();
        fieldSims.put("TITLE", new IBSimilarity(new DistributionSPL(), new LambdaDF(), new NormalizationH2()));
        fieldSims.put("TEXT", new BM25Similarity());
        
        final Similarity defaultSim = new BM25Similarity();
        
        Similarity mixedSimilarity = new PerFieldSimilarityWrapper() {
            @Override
            public Similarity get(String fieldName) {
                return fieldSims.getOrDefault(fieldName, defaultSim);
            }
        };
        searcher.setSimilarity(mixedSimilarity); 

        // ===========================================================================
        // 2. QUERY PARSER SETUP (Fix Applied Here)
        // ===========================================================================
        
        // Title Parser
        QueryParser titleParser = new QueryParser("TITLE", analyzer);
        titleParser.setSplitOnWhitespace(true); // <-- REQUIRED FIX for Lucene 9.x
        titleParser.setAutoGeneratePhraseQueries(true); 
        titleParser.setPhraseSlop(2); 

        // Text Parser
        QueryParser textParser = new QueryParser("TEXT", analyzer);
        textParser.setSplitOnWhitespace(true); // <-- REQUIRED FIX for Lucene 9.x
        textParser.setAutoGeneratePhraseQueries(true); 
        textParser.setPhraseSlop(8); 

        TopicParser topicParser = new TopicParser();
        List<Topic> topics = topicParser.parse(topicsPath);
        
        int totalResultsWritten = 0; 
        
        // --- Ensure output directory exists ---
        if (outputRun.getParent() != null) {
            outputRun.getParent().toFile().mkdirs();
        }

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                
                // ===========================================================================
                // 3. BOOSTED QUERY CONSTRUCTION
                // ===========================================================================
                BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
                
                try {
                    // --- Title: Boost 3.5 ---
                    if (topic.title != null && !topic.title.isEmpty()) {
                        Query titleQuery = titleParser.parse(QueryParser.escape(topic.title));
                        queryBuilder.add(new BoostQuery(titleQuery, 3.5f), BooleanClause.Occur.SHOULD);
                    }

                    // --- Description: Boost 1.7 ---
                    if (topic.description != null && !topic.description.isEmpty()) {
                        Query descQuery = textParser.parse(QueryParser.escape(topic.description));
                        queryBuilder.add(new BoostQuery(descQuery, 1.7f), BooleanClause.Occur.SHOULD);
                    }

                    // --- Narrative: Filtered, Boost 1.0 ---
                    if (topic.narrative != null && !topic.narrative.isEmpty()) {
                        String cleanNarrative = filterNegativeNarrative(topic.narrative);
                        
                        if (!cleanNarrative.trim().isEmpty()) {
                            Query narrQuery = textParser.parse(QueryParser.escape(cleanNarrative));
                            queryBuilder.add(narrQuery, BooleanClause.Occur.SHOULD);
                        }
                    }
                    
                    BooleanQuery finalQuery = queryBuilder.build();
                    
                    // ===========================================================================
                    // 4. EXECUTE SEARCH
                    // ===========================================================================
                    ScoreDoc[] hits = searcher.search(finalQuery, numDocs).scoreDocs;

                    for (int rank = 0; rank < hits.length; rank++) {
                        ScoreDoc hit = hits[rank];
                        Document doc = searcher.doc(hit.doc);
                        
                        String docNo = doc.get("DOCNO");
                        if (docNo == null || docNo.isEmpty()) {
                            continue; 
                        }
                        
                        String trecLine = String.format(
                            "%s Q0 %s %d %.4f %s", 
                            topic.number, docNo, rank + 1, hit.score, RUN_TAG
                        );
                        writer.println(trecLine);
                        totalResultsWritten++;
                    }
                    writer.flush(); 

                } catch (org.apache.lucene.queryparser.classic.ParseException e) {
                    System.err.println("Error parsing query for topic " + topic.number + ": " + e.getMessage());
                }
            }
            System.out.println("Finished searching. Wrote " + totalResultsWritten + " results.");
            System.out.println("Results saved to: " + outputRun.toAbsolutePath());
            
        } finally {
            reader.close(); 
        }
    }

    private String filterNegativeNarrative(String narrative) {
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