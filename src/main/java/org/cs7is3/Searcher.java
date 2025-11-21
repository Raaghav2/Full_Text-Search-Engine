package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

public class Searcher {

    private final Analyzer analyzer = new CustomAnalyzer(); 
    private static final String RUN_TAG = "CS7IS3_Simple_KStem_Dirichlet";

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);

        // ENGINE: LM Dirichlet (Mu=2000)
        // Standard setting. Smooths probabilities based on document length.
        searcher.setSimilarity(new LMDirichletSimilarity(2000));

        // Parser targeting the TEXT field
        // Dirichlet works best on the main body of text.
        QueryParser textParser = new QueryParser("TEXT", analyzer);
        textParser.setSplitOnWhitespace(true);

        TopicParser topicParser = new TopicParser();
        List<Topic> topics = topicParser.parse(topicsPath);
        
        if (outputRun.getParent() != null) outputRun.getParent().toFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                
                BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
                
                try {
                    // =========================================================
                    // STRATEGY: Flat Probabilistic Query
                    // =========================================================
                    // We combine all parts into a single "Topic Model".
                    // We trust KStem to match the concepts.
                    // We trust Dirichlet to handle the weighting.

                    // 1. Title
                    if (topic.title != null) {
                        Query q = textParser.parse(QueryParser.escape(topic.title));
                        queryBuilder.add(q, BooleanClause.Occur.SHOULD);
                    }

                    // 2. Description
                    if (topic.description != null) {
                        Query q = textParser.parse(QueryParser.escape(topic.description));
                        queryBuilder.add(q, BooleanClause.Occur.SHOULD);
                    }

                    // 3. Narrative (Filtered)
                    // We filter "not relevant" to avoid polluting the probability model.
                    if (topic.narrative != null) {
                        String cleanNarr = filterNegativeNarrative(topic.narrative);
                        if (!cleanNarr.trim().isEmpty()) {
                            Query q = textParser.parse(QueryParser.escape(cleanNarr));
                            queryBuilder.add(q, BooleanClause.Occur.SHOULD);
                        }
                    }
                    
                    // EXECUTE
                    ScoreDoc[] hits = searcher.search(queryBuilder.build(), numDocs).scoreDocs;

                    for (int rank = 0; rank < hits.length; rank++) {
                        ScoreDoc hit = hits[rank];
                        Document doc = searcher.doc(hit.doc);
                        String docNo = doc.get("DOCNO");
                        if (docNo == null) continue; 
                        
                        writer.println(String.format("%s Q0 %s %d %.4f %s", 
                            topic.number, docNo, rank + 1, hit.score, RUN_TAG));
                    }
                    writer.flush(); 

                } catch (Exception e) {
                    System.err.println("Error on topic " + topic.number);
                }
            }
            System.out.println("Simple Dirichlet Search Complete.");
            
        } finally {
            reader.close(); 
        }
    }

    private String filterNegativeNarrative(String narrative) {
        StringBuilder sb = new StringBuilder();
        for (String s : narrative.split("[\\.\\;\\n]")) {
            String lower = s.toLowerCase();
            if (!lower.contains("not relevant") && 
                !lower.contains("irrelevant") && 
                !lower.contains("unless")) {
                sb.append(s).append(" ");
            }
        }
        return sb.toString();
    }
}