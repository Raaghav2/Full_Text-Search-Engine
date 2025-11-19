package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashMap; 
import java.util.List;
import java.util.Map; 

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser; 
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.DistributionSPL; 
import org.apache.lucene.search.similarities.LambdaDF; 
import org.apache.lucene.search.similarities.NormalizationH2; 

public class Searcher {

    private final Analyzer analyzer = new EnglishAnalyzer(); 
    private static final String[] SEARCH_FIELDS = {"TITLE", "TEXT"};
    private static final String RUN_TAG = "CS7IS3_Mixed_IB_BM25";

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);

        final Map<String, Similarity> fieldSims = new HashMap<>();
        
        fieldSims.put("TITLE", new IBSimilarity(new DistributionSPL(), new LambdaDF(), new NormalizationH2()));
        
        fieldSims.put("TEXT", new BM25Similarity());

        final Similarity defaultSim = new BM25Similarity();

        Similarity mixedSimilarity = new PerFieldSimilarityWrapper() {
            @Override
            public Similarity get(String fieldName) {
                if (fieldSims.containsKey(fieldName)) {
                    return fieldSims.get(fieldName);
                } else {
                    return defaultSim;
                }
            }
        };

        searcher.setSimilarity(mixedSimilarity); 
        
        TopicParser topicParser = new TopicParser();
        List<Topic> topics = topicParser.parse(topicsPath);
        
        MultiFieldQueryParser queryParser = new MultiFieldQueryParser(SEARCH_FIELDS, analyzer);
        
        int totalResultsWritten = 0; 
        
        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                String queryString = generateQueryString(topic);
                
                try {
                    Query query = queryParser.parse(QueryParser.escape(queryString));
                    ScoreDoc[] hits = searcher.search(query, numDocs).scoreDocs;

                    for (int rank = 0; rank < hits.length; rank++) {
                        ScoreDoc hit = hits[rank];
                        Document doc = searcher.doc(hit.doc);
                        
                        String docNo = doc.get("DOCNO");
                        
                        if (docNo == null || docNo.isEmpty()) {
                            System.err.println("WARNING: Found a document with a missing DOCNO. Lucene DocID: " + hit.doc);
                            continue; 
                        }
                        
                        double score = hit.score;
                        String trecLine = String.format(
                            "%s Q0 %s %d %.4f %s", 
                            topic.number, docNo, rank + 1, score, RUN_TAG
                        );
                        writer.println(trecLine);
                        totalResultsWritten++;
                    }
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

    private String generateQueryString(Topic topic) {
        return topic.title + " " + topic.description + " " + topic.narrative;
    }
}