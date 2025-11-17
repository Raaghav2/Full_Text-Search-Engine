package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

// TODO: Implement your Lucene searcher
// This class should search topics against the index and produce TREC-format results
//
// Requirements:
// 1. Parse topics from the topics file
// 2. Generate queries from topic information (title, description, narrative)
// 3. Execute searches against the Lucene index
// 4. Write TREC-format results: "topic_id Q0 docno rank score run_tag"
// 5. Output exactly 1000 results per topic
//
// The GitHub Actions workflow will call:
//   searcher.searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs)

public class Searcher {
    private final Analyzer analyzer = new EnglishAnalyzer(); 
    private static final String SEARCH_FIELD = "TEXT"; 
    private static final String RUN_TAG = "CS7IS3_BM25_TD"; 
    

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new BM25Similarity()); 
        TopicParser topicParser = new TopicParser();
        List<Topic> topics = topicParser.parse(topicsPath);
        QueryParser queryParser = new QueryParser(SEARCH_FIELD, analyzer);
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
                        double score = hit.score;
                        String trecLine = String.format(
                            "%s Q0 %s %d %.4f %s", 
                            topic.number,      
                            docNo,              
                            rank + 1,            
                            score,               
                            RUN_TAG             
                        );
                        writer.println(trecLine);
                    }
                    
                } catch (org.apache.lucene.queryparser.classic.ParseException e) {
                    System.err.println("Error parsing query for topic " + topic.number + ": " + e.getMessage());
                }
            }
            
            System.out.println("Finished searching. Results saved to: " + outputRun.toAbsolutePath());
            
        } finally {
            reader.close(); // Close the index reader
        }
    }
 
    private String generateQueryString(Topic topic) {
        String titlePart = topic.title;
        String descPart = topic.description;
        return titlePart + " " + descPart;
    }
}
