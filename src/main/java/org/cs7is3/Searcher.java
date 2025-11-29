package org.cs7is3;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queries.mlt.MoreLikeThis;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Lucene searcher for CS7IS3 Assignment 2
 * Parses topics and generates queries automatically
 */
public class Searcher {
    
    private static final Pattern TOPIC_PATTERN = Pattern.compile(
        "<top>.*?<num>\\s*Number:\\s*(\\d+).*?<title>\\s*(.*?)\\s*<desc>.*?Description:\\s*(.*?)\\s*<narr>.*?Narrative:\\s*(.*?)\\s*</top>", 
        Pattern.DOTALL
    );
    
    private static final String RUN_TAG = "cs7is3-group8";
    
    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) 
            throws IOException {
        
        System.out.println("Starting topic search...");
        System.out.println("Index: " + indexPath);
        System.out.println("Topics: " + topicsPath);
        System.out.println("Output: " + outputRun);
        System.out.println("Number of docs per topic: " + numDocs);
        
        // Create output directory if needed
        Files.createDirectories(outputRun.getParent());
        
        // Parse topics
        List<Topic> topics = parseTopics(topicsPath);
        System.out.println("Parsed " + topics.size() + " topics");
        
        // Open index for searching
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            
            // Setup enhanced query parser with optimized field boosting
            EnglishAnalyzer analyzer = new EnglishAnalyzer();
            String[] fields = {"TITLE", "TEXT", "CONTENT"};
            Map<String, Float> boosts = new HashMap<>();
            boosts.put("TITLE", 3.5f);    // Increased title importance
            boosts.put("TEXT", 1.0f);
            boosts.put("CONTENT", 0.8f);   // Reduced to avoid double-counting
            
            // Set custom BM25 parameters for better performance
            searcher.setSimilarity(new BM25Similarity(1.6f, 0.75f));
            
            MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, analyzer, boosts);
            parser.setDefaultOperator(QueryParser.Operator.OR);
            
            // Process each topic with advanced strategies
            try (BufferedWriter writer = Files.newBufferedWriter(outputRun)) {
                for (Topic topic : topics) {
                    try {
                        processAdvancedTopicSearch(searcher, parser, analyzer, topic, writer, numDocs);
                    } catch (Exception e) {
                        System.err.println("Error processing topic " + topic.getNumber() + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }
        
        System.out.println("Search completed! Results written to: " + outputRun);
    }
    
    private void processAdvancedTopicSearch(IndexSearcher searcher, MultiFieldQueryParser parser, 
                                           EnglishAnalyzer analyzer, Topic topic, BufferedWriter writer, int numDocs) 
            throws IOException, ParseException {
        
        System.out.println("Processing topic " + topic.getNumber() + " with advanced strategies...");
        
        // Strategy 1: Multiple query approaches
        List<TopDocs> allResults = new ArrayList<>();
        
        // 1. Standard combined query
        String combinedQuery = generateEnhancedQuery(topic);
        Query query1 = parser.parse(QueryParser.escape(combinedQuery));
        TopDocs results1 = searcher.search(query1, numDocs);
        allResults.add(results1);
        
        // 2. Title-focused query with phrase matching
        String titleQuery = generateTitleQuery(topic);
        if (!titleQuery.isEmpty()) {
            Query query2 = parser.parse(titleQuery);
            TopDocs results2 = searcher.search(query2, Math.min(500, numDocs));
            allResults.add(results2);
        }
        
        // 3. Description-expanded query
        String expandedQuery = generateExpandedQuery(topic, analyzer);
        if (!expandedQuery.isEmpty()) {
            Query query3 = parser.parse(QueryParser.escape(expandedQuery));
            TopDocs results3 = searcher.search(query3, Math.min(500, numDocs));
            allResults.add(results3);
        }
        
        // 4. Pseudo-relevance feedback (if we have initial results)
        if (results1.scoreDocs.length > 0) {
            String feedbackQuery = generatePseudoRelevanceFeedbackQuery(searcher, results1, analyzer, topic);
            if (!feedbackQuery.isEmpty()) {
                Query query4 = parser.parse(QueryParser.escape(feedbackQuery));
                TopDocs results4 = searcher.search(query4, Math.min(500, numDocs));
                allResults.add(results4);
            }
        }
        
        // Merge and rerank results
        TopDocs finalResults = mergeAndRerankResults(allResults, searcher, topic, numDocs);
        
        // Write results in TREC format
        writeResults(writer, topic.getNumber(), finalResults, searcher);
        
        System.out.println("Topic " + topic.getNumber() + " completed: " + finalResults.scoreDocs.length + " results");
    }
    
    private String generateQuery(Topic topic) {
        StringBuilder queryBuilder = new StringBuilder();
        
        // Primary: Use title (most important)
        String title = cleanQueryText(topic.getTitle());
        if (!title.isEmpty()) {
            queryBuilder.append(title);
        }
        
        // Secondary: Add key terms from description
        String description = cleanQueryText(topic.getDescription());
        if (!description.isEmpty()) {
            if (queryBuilder.length() > 0) {
                queryBuilder.append(" ");
            }
            queryBuilder.append(description);
        }
        
        // Advanced: Add selective terms from narrative (optional)
        String narrative = extractKeyTermsFromNarrative(topic.getNarrative());
        if (!narrative.isEmpty()) {
            if (queryBuilder.length() > 0) {
                queryBuilder.append(" ");
            }
            queryBuilder.append(narrative);
        }
        
        return queryBuilder.toString();
    }
    
    private String cleanQueryText(String text) {
        if (text == null || text.trim().isEmpty()) {
            return "";
        }
        
        // Remove common stop phrases and clean up
        text = text.replaceAll("(?i)\\b(what|how|when|where|why|who|which|that|this|the|a|an)\\b", " ");
        text = text.replaceAll("[^a-zA-Z0-9\\s]", " ");  // Remove special characters
        text = text.replaceAll("\\s+", " ");              // Normalize whitespace
        
        return text.trim();
    }
    
    private String extractKeyTermsFromNarrative(String narrative) {
        if (narrative == null || narrative.trim().isEmpty()) {
            return "";
        }
        
        // Extract only positive terms from narrative, avoid negations
        String[] sentences = narrative.split("[.!?]");
        StringBuilder keyTerms = new StringBuilder();
        
        for (String sentence : sentences) {
            if (!sentence.toLowerCase().contains("not relevant") && 
                !sentence.toLowerCase().contains("not ") &&
                !sentence.toLowerCase().contains("no ") &&
                sentence.toLowerCase().contains("relevant")) {
                
                String cleaned = cleanQueryText(sentence);
                if (!cleaned.isEmpty() && keyTerms.length() < 100) { // Limit narrative contribution
                    if (keyTerms.length() > 0) {
                        keyTerms.append(" ");
                    }
                    keyTerms.append(cleaned);
                }
            }
        }
        
        return keyTerms.toString();
    }
    
    private void writeResults(BufferedWriter writer, String topicNumber, TopDocs results, 
                             IndexSearcher searcher) throws IOException {
        
        ScoreDoc[] scoreDocs = results.scoreDocs;
        
        for (int i = 0; i < scoreDocs.length; i++) {
            Document doc = searcher.doc(scoreDocs[i].doc);
            String docno = doc.get("DOCNO");
            
            if (docno != null) {
                // TREC format: topic_id Q0 docno rank score run_tag
                String line = String.format("%s Q0 %s %d %.6f %s%n", 
                    topicNumber, docno, (i + 1), scoreDocs[i].score, RUN_TAG);
                writer.write(line);
            }
        }
    }
    
    private List<Topic> parseTopics(Path topicsPath) throws IOException {
        List<Topic> topics = new ArrayList<>();
        String content = Files.readString(topicsPath);
        
        Matcher matcher = TOPIC_PATTERN.matcher(content);
        while (matcher.find()) {
            String number = matcher.group(1);
            String title = matcher.group(2).trim();
            String description = matcher.group(3).trim();
            String narrative = matcher.group(4).trim();
            
            topics.add(new Topic(number, title, description, narrative));
        }
        
        return topics;
    }
    
    private String generateEnhancedQuery(Topic topic) {
        StringBuilder query = new StringBuilder();
        
        // Add title with higher importance
        if (topic.getTitle() != null && !topic.getTitle().trim().isEmpty()) {
            String title = cleanQueryText(topic.getTitle());
            query.append(title);
            
            // Add phrase version for exact matching
            if (title.contains(" ")) {
                query.append(" \"").append(title).append("\"^2");
            }
        }
        
        // Add description with term filtering
        if (topic.getDescription() != null && !topic.getDescription().trim().isEmpty()) {
            if (query.length() > 0) query.append(" ");
            String description = cleanDescription(topic.getDescription());
            query.append(description);
        }
        
        return query.toString();
    }
    
    private String generateTitleQuery(Topic topic) {
        if (topic.getTitle() == null || topic.getTitle().trim().isEmpty()) {
            return "";
        }
        
        String title = cleanQueryText(topic.getTitle());
        StringBuilder query = new StringBuilder();
        
        // Exact phrase match with high boost
        query.append("TITLE:\"").append(title).append("\"^5 ");
        
        // Individual terms in title field
        String[] terms = title.split("\\s+");
        for (String term : terms) {
            if (term.length() > 2) {
                query.append("TITLE:").append(term).append("^3 ");
            }
        }
        
        return query.toString().trim();
    }
    
    private String generateExpandedQuery(Topic topic, EnglishAnalyzer analyzer) {
        StringBuilder query = new StringBuilder();
        
        // Start with basic query
        query.append(generateEnhancedQuery(topic));
        
        // Add synonyms and related terms (simplified approach)
        if (topic.getTitle() != null) {
            String[] titleTerms = topic.getTitle().toLowerCase().split("\\s+");
            for (String term : titleTerms) {
                // Add common variations
                if (term.endsWith("s") && term.length() > 3) {
                    query.append(" ").append(term.substring(0, term.length() - 1));
                }
                if (term.endsWith("ing") && term.length() > 5) {
                    query.append(" ").append(term.substring(0, term.length() - 3));
                }
            }
        }
        
        return query.toString();
    }
    
    private String generatePseudoRelevanceFeedbackQuery(IndexSearcher searcher, TopDocs initialResults, 
                                                       EnglishAnalyzer analyzer, Topic topic) {
        try {
            // Use top 3 documents for feedback
            int feedbackDocs = Math.min(3, initialResults.scoreDocs.length);
            Map<String, Integer> termFreq = new HashMap<>();
            
            for (int i = 0; i < feedbackDocs; i++) {
                Document doc = searcher.doc(initialResults.scoreDocs[i].doc);
                String content = doc.get("CONTENT");
                String title = doc.get("TITLE");
                
                if (content != null) {
                    extractTerms(content, termFreq, analyzer, 0.5f);
                }
                if (title != null) {
                    extractTerms(title, termFreq, analyzer, 1.0f);
                }
            }
            
            // Get top expansion terms
            return termFreq.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(5)
                .map(e -> e.getKey())
                .collect(Collectors.joining(" "));
                
        } catch (IOException e) {
            System.err.println("Error in pseudo-relevance feedback: " + e.getMessage());
            return "";
        }
    }
    
    private void extractTerms(String text, Map<String, Integer> termFreq, EnglishAnalyzer analyzer, float weight) {
        try (TokenStream tokenStream = analyzer.tokenStream("field", text)) {
            CharTermAttribute termAttr = tokenStream.getAttribute(CharTermAttribute.class);
            tokenStream.reset();
            
            while (tokenStream.incrementToken()) {
                String term = termAttr.toString();
                if (term.length() > 3 && term.matches("[a-zA-Z]+")) {
                    termFreq.merge(term, (int)(weight * 10), Integer::sum);
                }
            }
            tokenStream.end();
        } catch (IOException e) {
            // Ignore tokenization errors
        }
    }
    
    private TopDocs mergeAndRerankResults(List<TopDocs> allResults, IndexSearcher searcher, 
                                        Topic topic, int numDocs) throws IOException {
        
        Map<String, Float> docScores = new HashMap<>();
        Set<String> seenDocs = new HashSet<>();
        
        // Combine results with different weights
        float[] weights = {1.0f, 0.8f, 0.6f, 0.4f}; // Weights for different strategies
        
        for (int i = 0; i < allResults.size() && i < weights.length; i++) {
            TopDocs results = allResults.get(i);
            float weight = weights[i];
            
            for (int j = 0; j < results.scoreDocs.length; j++) {
                ScoreDoc scoreDoc = results.scoreDocs[j];
                Document doc = searcher.doc(scoreDoc.doc);
                String docno = doc.get("DOCNO");
                
                if (docno != null) {
                    // Combine scores with position penalty
                    float positionPenalty = 1.0f / (float)Math.log(j + 2);
                    float combinedScore = scoreDoc.score * weight * positionPenalty;
                    
                    docScores.merge(docno, combinedScore, Float::sum);
                    seenDocs.add(docno);
                }
            }
        }
        
        // Create final results
        List<ScoreDoc> finalScores = new ArrayList<>();
        for (Map.Entry<String, Float> entry : docScores.entrySet()) {
            // Find document ID for this docno
            for (TopDocs results : allResults) {
                for (ScoreDoc scoreDoc : results.scoreDocs) {
                    try {
                        Document doc = searcher.doc(scoreDoc.doc);
                        if (entry.getKey().equals(doc.get("DOCNO"))) {
                            finalScores.add(new ScoreDoc(scoreDoc.doc, entry.getValue()));
                            break;
                        }
                    } catch (IOException e) {
                        // Skip problematic docs
                    }
                }
                if (finalScores.size() >= numDocs) break;
            }
            if (finalScores.size() >= numDocs) break;
        }
        
        // Sort by score and limit results
        finalScores.sort((a, b) -> Float.compare(b.score, a.score));
        ScoreDoc[] finalArray = finalScores.subList(0, Math.min(numDocs, finalScores.size()))
                                          .toArray(new ScoreDoc[0]);
        
        return new TopDocs(new TotalHits(finalArray.length, TotalHits.Relation.EQUAL_TO), finalArray);
    }
    
    private String cleanDescription(String description) {
        // Remove common stop patterns and clean description
        String cleaned = description.replaceAll("(?i)\\b(relevant documents?|discuss|document|report|article)\\b", "")
                                   .replaceAll("\\s+", " ")
                                   .trim();
        return cleaned;
    }
    
    /**
     * Inner class to represent a topic
     */
    private static class Topic {
        private final String number;
        private final String title;
        private final String description;
        private final String narrative;
        
        public Topic(String number, String title, String description, String narrative) {
            this.number = number;
            this.title = title;
            this.description = description;
            this.narrative = narrative;
        }
        
        public String getNumber() { return number; }
        public String getTitle() { return title; }
        public String getDescription() { return description; }
        public String getNarrative() { return narrative; }
        
        @Override
        public String toString() {
            return String.format("Topic %s: %s", number, title);
        }
    }
}