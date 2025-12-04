package org.cs7is3;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

public class Searcher {
    private final Analyzer analyzer = new EnglishAnalyzer();
    private static final String RUN_TAG = "CS7IS3_Entity_RM3_Compact";

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new BM25Similarity(1.2f, 0.75f));

        QueryParser parser = new QueryParser("TEXT", analyzer);
        parser.setSplitOnWhitespace(true);

        List<Topic> topics = new TopicParser().parse(topicsPath);
        if (outputRun.getParent() != null) outputRun.getParent().toFile().mkdirs();
        int totalDocs = reader.maxDoc();

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                try {
                    BooleanQuery.Builder anchor = new BooleanQuery.Builder();
                    if (topic.title != null) anchor.add(new BoostQuery(parser.parse(QueryParser.escape(topic.title)), 3.0f), BooleanClause.Occur.SHOULD);
                    if (topic.description != null) anchor.add(new BoostQuery(parser.parse(QueryParser.escape(topic.description)), 1.3f), BooleanClause.Occur.SHOULD);
                    if (topic.narrative != null) {
                        String n = filterNarrative(topic.narrative);
                        if (!n.isEmpty()) anchor.add(new BoostQuery(parser.parse(QueryParser.escape(n)), 0.5f), BooleanClause.Occur.SHOULD);
                    }

                    TopDocs pilot = searcher.search(anchor.build(), 20);// see the top 20 docs for query expansion
                    Map<String, Double> weights = new HashMap<>();
                    Set<String> original = getQueryTerms(topic);

                    for (ScoreDoc hit : pilot.scoreDocs) {
                        String text = searcher.doc(hit.doc).get("TEXT");
                        if (text == null) continue;
                        
                        Map<String, Boolean> terms = analyze(text);
                        for (Map.Entry<String, Boolean> e : terms.entrySet()) {
                            String t = e.getKey();
                            if (original.contains(t)) continue;

                            int df = reader.docFreq(new Term("TEXT", t));

                            double w = (Math.log((double)totalDocs / (df + 1)) + 1.0) * hit.score;
                            if (e.getValue()) w *= 1.25;//look if they are an entity
                            weights.put(t, weights.getOrDefault(t, 0.0) + w);
                        }
                    }

                    List<String> exp = weights.entrySet().stream()
                        .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                        .limit(40).map(Map.Entry::getKey).collect(Collectors.toList());// add the top 40 to your list

                    BooleanQuery.Builder finalQ = new BooleanQuery.Builder();
                    finalQ.add(anchor.build(), BooleanClause.Occur.SHOULD);
                    if (!exp.isEmpty()) {
                        finalQ.add(new BoostQuery(parser.parse(QueryParser.escape(String.join(" ", exp))), 0.5f), BooleanClause.Occur.SHOULD);//build the final query
                    }

                    ScoreDoc[] hits = searcher.search(finalQ.build(), numDocs).scoreDocs;
                    for (int i = 0; i < hits.length; i++) {
                        writer.printf("%s Q0 %s %d %.4f %s%n", topic.number, searcher.doc(hits[i].doc).get("DOCNO"), i + 1, hits[i].score, RUN_TAG);
                    }
                    writer.flush();
                } catch (Exception e) { System.err.println("Error: " + topic.number); }
            }
            System.out.println("Search Complete.");
        } finally { reader.close(); }
    }

    private Map<String, Boolean> analyze(String text) throws IOException {
        Map<String, Boolean> map = new HashMap<>();
        Set<String> caps = new HashSet<>();
        String[] raw = text.split("\\s+"); //split the words
        int max = Math.min(raw.length, 200); //check only the top200
        for (int i = 0; i < max; i++) {
            if (raw[i].length() > 0 && Character.isUpperCase(raw[i].charAt(0)))// if the word is uppercase, it is an entity
                caps.add(raw[i].replaceAll("[^a-zA-Z]", "").toLowerCase());// store the lowercase version in the set
        }
        try (TokenStream ts = analyzer.tokenStream("TEXT", new StringReader(text))) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            int c = 0;
            while (ts.incrementToken() && c++ < 200) {
                String s = term.toString();
                if (s.length() > 3 && !s.matches(".*\\d.*")) map.put(s, caps.contains(s));//add the term to the map:True/False
            }
            ts.end();
        }
        return map;
    }

    private Set<String> getQueryTerms(Topic t) throws IOException {
        Set<String> s = new HashSet<>();
        try (TokenStream ts = analyzer.tokenStream("TEXT", new StringReader((t.title != null ? t.title : "") + " " + (t.description != null ? t.description : "")))) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) s.add(term.toString());
            ts.end();
        }
        return s;
    }

    private String filterNarrative(String n) {
        StringBuilder sb = new StringBuilder();
        String[] sentences = n.split("[\\.\\;\\n]+"); 

        for (String sentence : sentences) {
            String lower = sentence.toLowerCase();
            if (!lower.contains("not relevant") && !lower.contains("irrelevant")) {
                sb.append(sentence).append(" ");
            }
        }
        return sb.toString();
    }
}
