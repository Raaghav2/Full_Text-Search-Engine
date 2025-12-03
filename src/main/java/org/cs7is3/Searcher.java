package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

public class Searcher {

    private final Analyzer analyzer = new EnglishAnalyzer();

    private static final String RUN_TAG = "CS7IS3_Entity_RM3_Tuned";

    private static final int FEEDBACK_DOCS = 15;
    private static final int EXPANSION_TERMS = 30;
    private static final float EXPANSION_BOOST = 0.4f;
    private static final float TITLE_BOOST = 4.0f;
    private static final float DESC_BOOST = 1.2f;
    private static final float NARR_BOOST = 0.3f;
    private static final double MAX_DF_RATIO = 0.10;
    private static final int MAX_FEEDBACK_TOKENS = 200;

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new BM25Similarity(1.2f, 0.75f));

        QueryParser parser = new QueryParser("TEXT", analyzer);
        parser.setSplitOnWhitespace(true);

        List<Topic> topics = new TopicParser().parse(topicsPath);
        if (outputRun.getParent() != null) {
            outputRun.getParent().toFile().mkdirs();
        }
        int totalDocs = reader.maxDoc();

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                try {
                    BooleanQuery.Builder anchor = new BooleanQuery.Builder();

                    if (topic.title != null && !topic.title.isEmpty()) {
                        anchor.add(
                                new BoostQuery(
                                        parser.parse(QueryParser.escape(topic.title)),
                                        TITLE_BOOST
                                ),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    if (topic.description != null && !topic.description.isEmpty()) {
                        anchor.add(
                                new BoostQuery(
                                        parser.parse(QueryParser.escape(topic.description)),
                                        DESC_BOOST
                                ),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    if (topic.narrative != null && !topic.narrative.isEmpty()) {
                        String n = filterNarrative(topic.narrative);
                        if (!n.isEmpty()) {
                            anchor.add(
                                    new BoostQuery(
                                            parser.parse(QueryParser.escape(n)),
                                            NARR_BOOST
                                    ),
                                    BooleanClause.Occur.SHOULD
                            );
                        }
                    }

                    TopDocs pilot = searcher.search(anchor.build(), FEEDBACK_DOCS);

                    Map<String, Double> weights = new HashMap<>();
                    Map<String, Integer> fbDocCount = new HashMap<>();
                    Set<String> original = getQueryTerms(topic);

                    for (ScoreDoc hit : pilot.scoreDocs) {
                        Document d = searcher.doc(hit.doc);
                        String text = d.get("TEXT");
                        if (text == null) {
                            continue;
                        }

                        Map<String, Boolean> terms = analyze(text);
                        for (Map.Entry<String, Boolean> e : terms.entrySet()) {
                            String t = e.getKey();
                            if (original.contains(t)) {
                                continue;
                            }

                            fbDocCount.put(t, fbDocCount.getOrDefault(t, 0) + 1);

                            int df = reader.docFreq(new Term("TEXT", t));
                            if (df < 2 || df > (int) (totalDocs * MAX_DF_RATIO)) {
                                continue;
                            }

                            double w = (Math.log((double) totalDocs / (df + 1)) + 1.0) * hit.score;
                            if (e.getValue()) {
                                w *= 1.25;
                            }
                            weights.put(t, weights.getOrDefault(t, 0.0) + w);
                        }
                    }

                    List<String> exp = weights.entrySet().stream()
                            .filter(e -> fbDocCount.getOrDefault(e.getKey(), 0) >= 2)
                            .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                            .limit(EXPANSION_TERMS)
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    BooleanQuery.Builder finalQ = new BooleanQuery.Builder();
                    finalQ.add(anchor.build(), BooleanClause.Occur.SHOULD);

                    if (!exp.isEmpty()) {
                        String expQueryStr = String.join(" ", exp);
                        finalQ.add(
                                new BoostQuery(
                                        parser.parse(QueryParser.escape(expQueryStr)),
                                        EXPANSION_BOOST
                                ),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    ScoreDoc[] hits = searcher.search(finalQ.build(), numDocs).scoreDocs;
                    for (int i = 0; i < hits.length; i++) {
                        Document doc = searcher.doc(hits[i].doc);
                        String docno = doc.get("DOCNO");
                        writer.printf(
                                "%s Q0 %s %d %.4f %s%n",
                                topic.number,
                                docno,
                                i + 1,
                                hits[i].score,
                                RUN_TAG
                        );
                    }
                    writer.flush();
                } catch (Exception e) {
                    System.err.println("Error on topic: " + topic.number);
                    e.printStackTrace();
                }
            }
            System.out.println("Search Complete (Entity-aware RM3 tuned).");
        } finally {
            reader.close();
        }
    }

    private Map<String, Boolean> analyze(String text) throws IOException {
        Map<String, Boolean> map = new HashMap<>();
        Set<String> caps = new HashSet<>();

        String[] raw = text.split("\\s+");
        int max = Math.min(raw.length, MAX_FEEDBACK_TOKENS);
        for (int i = 0; i < max; i++) {
            if (raw[i].length() > 0 && Character.isUpperCase(raw[i].charAt(0))) {
                String norm = raw[i].replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!norm.isEmpty()) {
                    caps.add(norm);
                }
            }
        }

        try (TokenStream ts = analyzer.tokenStream("TEXT", new StringReader(text))) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            int c = 0;
            while (ts.incrementToken() && c++ < MAX_FEEDBACK_TOKENS) {
                String s = term.toString();
                if (s.length() > 3 && !s.matches(".*\\d.*")) {
                    map.put(s, caps.contains(s));
                }
            }
            ts.end();
        }

        return map;
    }

    private Set<String> getQueryTerms(Topic t) throws IOException {
        Set<String> s = new HashSet<>();
        StringBuilder sb = new StringBuilder();
        if (t.title != null) sb.append(t.title).append(' ');
        if (t.description != null) sb.append(t.description);

        try (TokenStream ts = analyzer.tokenStream("TEXT", new StringReader(sb.toString()))) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                s.add(term.toString());
            }
            ts.end();
        }

        return s;
    }

    private String filterNarrative(String n) {
        StringBuilder sb = new StringBuilder();
        for (String s : n.split("[\\s\\.\\;\\n]+")) {
            String l = s.toLowerCase().replaceAll("[^a-z]", "");
            if (l.isEmpty()) continue;
            if (l.contains("not")) continue;
            if (l.contains("irrelevant")) continue;
            if (l.contains("ignore")) continue;
            if (l.contains("unrelated")) continue;
            sb.append(s).append(' ');
        }
        return sb.toString();
    }
}

