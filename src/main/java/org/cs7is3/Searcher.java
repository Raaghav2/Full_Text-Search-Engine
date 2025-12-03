package org.cs7is3;

import java.io.*;
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
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

public class Searcher {

    private final Analyzer analyzer = new EnglishAnalyzer();
    private static final String RUN_TAG = "CS7IS3_TwoStage_SDM_Entity_RM3";

    private static final int FEEDBACK_DOCS = 20;
    private static final int EXPANSION_TERMS = 40;
    private static final float EXPANSION_BOOST = 0.5f;

    private static final float TITLE_BOOST = 3.0f;
    private static final float DESC_BOOST = 1.3f;
    private static final float NARR_BOOST = 0.5f;

    private static final double MAX_DF_RATIO = 0.15;
    private static final int MAX_FEEDBACK_TOKENS = 200;

    private static final float SDM_UNIGRAM_WEIGHT = 0.8f;
    private static final float SDM_PHRASE_WEIGHT = 0.2f;
    private static final int PHRASE_SLOP = 1;

    private static final int RERANK_CANDIDATES = 1000;
    private static final double RERANK_TERM_WEIGHT = 0.8;
    private static final double RERANK_BIGRAM_WEIGHT = 0.5;
    private static final double RERANK_LEN_PENALTY = 0.1;

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
                    BooleanQuery.Builder anchorBuilder = new BooleanQuery.Builder();

                    if (topic.title != null && !topic.title.isEmpty()) {
                        anchorBuilder.add(
                                new BoostQuery(
                                        parser.parse(QueryParser.escape(topic.title)),
                                        TITLE_BOOST
                                ),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    if (topic.description != null && !topic.description.isEmpty()) {
                        anchorBuilder.add(
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
                            anchorBuilder.add(
                                    new BoostQuery(
                                            parser.parse(QueryParser.escape(n)),
                                            NARR_BOOST
                                    ),
                                    BooleanClause.Occur.SHOULD
                            );
                        }
                    }

                    Query anchorUnigram = anchorBuilder.build();

                    List<String> termList = getQueryTermList(topic);
                    List<String> queryTerms = new ArrayList<>(new LinkedHashSet<>(termList));
                    List<String> queryBigrams = getQueryBigrams(termList);

                    Query sdmQuery = buildSDMQuery(anchorUnigram, termList);

                    TopDocs pilot = searcher.search(sdmQuery, FEEDBACK_DOCS);

                    Map<String, Double> weights = new HashMap<>();
                    Set<String> original = getQueryTerms(topic);

                    for (ScoreDoc hit : pilot.scoreDocs) {
                        Document d = searcher.doc(hit.doc);
                        String text = d.get("TEXT");
                        if (text == null) continue;

                        Map<String, Boolean> terms = analyze(text);
                        for (Map.Entry<String, Boolean> e : terms.entrySet()) {
                            String t = e.getKey();
                            if (original.contains(t)) continue;

                            int df = reader.docFreq(new Term("TEXT", t));
                            if (df < 2 || df > (int) (totalDocs * MAX_DF_RATIO)) continue;

                            double w = (Math.log((double) totalDocs / (df + 1)) + 1.0) * hit.score;
                            if (e.getValue()) w *= 1.25;
                            weights.put(t, weights.getOrDefault(t, 0.0) + w);
                        }
                    }

                    List<String> exp = weights.entrySet().stream()
                            .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                            .limit(EXPANSION_TERMS)
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    BooleanQuery.Builder finalQ = new BooleanQuery.Builder();
                    finalQ.add(sdmQuery, BooleanClause.Occur.SHOULD);

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

                    int rerankDocs = Math.max(numDocs, RERANK_CANDIDATES);
                    TopDocs firstPass = searcher.search(finalQ.build(), rerankDocs);

                    List<RerankedDoc> reranked = new ArrayList<>();
                    for (ScoreDoc sd : firstPass.scoreDocs) {
                        Document doc = searcher.doc(sd.doc);
                        String docno = doc.get("DOCNO");
                        String text = doc.get("TEXT");
                        if (docno == null || text == null) continue;

                        double finalScore = computeRerankScore(text, queryTerms, queryBigrams, sd.score);
                        reranked.add(new RerankedDoc(sd.doc, docno, finalScore));
                    }

                    reranked.sort((a, b) -> Double.compare(b.score, a.score));

                    int outN = Math.min(numDocs, reranked.size());
                    for (int i = 0; i < outN; i++) {
                        RerankedDoc rd = reranked.get(i);
                        writer.printf(
                                "%s Q0 %s %d %.4f %s%n",
                                topic.number,
                                rd.docno,
                                i + 1,
                                rd.score,
                                RUN_TAG
                        );
                    }
                    writer.flush();
                } catch (Exception e) {
                    System.err.println("Error: " + topic.number);
                    e.printStackTrace();
                }
            }
            System.out.println("Search Complete (Two-stage SDM + Entity RM3).");
        } finally {
            reader.close();
        }
    }

    private Query buildSDMQuery(Query anchorUnigram, List<String> terms) {
        BooleanQuery.Builder sdm = new BooleanQuery.Builder();
        sdm.add(new BoostQuery(anchorUnigram, SDM_UNIGRAM_WEIGHT), BooleanClause.Occur.SHOULD);

        Query phraseQ = buildPhraseBigrams(terms);
        if (phraseQ != null) {
            sdm.add(new BoostQuery(phraseQ, SDM_PHRASE_WEIGHT), BooleanClause.Occur.SHOULD);
        }

        return sdm.build();
    }

    private Query buildPhraseBigrams(List<String> terms) {
        if (terms.size() < 2) return null;
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        for (int i = 0; i + 1 < terms.size(); i++) {
            String t1 = terms.get(i);
            String t2 = terms.get(i + 1);
            if (t1.equals(t2)) continue;

            PhraseQuery.Builder pb = new PhraseQuery.Builder();
            pb.add(new Term("TEXT", t1), 0);
            pb.add(new Term("TEXT", t2), 1);
            pb.setSlop(PHRASE_SLOP);
            b.add(pb.build(), BooleanClause.Occur.SHOULD);
        }
        return b.build();
    }

    private List<String> getQueryBigrams(List<String> terms) {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (int i = 0; i + 1 < terms.size(); i++) {
            String t1 = terms.get(i);
            String t2 = terms.get(i + 1);
            if (t1.equals(t2)) continue;
            set.add(t1 + " " + t2);
        }
        return new ArrayList<>(set);
    }

    private double computeRerankScore(String text, List<String> queryTerms, List<String> queryBigrams, float firstStageScore) throws IOException {
        if (text == null || text.isEmpty()) return firstStageScore;

        Set<String> docTerms = new HashSet<>();
        Set<String> docBigrams = new HashSet<>();
        int len = 0;

        try (TokenStream ts = analyzer.tokenStream("TEXT", new StringReader(text))) {
            CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            String prev = null;
            while (ts.incrementToken()) {
                String s = termAttr.toString();
                len++;
                docTerms.add(s);
                if (prev != null) {
                    docBigrams.add(prev + " " + s);
                }
                prev = s;
            }
            ts.end();
        }

        int matchedTerms = 0;
        for (String qt : queryTerms) {
            if (docTerms.contains(qt)) matchedTerms++;
        }
        double termCov = queryTerms.isEmpty() ? 0.0 : (double) matchedTerms / queryTerms.size();

        int matchedBigrams = 0;
        for (String bg : queryBigrams) {
            if (docBigrams.contains(bg)) matchedBigrams++;
        }
        double bigramCov = queryBigrams.isEmpty() ? 0.0 : (double) matchedBigrams / queryBigrams.size();

        double lenNorm = len > 0 ? Math.log(1.0 + len) : 0.0;

        return firstStageScore
                + RERANK_TERM_WEIGHT * termCov
                + RERANK_BIGRAM_WEIGHT * bigramCov
                - RERANK_LEN_PENALTY * lenNorm;
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
        if (t.description != null) sb.append(t.description).append(' ');

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

    private List<String> getQueryTermList(Topic t) throws IOException {
        List<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        if (t.title != null) sb.append(t.title).append(' ');
        if (t.description != null) sb.append(t.description).append(' ');
        if (t.narrative != null) sb.append(filterNarrative(t.narrative));

        try (TokenStream ts = analyzer.tokenStream("TEXT", new StringReader(sb.toString()))) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                list.add(term.toString());
            }
            ts.end();
        }
        return list;
    }

    private String filterNarrative(String n) {
        StringBuilder sb = new StringBuilder();
        for (String s : n.split("[\\s\\.\\;\\n]+")) {
            String l = s.toLowerCase().replaceAll("[^a-z]", "");
            if (l.isEmpty()) continue;
            if (l.contains("not")) continue;
            if (l.contains("irrelevant")) continue;
            if (l.contains("ignore")) continue;
            sb.append(s).append(" ");
        }
        return sb.toString();
    }

    private static class RerankedDoc {
        final int docId;
        final String docno;
        final double score;

        RerankedDoc(int docId, String docno, double score) {
            this.docId = docId;
            this.docno = docno;
            this.score = score;
        }
    }
}



