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
    private static final String RUN_TAG = "CS7IS3_SDM_Entity_RM3_Phrase";

    // RM3 / PRF 参数
    private static final int FEEDBACK_DOCS = 20;
    private static final int EXPANSION_TERMS = 40;
    private static final float EXPANSION_BOOST = 0.5f;

    // 原 query 各部分权重
    private static final float TITLE_BOOST = 3.0f;
    private static final float DESC_BOOST = 1.3f;
    private static final float NARR_BOOST = 0.5f;

    private static final double MAX_DF_RATIO = 0.15;
    private static final int MAX_FEEDBACK_TOKENS = 200;

    // SDM-ish 参数：unigram + phrase
    private static final float SDM_UNIGRAM_WEIGHT = 0.8f;
    private static final float SDM_PHRASE_WEIGHT = 0.2f;
    private static final int PHRASE_SLOP = 1; // 小窗口短语匹配

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
                    // 1. 构造 anchor（原始 title/description/narrative 加权）
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

                    // 2. 从 topic 文本里提取 term 列表，用来构造 phrase bigrams
                    List<String> termList = getQueryTermList(topic);

                    // 3. 构造 SDM 风格的 query：unigram + phrase bigrams
                    Query sdmQuery = buildSDMQuery(anchorUnigram, termList);

                    // 4. 用 SDM query 做 PRF：检索 top FEEDBACK_DOCS 作反馈文档
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

                    // 5. 选出 top K 扩展词
                    List<String> exp = weights.entrySet().stream()
                            .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                            .limit(EXPANSION_TERMS)
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    // 6. 最终查询：SDM(query) + 扩展词
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
                    System.err.println("Error: " + topic.number);
                    e.printStackTrace();
                }
            }
            System.out.println("Search Complete (SDM + Entity RM3 phrase).");
        } finally {
            reader.close();
        }
    }

    // ===== SDM 相关：unigram + phrase bigrams =====

    private Query buildSDMQuery(Query anchorUnigram, List<String> terms) {
        BooleanQuery.Builder sdm = new BooleanQuery.Builder();

        // unigram 部分：用原 anchor query
        sdm.add(new BoostQuery(anchorUnigram, SDM_UNIGRAM_WEIGHT), BooleanClause.Occur.SHOULD);

        // phrase bigrams 部分
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

    // ===== 文档分析（用于 PRF term 提取） =====

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
            sb.append(s).append(" ");
        }
        return sb.toString();
    }
}



