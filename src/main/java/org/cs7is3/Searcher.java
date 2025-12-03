package org.cs7is3;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;
import org.apache.lucene.document.Document; 
public class Searcher {

    private final Analyzer analyzer;
    private static final String RUN_TAG = "CS7IS3_Entity_RM3_Compact";

    public Searcher() {
        try {
            // CustomAnalyzer: standard tokenizer + lowercase + stop + kstem + asciifolding
            this.analyzer = CustomAnalyzer.builder()
                    .withTokenizer("standard")
                    .addTokenFilter("lowercase")
                    .addTokenFilter("stop")
                    .addTokenFilter("kstem")
                    .addTokenFilter("asciifolding")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create CustomAnalyzer", e);
        }
    }

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
                    // 1. 构建 anchor（原始查询：title + description + narrative）
                    BooleanQuery.Builder anchor = new BooleanQuery.Builder();

                    if (topic.title != null) {
                        anchor.add(
                                new BoostQuery(
                                        parser.parse(QueryParser.escape(topic.title)),
                                        3.0f
                                ),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    if (topic.description != null) {
                        anchor.add(
                                new BoostQuery(
                                        parser.parse(QueryParser.escape(topic.description)),
                                        1.3f
                                ),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    if (topic.narrative != null) {
                        String n = filterNarrative(topic.narrative);
                        if (!n.isEmpty()) {
                            anchor.add(
                                    new BoostQuery(
                                            parser.parse(QueryParser.escape(n)),
                                            0.5f
                                    ),
                                    BooleanClause.Occur.SHOULD
                            );
                        }
                    }

                    // 2. 用 anchor 搜 top 20 文档做 query expansion
                    TopDocs pilot = searcher.search(anchor.build(), 20);
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
                            // 太罕见或者太频繁的词去掉
                            if (df < 2 || df > (int) (totalDocs * 0.15)) continue;

                            double w = (Math.log((double) totalDocs / (df + 1)) + 1.0) * hit.score;
                            if (e.getValue()) {
                                // 如果是“实体”词，加一点权重
                                w *= 1.25;
                            }
                            weights.put(t, weights.getOrDefault(t, 0.0) + w);
                        }
                    }

                    // 3. 选 top 40 扩展词
                    List<String> exp = weights.entrySet().stream()
                            .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                            .limit(40)
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    // 4. 构建最终查询：原 query + 扩展 query
                    BooleanQuery.Builder finalQ = new BooleanQuery.Builder();
                    finalQ.add(anchor.build(), BooleanClause.Occur.SHOULD);

                    if (!exp.isEmpty()) {
                        String expQueryStr = String.join(" ", exp);
                        finalQ.add(
                                new BoostQuery(
                                        parser.parse(QueryParser.escape(expQueryStr)),
                                        0.5f
                                ),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    // 5. 最终检索并输出 TREC run
                    ScoreDoc[] hits = searcher.search(finalQ.build(), numDocs).scoreDocs;
                    for (int i = 0; i < hits.length; i++) {
                        Document doc = searcher.doc(hits[i].doc);
                        String docNo = doc.get("DOCNO");
                        writer.printf(
                                "%s Q0 %s %d %.4f %s%n",
                                topic.number,
                                docNo,
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

            System.out.println("Search Complete.");
        } finally {
            reader.close();
        }
    }

    /**
     * 对文档文本做分析：
     * 返回 map(term -> 是否是“实体”(首字母大写出现过))
     */
    private Map<String, Boolean> analyze(String text) throws IOException {
        Map<String, Boolean> map = new HashMap<>();
        Set<String> caps = new HashSet<>();

        // 简单用原始字符串判断首字母大写的“实体”
        String[] raw = text.split("\\s+");
        int max = Math.min(raw.length, 200); // 只看前 200 个词
        for (int i = 0; i < max; i++) {
            if (raw[i].length() > 0 && Character.isUpperCase(raw[i].charAt(0))) {
                String normalized = raw[i].replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!normalized.isEmpty()) {
                    caps.add(normalized);
                }
            }
        }

        // 用 CustomAnalyzer 做分词
        try (TokenStream ts = analyzer.tokenStream("TEXT", new StringReader(text))) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            int c = 0;
            while (ts.incrementToken() && c++ < 200) {
                String s = term.toString(); // 已经是 lowercase + kstem + stop 等处理后的
                if (s.length() > 3 && !s.matches(".*\\d.*")) {
                    map.put(s, caps.contains(s));
                }
            }
            ts.end();
        }

        return map;
    }

    /**
     * 提取原始查询(title + description)中的 term 集合
     */
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

    /**
     * narrative 里去掉 "not", "irrelevant" 一类负面描述的句子
     */
    private String filterNarrative(String n) {
        StringBuilder sb = new StringBuilder();
        for (String s : n.split("[\\s\\.\\;\\n]+")) {
            String l = s.toLowerCase().replaceAll("[^a-z]", "");
            if (!l.contains("not") && !l.contains("irrelevant") && !l.isEmpty()) {
                sb.append(s).append(' ');
            }
        }
        return sb.toString();
    }
}
