package org.cs7is3;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Searcher {

    // 使用自定义分析器（KStem + 停用词等）
    private final Analyzer analyzer = new CustomAnalyzer();

    // TREC run 的标签（在 leaderboard 上显示）
    private static final String RUN_TAG = "CS7IS3_Ultimate_Boosted_KStem";
    private static final int DEFAULT_NUM_DOCS = 1000;

    /**
     * 运行检索，生成 TREC 格式的 run 文件
     *
     * @param indexPath 索引目录
     * @param topicsPath topics 文件路径
     * @param outputRun 输出 run 文件路径
     * @param numDocs 每个 topic 检索的文档数（<=0 时使用默认 1000）
     */
    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {

        if (numDocs <= 0) {
            numDocs = DEFAULT_NUM_DOCS;
        }

        // 打开索引
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
            IndexSearcher searcher = new IndexSearcher(reader);

            // 统一使用经过调参的 BM25，相对稳定
            // 可以根据需要多试一些参数组合，例如 (1.2f, 0.75f), (1.5f, 0.75f), (1.5f, 0.9f)
            searcher.setSimilarity(new BM25Similarity(1.5f, 0.75f));

            // TITLE 字段：启用短语查询 + 小 slop，利用短语信息
            QueryParser titleParser = new QueryParser("TITLE", analyzer);
            titleParser.setAutoGeneratePhraseQueries(true);
            titleParser.setPhraseSlop(2);

            // TEXT 字段（description / narrative）：只做 bag-of-words，不自动短语
            QueryParser textParser = new QueryParser("TEXT", analyzer);
            textParser.setAutoGeneratePhraseQueries(false);

            // 解析 topics
            TopicParser topicParser = new TopicParser();
            List<Topic> topics = topicParser.parse(topicsPath);

            // 确保输出目录存在
            if (outputRun.getParent() != null) {
                Files.createDirectories(outputRun.getParent());
            }

            int totalResultsWritten = 0;

            try (PrintWriter writer =
                         new PrintWriter(Files.newBufferedWriter(outputRun, StandardCharsets.UTF_8))) {

                for (Topic topic : topics) {

                    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

                    try {
                        // -------------------------
                        // 1) TITLE：权重最高（3.5）
                        // -------------------------
                        if (!isNullOrEmpty(topic.title)) {
                            String escapedTitle = QueryParser.escape(topic.title);
                            Query titleQuery = titleParser.parse(escapedTitle);
                            queryBuilder.add(
                                    new BoostQuery(titleQuery, 3.5f),
                                    BooleanClause.Occur.SHOULD
                            );
                        }

                        // -------------------------
                        // 2) DESCRIPTION：中等权重（1.7）
                        // -------------------------
                        if (!isNullOrEmpty(topic.description)) {
                            String escapedDesc = QueryParser.escape(topic.description);
                            Query descQuery = textParser.parse(escapedDesc);
                            queryBuilder.add(
                                    new BoostQuery(descQuery, 1.7f),
                                    BooleanClause.Occur.SHOULD
                            );
                        }

                        // -------------------------
                        // 3) NARRATIVE：过滤负例 + 截断，仅轻微补充
                        // -------------------------
                        if (!isNullOrEmpty(topic.narrative)) {
                            String cleanNarrative = filterNegativeNarrative(topic.narrative);
                            if (!isNullOrEmpty(cleanNarrative)) {
                                String escapedNarr = QueryParser.escape(cleanNarrative);
                                Query narrQuery = textParser.parse(escapedNarr);
                                // 这里不额外 Boost，保持默认 1.0 权重
                                queryBuilder.add(
                                        narrQuery,
                                        BooleanClause.Occur.SHOULD
                                );
                            }
                        }

                        BooleanQuery finalQuery = queryBuilder.build();

                        // 如果这个 topic 没有任何子句，就跳过
                        if (finalQuery.clauses().isEmpty()) {
                            System.err.println("Empty query for topic " + topic.number + ", skipping.");
                            continue;
                        }

                        // 执行检索
                        TopDocs topDocs = searcher.search(finalQuery, numDocs);
                        ScoreDoc[] hits = topDocs.scoreDocs;

                        for (int rank = 0; rank < hits.length; rank++) {
                            ScoreDoc hit = hits[rank];
                            Document doc = searcher.doc(hit.doc);

                            // DOCNO 字段名要和你 index 时一致
                            String docNo = doc.get("DOCNO");
                            if (isNullOrEmpty(docNo)) {
                                continue;
                            }

                            // TREC format: qid Q0 docno rank score runTag
                            writer.printf(
                                    "%s Q0 %s %d %.4f %s%n",
                                    topic.number,
                                    docNo,
                                    rank + 1,
                                    hit.score,
                                    RUN_TAG
                            );
                            totalResultsWritten++;
                        }

                        writer.flush();

                    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
                        System.err.println("Error parsing query for topic " + topic.number + ": " + e.getMessage());
                    }
                }

                System.out.println("Finished searching. Wrote " + totalResultsWritten + " results.");
                System.out.println("Results saved to: " + outputRun.toAbsolutePath());
            }
        }
    }

    /**
     * 过滤 narrative 中明显的“负例句子”，并最多保留 2 句正向描述，
     * 避免 query 过长影响 BM25。
     */
    private String filterNegativeNarrative(String narrative) {
        String[] sentences = narrative.split("[\\.\\;\\n]");
        StringBuilder clean = new StringBuilder();
        int added = 0;

        for (String raw : sentences) {
            String sentence = raw.trim();
            if (sentence.isEmpty()) continue;

            String lower = sentence.toLowerCase();
            if (lower.contains("not relevant") || lower.contains("irrelevant")) {
                // 负例句子直接跳过
                continue;
            }

            clean.append(sentence).append(' ');
            added++;

            // 最多保留两句
            if (added >= 2) break;
        }

        return clean.toString().trim();
    }

    private boolean isNullOrEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }
}
