package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashMap; 
import java.util.List;
import java.util.Map;     

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;

// Advanced Querying
import org.apache.lucene.queryparser.classic.QueryParser; 
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.cs7is3.TopicParser.Topic;

// Mixed Similarity
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.DistributionSPL; 
import org.apache.lucene.search.similarities.LambdaDF;        
import org.apache.lucene.search.similarities.NormalizationH2; 

public class Searcher {

    // 自定义分析器（KStem + 停用词等）
    private final Analyzer analyzer = new CustomAnalyzer(); 

    // leaderboard 上显示的 run 名
    private static final String RUN_TAG = "CS7IS3_Ultimate_Boosted_Phrase_KStem_PRF";

    // 每个 topic 最终返回的文档数
    private static final int DEFAULT_NUM_DOCS = 1000;

    // PRF 参数：用前 fbDocs 篇文档做伪相关反馈，扩展查询权重 prfBoost
    private static final int FB_DOCS = 5;       // 你可以试 5 或 10
    private static final float PRF_BOOST = 0.8f; // 扩展查询的权重（0.4 ~ 0.8 之间可调）

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        
        if (numDocs <= 0) {
            numDocs = DEFAULT_NUM_DOCS;
        }

        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);

        // ======================================================================
        // 1. Mixed Similarity（保留你原来的设置）
        // ======================================================================
        final Map<String, Similarity> fieldSims = new HashMap<>();
        fieldSims.put("TITLE", new IBSimilarity(new DistributionSPL(), new LambdaDF(), new NormalizationH2()));
        fieldSims.put("TEXT", new BM25Similarity());
        
        final Similarity defaultSim = new BM25Similarity();
        
        Similarity mixedSimilarity = new PerFieldSimilarityWrapper() {
            @Override
            public Similarity get(String fieldName) {
                return fieldSims.getOrDefault(fieldName, defaultSim);
            }
        };
        searcher.setSimilarity(mixedSimilarity); 
        
        // ======================================================================
        // 2. QueryParser 设置
        // ======================================================================
        // Title Parser
        QueryParser titleParser = new QueryParser("TITLE", analyzer);
        titleParser.setSplitOnWhitespace(true); 
        titleParser.setAutoGeneratePhraseQueries(true); 
        titleParser.setPhraseSlop(2); 

        // Text Parser
        QueryParser textParser = new QueryParser("TEXT", analyzer);
        textParser.setSplitOnWhitespace(true); 
        textParser.setAutoGeneratePhraseQueries(true); 
        textParser.setPhraseSlop(8); 

        TopicParser topicParser = new TopicParser();
        List<Topic> topics = topicParser.parse(topicsPath);
        
        int totalResultsWritten = 0; 
        
        // --- Ensure output directory exists ---
        if (outputRun.getParent() != null) {
            outputRun.getParent().toFile().mkdirs();
        }

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                
                // ==================================================================
                // 3. 构造原始查询（Original Query）
                // ==================================================================
                BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
                
                try {
                    // --- Title: Boost 3.5 ---
                    if (topic.title != null && !topic.title.isEmpty()) {
                        Query titleQuery = titleParser.parse(QueryParser.escape(topic.title));
                        queryBuilder.add(new BoostQuery(titleQuery, 3.5f), BooleanClause.Occur.SHOULD);
                    }

                    // --- Description: Boost 1.7 ---
                    if (topic.description != null && !topic.description.isEmpty()) {
                        Query descQuery = textParser.parse(QueryParser.escape(topic.description));
                        queryBuilder.add(new BoostQuery(descQuery, 1.7f), BooleanClause.Occur.SHOULD);
                    }

                    // --- Narrative: Filtered, Boost 1.0 ---
                    if (topic.narrative != null && !topic.narrative.isEmpty()) {
                        String cleanNarrative = filterNegativeNarrative(topic.narrative);
                        
                        if (!cleanNarrative.trim().isEmpty()) {
                            Query narrQuery = textParser.parse(QueryParser.escape(cleanNarrative));
                            queryBuilder.add(narrQuery, BooleanClause.Occur.SHOULD);
                        }
                    }
                    
                    BooleanQuery originalQuery = queryBuilder.build();

                    // 如果原始查询没有任何子句，直接跳过这个 topic
                    if (originalQuery.clauses().isEmpty()) {
                        System.err.println("Empty query for topic " + topic.number + ", skipping.");
                        continue;
                    }

                    // ==================================================================
                    // 4. PRF：基于原始查询做一次初检 + 构造扩展查询
                    // ==================================================================
                    Query finalQuery = originalQuery;
                    try {
                        finalQuery = buildPrfQuery(
                                searcher,
                                originalQuery,
                                textParser,
                                FB_DOCS,
                                PRF_BOOST
                        );
                    } catch (ParseException e) {
                        System.err.println("PRF parse error for topic " + topic.number + ": " + e.getMessage());
                        // 出问题就退回原始查询
                        finalQuery = originalQuery;
                    }

                    // ==================================================================
                    // 5. 执行最终检索（用扩展后的查询）
                    // ==================================================================
                    ScoreDoc[] hits = searcher.search(finalQuery, numDocs).scoreDocs;

                    for (int rank = 0; rank < hits.length; rank++) {
                        ScoreDoc hit = hits[rank];
                        Document doc = searcher.doc(hit.doc);
                        
                        String docNo = doc.get("DOCNO");
                        if (docNo == null || docNo.isEmpty()) {
                            continue; 
                        }
                        
                        String trecLine = String.format(
                            "%s Q0 %s %d %.4f %s", 
                            topic.number, docNo, rank + 1, hit.score, RUN_TAG
                        );
                        writer.println(trecLine);
                        totalResultsWritten++;
                    }
                    writer.flush(); 

                } catch (ParseException e) {
                    System.err.println("Error parsing query for topic " + topic.number + ": " + e.getMessage());
                }
            }
            System.out.println("Finished searching. Wrote " + totalResultsWritten + " results.");
            System.out.println("Results saved to: " + outputRun.toAbsolutePath());
            
        } finally {
            reader.close(); 
        }
    }

    /**
     * PRF：使用伪相关反馈扩展查询
     * - 用 originalQuery 先搜 top fbDocs 文档
     * - 把这些文档的 TEXT 字段拼接起来
     * - 截断到最多 maxTokens 个词
     * - 用 textParser 解析成反馈查询，并以 prfBoost 加到原查询上
     */
    private Query buildPrfQuery(IndexSearcher searcher,
                                Query originalQuery,
                                QueryParser textParser,
                                int fbDocs,
                                float prfBoost) throws IOException, ParseException {

        // 1) 初次检索，取反馈文档
        TopDocs fbTopDocs = searcher.search(originalQuery, fbDocs);
        ScoreDoc[] fbHits = fbTopDocs.scoreDocs;

        if (fbHits.length == 0) {
            // 没有结果，直接用原始查询
            return originalQuery;
        }

        StringBuilder fbTextBuilder = new StringBuilder();
        int usedDocs = 0;

        for (ScoreDoc sd : fbHits) {
            Document d = searcher.doc(sd.doc);
            String text = d.get("TEXT"); // 字段名要和索引时一致
            if (text != null && !text.isEmpty()) {
                fbTextBuilder.append(text).append(' ');
                usedDocs++;
            }
            if (usedDocs >= fbDocs) {
                break;
            }
        }

        String fbRaw = fbTextBuilder.toString().trim();
        if (fbRaw.isEmpty()) {
            // 没有可用反馈文本，返回原始查询
            return originalQuery;
        }

        // 2) 简单截断，避免 query 过长（比如最多 300 个 token）
        String[] tokens = fbRaw.split("\\s+");
        int maxTokens = Math.min(tokens.length, 300);
        StringBuilder truncated = new StringBuilder();
        for (int i = 0; i < maxTokens; i++) {
            truncated.append(tokens[i]).append(' ');
        }

        String fbText = truncated.toString().trim();
        if (fbText.isEmpty()) {
            return originalQuery;
        }

        // 3) 解析反馈文本为查询
        Query fbQuery = textParser.parse(QueryParser.escape(fbText));

        // 4) 合并原始查询和反馈查询
        BooleanQuery.Builder prfBuilder = new BooleanQuery.Builder();
        prfBuilder.add(originalQuery, BooleanClause.Occur.SHOULD); // 原始查询
        prfBuilder.add(new BoostQuery(fbQuery, prfBoost), BooleanClause.Occur.SHOULD); // 扩展部分

        return prfBuilder.build();
    }

    /**
     * 过滤 narrative 中包含 "not relevant" / "irrelevant" 的负例句子。
     * （目前是不过滤长度，如果想进一步优化，可以只保留前 1-2 句）
     */
    private String filterNegativeNarrative(String narrative) {
    String[] sentences = narrative.split("[\\.\\;\\n]");
    StringBuilder cleanText = new StringBuilder();
    int added = 0;

    for (String raw : sentences) {
        String sentence = raw.trim();
        if (sentence.isEmpty()) continue;

        String lower = sentence.toLowerCase();
        if (lower.contains("not relevant") || lower.contains("irrelevant")) {
            continue;
        }

        cleanText.append(sentence).append(" ");
        added++;

        if (added >= 2) break; // 只保留前 2 句
    }
    return cleanText.toString().trim();
}
}

