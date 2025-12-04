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
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.MultiSimilarity;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.NormalizationH2;

import org.cs7is3.TopicParser.Topic;

public class Searcher {

    private final Analyzer analyzer = new EnglishAnalyzer();
    private static final String RUN_TAG = "CS7IS3_BM25_IB_Hybrid";

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        // 打开索引
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);

        // BM25 + IB Hybrid（使用 CombSUM：MultiSimilarity）
        BM25Similarity bm25 = new BM25Similarity(1.2f, 0.75f);
        IBSimilarity ib = new IBSimilarity(
                new DistributionLL(),   // log-logistic 分布
                new LambdaDF(),         // lambda = df/N
                new NormalizationH2()   // 频率归一化
        );
        searcher.setSimilarity(new MultiSimilarity(new org.apache.lucene.search.similarities.Similarity[]{
                bm25,
                ib
        }));

        // 查询解析（对 TEXT 字段）
        QueryParser parser = new QueryParser("TEXT", analyzer);
        parser.setSplitOnWhitespace(true);

        List<Topic> topics = new TopicParser().parse(topicsPath);
        if (outputRun.getParent() != null) {
            outputRun.getParent().toFile().mkdirs();
        }

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                try {
                    // 构建 title + description 加权查询
                    BooleanQuery.Builder qb = new BooleanQuery.Builder();

                    if (topic.title != null && !topic.title.isEmpty()) {
                        Query tq = parser.parse(QueryParser.escape(topic.title));
                        qb.add(new BoostQuery(tq, 3.0f), BooleanClause.Occur.SHOULD);
                    }

                    if (topic.description != null && !topic.description.isEmpty()) {
                        Query dq = parser.parse(QueryParser.escape(topic.description));
                        qb.add(new BoostQuery(dq, 1.3f), BooleanClause.Occur.SHOULD);
                    }

                    Query finalQuery = qb.build();

                    TopDocs results = searcher.search(finalQuery, numDocs);
                    ScoreDoc[] hits = results.scoreDocs;

                    for (int i = 0; i < hits.length; i++) {
                        Document doc = searcher.doc(hits[i].doc);
                        String docno = doc.get("DOCNO");
                        if (docno == null) {
                            docno = String.valueOf(hits[i].doc);
                        }

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
            System.out.println("Search Complete (BM25 + IB Hybrid).");
        } finally {
            reader.close();
        }
    }
}
