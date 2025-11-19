package org.cs7is3;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.queryparser.classic.QueryParser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

    private static final boolean USE_LM_DIRICHLET = true;
    private static final String RUN_TAG = "qiming-lmd";

    public void searchTopics(Path indexPath, Path topicsPath,
                             Path outputRun, int numDocs) throws IOException {

        Directory dir = FSDirectory.open(indexPath);
        try (IndexReader reader = DirectoryReader.open(dir);
             BufferedWriter writer = Files.newBufferedWriter(outputRun, StandardCharsets.UTF_8)) {

            IndexSearcher searcher = new IndexSearcher(reader);

            if (USE_LM_DIRICHLET) {
                searcher.setSimilarity(new LMDirichletSimilarity(2000f));
            } else {
                searcher.setSimilarity(new BM25Similarity());
            }

            Analyzer analyzer = new StandardAnalyzer();

            List<Topic> topics = parseTopics(topicsPath);

            for (Topic topic : topics) {
                try {
                    Query query = buildBoostedQuery(topic, analyzer);
                    TopDocs topDocs = searcher.search(query, numDocs);

                    ScoreDoc[] hits = topDocs.scoreDocs;
                    int limit = Math.min(numDocs, hits.length);

                    for (int rank = 0; rank < limit; rank++) {
                        ScoreDoc sd = hits[rank];
                        Document doc = searcher.doc(sd.doc);
                        String docno = doc.get("docno");
                        if (docno == null) {
                            continue;
                        }

                        // topic_id Q0 docno rank score run_tag
                        writer.write(String.format(
                                "%s Q0 %s %d %f %s%n",
                                topic.id,
                                docno,
                                rank + 1,
                                sd.score,
                                RUN_TAG
                        ));
                    }

                } catch (Exception e) {
                    System.err.println("Error searching topic " + topic.id);
                    e.printStackTrace();
                }
            }
        }
    }

    private static class Topic {
        String id;
        String title;
        String description;
        String narrative;
    }

    private List<Topic> parseTopics(Path topicsPath) throws IOException {
        List<String> lines = Files.readAllLines(topicsPath, StandardCharsets.UTF_8);
        List<Topic> topics = new ArrayList<>();

        Topic current = null;
        StringBuilder descBuf = null;
        StringBuilder narrBuf = null;
        boolean inDesc = false;
        boolean inNarr = false;

        for (String line : lines) {
            String trim = line.trim();

            if (trim.equalsIgnoreCase("<top>")) {
                current = new Topic();
                descBuf = new StringBuilder();
                narrBuf = new StringBuilder();
                inDesc = false;
                inNarr = false;

            } else if (trim.equalsIgnoreCase("</top>")) {
                if (current != null) {
                    current.description = descBuf.toString().trim();
                    current.narrative = narrBuf.toString().trim();
                    topics.add(current);
                }
                current = null;

            } else if (current != null) {
                if (trim.startsWith("<num>")) {
                    // e.g. "<num> Number: 401"
                    String id = trim.replaceAll("<.*?>", "")
                            .replace("Number:", "")
                            .trim();
                    current.id = id;

                } else if (trim.startsWith("<title>")) {
                    current.title = trim.replace("<title>", "").trim();

                } else if (trim.startsWith("<desc>")) {
                    inDesc = true;
                    inNarr = false;

                } else if (trim.startsWith("<narr>")) {
                    inNarr = true;
                    inDesc = false;

                } else {
                    if (inDesc) {
                        descBuf.append(line).append(' ');
                    } else if (inNarr) {
                        narrBuf.append(line).append(' ');
                    }
                }
            }
        }

        return topics;
    }

    private Query buildBoostedQuery(Topic topic, Analyzer analyzer) throws Exception {
        QueryParser parser = new QueryParser("contents", analyzer);

        String title = topic.title != null ? topic.title : "";
        String desc  = topic.description != null ? topic.description : "";
        String narr  = topic.narrative != null ? topic.narrative : "";

        Query titleQuery = parser.parse(QueryParser.escape(title));
        Query descQuery  = parser.parse(QueryParser.escape(desc));
        Query narrQuery  = parser.parse(QueryParser.escape(narr));

        Query boostedTitle = new BoostQuery(titleQuery, 2.0f);
        Query boostedDesc  = new BoostQuery(descQuery,  1.5f);
        Query boostedNarr  = new BoostQuery(narrQuery,  1.0f);

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(boostedTitle, BooleanClause.Occur.SHOULD);
        builder.add(boostedDesc,  BooleanClause.Occur.SHOULD);
        builder.add(boostedNarr,  BooleanClause.Occur.SHOULD);

        return builder.build();
    }
}
