package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import org.cs7is3.TopicParser.Topic;

public class Searcher {

    private final Analyzer analyzer;
    private static final String RUN_TAG = "CS7IS3_A1_Standard_BM25";

    public Searcher() {
        this.analyzer = new StandardAnalyzer();
    }

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new BM25Similarity());

        QueryParser parser = new QueryParser("TEXT", analyzer);
        parser.setSplitOnWhitespace(false);

        List<Topic> topics = new TopicParser().parse(topicsPath);

        if (outputRun.getParent() != null) {
            outputRun.getParent().toFile().mkdirs();
        }

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {
                try {
                    StringBuilder qText = new StringBuilder();
                    if (topic.title != null && !topic.title.isEmpty()) {
                        qText.append(topic.title.trim()).append(" ");
                    }
                    if (topic.description != null && !topic.description.isEmpty()) {
                        qText.append(topic.description.trim());
                    }

                    String queryString = qText.toString().trim();
                    if (queryString.isEmpty()) {
                        continue;
                    }

                    Query query = parser.parse(QueryParser.escape(queryString));
                    TopDocs results = searcher.search(query, numDocs);
                    ScoreDoc[] hits = results.scoreDocs;

                    for (int i = 0; i < hits.length; i++) {
                        Document doc = searcher.doc(hits[i].doc);
                        String docNo = doc.get("DOCNO");
                        if (docNo == null) {
                            docNo = String.valueOf(hits[i].doc);
                        }

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
                    System.err.println("Error processing topic: " + topic.number);
                    e.printStackTrace();
                }
            }
            System.out.println("Search Complete (A1 - StandardAnalyzer + BM25).");
        } finally {
            reader.close();
        }
    }
}
