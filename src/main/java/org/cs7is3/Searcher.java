package org.cs7is3;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;

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

import org.apache.lucene.search.similarities.LMDirichletSimilarity;

public class Searcher {

    private final Analyzer analyzer = new CustomAnalyzer();

    private static final String RUN_TAG = "CS7IS3_LMDirichlet_KStem_PRF";
    private static final int DEFAULT_NUM_DOCS = 1000;
    private static final int FB_DOCS = 10;
    private static final float PRF_BOOST = 0.5f;

    public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {

        if (numDocs <= 0) {
            numDocs = DEFAULT_NUM_DOCS;
        }

        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher searcher = new IndexSearcher(reader);

        searcher.setSimilarity(new LMDirichletSimilarity());

        QueryParser titleParser = new QueryParser("TITLE", analyzer);
        titleParser.setSplitOnWhitespace(true);
        titleParser.setAutoGeneratePhraseQueries(true);
        titleParser.setPhraseSlop(2);

        QueryParser textParser = new QueryParser("TEXT", analyzer);
        textParser.setAutoGeneratePhraseQueries(false);
        textParser.setPhraseSlop(8);

        TopicParser topicParser = new TopicParser();
        List<Topic> topics = topicParser.parse(topicsPath);

        int totalResultsWritten = 0;

        if (outputRun.getParent() != null) {
            outputRun.getParent().toFile().mkdirs();
        }

        try (PrintWriter writer = new PrintWriter(outputRun.toFile())) {
            for (Topic topic : topics) {

                BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

                try {
                    if (topic.title != null && !topic.title.isEmpty()) {
                        Query titleQuery =
                                titleParser.parse(QueryParser.escape(topic.title));
                        queryBuilder.add(
                                new BoostQuery(titleQuery, 3.5f),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    if (topic.description != null && !topic.description.isEmpty()) {
                        Query descQuery =
                                textParser.parse(QueryParser.escape(topic.description));
                        queryBuilder.add(
                                new BoostQuery(descQuery, 1.7f),
                                BooleanClause.Occur.SHOULD
                        );
                    }

                    if (topic.narrative != null && !topic.narrative.isEmpty()) {
                        String cleanNarrative = filterNegativeNarrative(topic.narrative);
                        if (!cleanNarrative.trim().isEmpty()) {
                            Query narrQuery =
                                    textParser.parse(QueryParser.escape(cleanNarrative));
                            queryBuilder.add(
                                    narrQuery,
                                    BooleanClause.Occur.SHOULD
                            );
                        }
                    }

                    BooleanQuery originalQuery = queryBuilder.build();

                    if (originalQuery.clauses().isEmpty()) {
                        System.err.println("Empty query for topic " + topic.number + ", skipping.");
                        continue;
                    }

                    Query finalQuery;
                    try {
                        finalQuery = buildPrfQuery(
                                searcher,
                                originalQuery,
                                textParser,
                                FB_DOCS,
                                PRF_BOOST
                        );
                    } catch (ParseException e) {
                        System.err.println("PRF parse error for topic "
                                + topic.number + ": " + e.getMessage());
                        finalQuery = originalQuery;
                    }

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
                                topic.number,
                                docNo,
                                rank + 1,
                                hit.score,
                                RUN_TAG
                        );
                        writer.println(trecLine);
                        totalResultsWritten++;
                    }

                    writer.flush();

                } catch (ParseException e) {
                    System.err.println("Error parsing query for topic "
                            + topic.number + ": " + e.getMessage());
                }
            }

            System.out.println("Finished searching. Wrote "
                    + totalResultsWritten + " results.");
            System.out.println("Results saved to: " + outputRun.toAbsolutePath());

        } finally {
            reader.close();
        }
    }

    private Query buildPrfQuery(IndexSearcher searcher,
                                Query originalQuery,
                                QueryParser textParser,
                                int fbDocs,
                                float prfBoost) throws IOException, ParseException {

        TopDocs fbTopDocs = searcher.search(originalQuery, fbDocs);
        ScoreDoc[] fbHits = fbTopDocs.scoreDocs;

        if (fbHits.length == 0) {
            return originalQuery;
        }

        StringBuilder fbTextBuilder = new StringBuilder();
        int usedDocs = 0;

        for (ScoreDoc sd : fbHits) {
            Document d = searcher.doc(sd.doc);
            String text = d.get("TEXT");
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
            return originalQuery;
        }

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

        Query fbQuery = textParser.parse(QueryParser.escape(fbText));

        BooleanQuery.Builder prfBuilder = new BooleanQuery.Builder();
        prfBuilder.add(originalQuery, BooleanClause.Occur.SHOULD);
        prfBuilder.add(
                new BoostQuery(fbQuery, prfBoost),
                BooleanClause.Occur.SHOULD
        );

        return prfBuilder.build();
    }

    private String filterNegativeNarrative(String narrative) {
        String[] sentences = narrative.split("[\\.\\;\\n]");
        StringBuilder cleanText = new StringBuilder();
        int added = 0;

        for (String raw : sentences) {
            String sentence = raw.trim();
            if (sentence.isEmpty()) {
                continue;
            }

            String lower = sentence.toLowerCase();
            if (lower.contains("not relevant") || lower.contains("irrelevant")) {
                continue;
            }

            cleanText.append(sentence).append(" ");
            added++;

            if (added >= 2) {
                break;
            }
        }
        return cleanText.toString().trim();
    }
}

