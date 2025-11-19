package org.cs7is3;

import org.cs7is3.query.QueryData;
import org.cs7is3.query.QueryReader;
import org.cs7is3.utils.Constants;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.*;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.BreakIterator;
import java.util.*;

import static org.cs7is3.Searcher.SimModel.BM25;

public class Searcher {

    protected static Similarity simfn;
    protected IndexReader reader;
    protected static Analyzer analyzer;
    protected QueryParser parser;
    protected static LMSimilarity.CollectionModel colModel;

    protected enum SimModel {
        CLASSIC, BM25, LMD, LMJ, MULTI
    }

    protected static Searcher.SimModel sim;

    private static void setSim(String val) {
        try {
            sim = Searcher.SimModel.valueOf(val);
        } catch (Exception e) {
            System.out.println("Similarity Function Not Recognized - Setting to Default");
            System.out.println("Possible Similarity Functions are:");
            for (Searcher.SimModel value : Searcher.SimModel.values()) {
                System.out.println("<MODELBM25>" + value.name() + "</MODELBM25>");
            }
            sim = BM25;
        }
    }

    public static void selectSimilarityFunction(Searcher.SimModel sim) {
        colModel = null;
        switch (sim) {

            case CLASSIC:
                simfn = new ClassicSimilarity();
                break;

            case BM25:
                simfn = new BM25Similarity(Constants.k, Constants.b);
                break;

            case LMD:
                colModel = new LMSimilarity.DefaultCollectionModel();
                simfn = new LMDirichletSimilarity(colModel, Constants.mu);
                break;

            case LMJ:
                colModel = new LMSimilarity.DefaultCollectionModel();
                simfn = new LMJelinekMercerSimilarity(colModel, Constants.lam);
                break;

            case MULTI:
                simfn = new LMJelinekMercerSimilarity(new LMSimilarity.DefaultCollectionModel(), Constants.lam);
                break;

            default:
                simfn = new BM25Similarity();
                break;
        }
    }

    public static void setParams(String similarityToUse) {
        setSim(similarityToUse.toUpperCase());
        analyzer = Constants.ANALYZER;
    }

    private static void executeQueries(String similarity) throws ParseException {

        try {
            IndexReader indexReader = DirectoryReader.open(FSDirectory.open(new File(Constants.INDEXPATH).toPath()));
            setParams(similarity);
            selectSimilarityFunction(sim);
            IndexSearcher indexSearcher = createIndexSearcher(indexReader, simfn);

            if (similarity.equalsIgnoreCase(Constants.MODELMULTI)) {
                Similarity[] sims = {
                        new BM25Similarity(),
                        new LMJelinekMercerSimilarity(new LMSimilarity.DefaultCollectionModel(), Constants.lam)
                };
                indexSearcher = createIndexSearcher(indexReader, new MultiSimilarity(sims));
            }

            analyzer = Constants.ANALYZER;

            QueryParser queryParser = new QueryParser(Constants.FIELD_ALL, analyzer);

            PrintWriter writer = new PrintWriter(Constants.searchResultFile2 + "_" + sim, "UTF-8");

            List<QueryData> loadedQueries = QueryReader.loadQueriesFromFile();
            int ct = 0;

            for (QueryData queryData : loadedQueries) {

                List<String> splitNarrative = splitNarrIntoRelNotRel(queryData.getNarrative());
                String relevantNarr = splitNarrative.get(0).trim();
                String irrelevantNarr = splitNarrative.get(1).trim();

                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();

                if (queryData.getTitle().length() > 0) {

                    Query titleQuery = queryParser.parse(QueryParser.escape(queryData.getTitle()));
                    Query descriptionQuery = queryParser.parse(QueryParser.escape(queryData.getDescription()));

                    Query narrativeQuery = null;
                    Query irrNarrativeQuery = null;

                    if (relevantNarr.length() > 0)
                        narrativeQuery = queryParser.parse(QueryParser.escape(relevantNarr));

                    if (irrelevantNarr.length() > 0)
                        irrNarrativeQuery = queryParser.parse(QueryParser.escape(irrelevantNarr));

                    booleanQuery.add(new BoostQuery(titleQuery, 6f), BooleanClause.Occur.SHOULD);
                    booleanQuery.add(new BoostQuery(descriptionQuery, 4f), BooleanClause.Occur.SHOULD);

                    if (narrativeQuery != null)
                        booleanQuery.add(new BoostQuery(narrativeQuery, 2f), BooleanClause.Occur.SHOULD);

                    if (irrNarrativeQuery != null)
                        booleanQuery.add(new BoostQuery(irrNarrativeQuery, 0.01f), BooleanClause.Occur.SHOULD);

                    ScoreDoc[] hits = indexSearcher.search(booleanQuery.build(), Constants.MAX_RETURN_RESULTS).scoreDocs;

                    int n = Math.min(Constants.MAX_RETURN_RESULTS, hits.length);

                    for (int hitIndex = 0; hitIndex < n; hitIndex++) {
                        ScoreDoc hit = hits[hitIndex];
                        writer.println(queryData.getQueryNum().trim()
                                + "\tQ0\t" + indexSearcher.doc(hit.doc).get("docno")
                                + "\t" + hitIndex
                                + "\t" + hit.score
                                + "\t" + Constants.runTag);
                    }
                }
            }

            closeIndexReader(indexReader);
            closePrintWriter(writer);

        } catch (IOException e) {
            System.out.println("ERROR writing results: " + e.getMessage());
        }
    }

    private static List<String> splitNarrIntoRelNotRel(String narrative) {
        StringBuilder relevantNarr = new StringBuilder();
        StringBuilder irrelevantNarr = new StringBuilder();
        List<String> splitNarrative = new ArrayList<>();

        BreakIterator bi = BreakIterator.getSentenceInstance();
        bi.setText(narrative);
        int index = 0;

        while (bi.next() != BreakIterator.DONE) {
            String sentence = narrative.substring(index, bi.current());

            if (!sentence.contains("not relevant") && !sentence.contains("irrelevant")) {
                relevantNarr.append(sentence.replaceAll(
                        "a relevant document identifies|a relevant document could|a relevant document may|a relevant document must|a relevant document will|a document will|to be relevant|relevant documents|a document must|relevant|will contain|will discuss|will provide|must cite",
                        ""));
            } else {
                irrelevantNarr.append(sentence.replaceAll("are also not relevant|are not relevant|are irrelevant|is not relevant|not|NOT", ""));
            }

            index = bi.current();
        }

        splitNarrative.add(relevantNarr.toString());
        splitNarrative.add(irrelevantNarr.toString());

        return splitNarrative;
    }

    static IndexSearcher createIndexSearcher(IndexReader indexReader, Similarity similarityModel) {
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(similarityModel);
        return indexSearcher;
    }

    static void closePrintWriter(PrintWriter writer) {
        writer.flush();
        writer.close();
    }

    static void closeIndexReader(IndexReader indexReader) {
        try {
            indexReader.close();
        } catch (IOException e) {
            System.out.println("ERROR closing index: " + e.getMessage());
        }
    }


    public static void searchTopics(java.nio.file.Path indexPath,
                                    java.nio.file.Path topicsPath,
                                    java.nio.file.Path outputPath,
                                    int topK) {

        try {

            executeQueries(Constants.MODELBM25);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {

        try {
            String sim;
            if (args.length != 0) {
                sim = args[0].toUpperCase();
                Constants.MODELUSED = sim;
            } else {
                sim = Constants.MODELBM25;
                Constants.MODELUSED = Constants.MODELBM25;
            }

            executeQueries(sim);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
