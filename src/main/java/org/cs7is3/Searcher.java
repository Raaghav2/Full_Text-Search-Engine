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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                System.out.println(Constants.CYAN_BOLD_BRIGHT + "Classic Similarity Function used." + Constants.ANSI_RESET);
                simfn = new ClassicSimilarity();
                break;
            case BM25:
                System.out.println(Constants.CYAN_BOLD_BRIGHT + "BM25 Similarity Function used." + Constants.ANSI_RESET);
                simfn = new BM25Similarity(Constants.k, Constants.b);
                break;
            case LMD:
                System.out.println(Constants.CYAN_BOLD_BRIGHT + "LM Dirichlet Similarity Function used." + Constants.ANSI_RESET);
                colModel = new LMSimilarity.DefaultCollectionModel();
                simfn = new LMDirichletSimilarity(colModel, Constants.mu);
                break;
            case LMJ:
                System.out.println(Constants.CYAN_BOLD_BRIGHT + "LM Jelinek Mercer Similarity Function used." + Constants.ANSI_RESET);
                colModel = new LMSimilarity.DefaultCollectionModel();
                simfn = new LMJelinekMercerSimilarity(colModel, Constants.lam);
                break;
            case MULTI:
                System.out.println(Constants.CYAN_BOLD_BRIGHT + "BM25 Similarity function With LMJ Similarity Function used." + Constants.ANSI_RESET);
                simfn = new LMJelinekMercerSimilarity(colModel, Constants.lam);
                break;

            default:
                System.out.println(Constants.CYAN_BOLD_BRIGHT + "Default Similarity Function" + Constants.ANSI_RESET);
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
//                Similarity[] sims = {new BM25Similarity(), new ClassicSimilarity()};
                Similarity[] sims = {new BM25Similarity(), new LMJelinekMercerSimilarity(new LMSimilarity.DefaultCollectionModel(), Constants.lam)};
                Similarity similarityModel = new MultiSimilarity(sims);
                indexSearcher = createIndexSearcher(indexReader, similarityModel);
            }
            analyzer = Constants.ANALYZER;

            Map<String, Float> boost = createBoostMap();
            QueryParser queryParser = new QueryParser(Constants.FIELD_ALL, analyzer);
//            QueryParser queryParser = new MultiFieldQueryParser(new String[]{Constants.FIELD_ALL, Constants.HEADLINE_TEXT}, analyzer, boost);

            PrintWriter writer = new PrintWriter(Constants.searchResultFile2 + "_" + sim, "UTF-8");
            List<QueryData> loadedQueries = QueryReader.loadQueriesFromFile();
            System.out.println(Constants.CYAN_BOLD_BRIGHT + "Loading and executing queries" + Constants.ANSI_RESET);
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
                    if (relevantNarr.length() > 0) {
                        narrativeQuery = queryParser.parse(QueryParser.escape(relevantNarr));
                    }
                    if (irrelevantNarr.length() > 0) {
                        irrNarrativeQuery = queryParser.parse(QueryParser.escape(irrelevantNarr));
                    }

                    booleanQuery.add(new BoostQuery(titleQuery, (float) 6), BooleanClause.Occur.SHOULD);
                    booleanQuery.add(new BoostQuery(descriptionQuery, (float) 4.0), BooleanClause.Occur.SHOULD);

                    if (narrativeQuery != null) {
                        booleanQuery.add(new BoostQuery(narrativeQuery, (float) 2.0), BooleanClause.Occur.SHOULD);
                    }
                    if (irrNarrativeQuery != null) {
                        booleanQuery.add(new BoostQuery(irrNarrativeQuery, (float) 0.01), BooleanClause.Occur.SHOULD);
                    }
                    ScoreDoc[] hits = indexSearcher.search(booleanQuery.build(), Constants.MAX_RETURN_RESULTS).scoreDocs;
                    int n = Math.min(Constants.MAX_RETURN_RESULTS, hits.length);

                    for (int hitIndex = 0; hitIndex < n; hitIndex++) {
                        ScoreDoc hit = hits[hitIndex];
                        writer.println(queryData.getQueryNum().trim()
                                + "\tQ0\t" + indexSearcher.doc(hit.doc).get("docno")
                                + "\t" + +hitIndex
                                + "\t" + hit.score
                                + "\t" + Constants.runTag);
                    }
                }
            }

            closeIndexReader(indexReader);
            closePrintWriter(writer);
            System.out.println(Constants.CYAN_BOLD_BRIGHT + "Queries executed" + Constants.ANSI_RESET);

        } catch (IOException e) {
            System.out.println("ERROR: an error occurred when instantiating the printWriter!");
            System.out.println(String.format("ERROR MESSAGE: %s", e.getMessage()));
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

    static Map<String, Float> createBoostMap() {
        Map<String, Float> boost = new HashMap<>();
        boost.put("all", (float) 5.0);
        boost.put(Constants.HEADLINE_TEXT, (float) 8.0);
        return boost;
    }

    static IndexSearcher createIndexSearcher(IndexReader indexReader, Similarity similarityModel) {
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(similarityModel);
        return indexSearcher;
    }

    public static void main(String[] args) {
        System.out.println(Constants.CYAN_BOLD_BRIGHT + "Searching Started " + Constants.ANSI_RESET);

        try {
            String sim;
            if (args.length != 0) {
                sim = args[0].toUpperCase();
                Constants.MODELUSED = sim;
            } else {
                System.out.println("Please mention similarity to use or default similarity MULTI(BM25 + LMJ) Similarity would be used.");
                sim = Constants.MODELBM25;
                Constants.MODELUSED = Constants.MODELBM25;
            }
            executeQueries(sim);
            System.out.println(Constants.CYAN_BOLD_BRIGHT + "Searching Completed " + Constants.ANSI_RESET);
            System.out.println(Constants.CYAN_BOLD_BRIGHT + "Search results are stored at: " + Constants.searchResultFile2 + "_" + sim + Constants.ANSI_RESET);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    static void closePrintWriter(PrintWriter writer) {
        writer.flush();
        writer.close();
    }

    static void closeIndexReader(IndexReader indexReader) {
        try {
            indexReader.close();
        } catch (IOException e) {
            System.out.println("ERROR: an error occurred when closing the index from the directory!");
            System.out.println(String.format("ERROR MESSAGE: %s", e.getMessage()));
        }
    }
}

