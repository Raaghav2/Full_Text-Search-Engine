package org.cs7is3.utils;

import org.cs7is3.SMJAnalyzer;
import org.apache.lucene.analysis.Analyzer;

public class Constants {

    // Common analyzer
    public static final Analyzer ANALYZER = new SMJAnalyzer();

    public static final String FIELD_ALL = "all";
    public static String precisionRecallGraphImagePath = "data/results/PRGraph";
    public static final String searchResultFile = "data/results/query_results";
    public static final String trecEvalPath = "data/trec_eval-9.0.7/trec_eval ";
    public static final String trecEvalRelevancePath = "data/qrels.assignment2.part1";
    public static final String trecEvalCommand = trecEvalPath + trecEvalRelevancePath + " " + searchResultFile;
    public static final String trecEvalResultFile = "data/results/TrecEval_Result";
    public static final String TXTExtension = ".txt";
    public static final float k = 1.2f;
    public static final float b = 0.75f;
    public static final float lam = 0.5f;
    public static final float beta = 500f;
    public static final float mu = 500f;
    public static final float c = 10f;
    public static final float delta = 1.0f;
    public static final String runTag = "bm25";

    public static final String MODELBM25 = "BM25";
    public static final String MODELCLASSIC = "CLASSIC";
    public static final String MODELLMJ = "LMJ";
    public static final String MODELLMD = "LMD";
    public static final String MODELMULTI = "MULTI";

    public static String MODELUSED = "";

    public static final int lengthFilterMinimumLength = 3;
    public static final int lengthFilterMaximumLength = 25;
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String CYAN_BOLD_BRIGHT = "\033[1;96m";
    public static final String ANSI_RED_BACKGROUND = "\u001B[41m";

    //Dataset file path
    public static final String FBISFILESPATH = "rawdata/fbis";
    public static final String LATIMES_FILESPATH = "rawdata/latimes";
    public static final String FR94FILESPATH = "rawdata/fr94";
    public static final String FT_FILESPATH = "rawdata/ft";

    //Index types
    public static final String FBISINDEXTYPE = "fbis";
    public static final String LATTIMESINDEXTYPE = "lattimes";
    public static final String FR94INDEXTYPE = "fr94";
    public static final String FTINDEXTYPE = "ft";

    //indexing tags
    public static final String FIELD_TEXT = "text";
    public static final String DOCNO_TEXT = "docno";
    public static final String HEADLINE_TEXT = "headline";
    public static final String BYLINE_TEXT = "byline";
    public static final String GRAPHIC_TEXT = "graphic";
    public static final String SECTION_TEXT = "section";
    public static final String PARENT_TEXT = "parent";
    public static final String DOCTITLE_TEXT = "doctitle";
    public static final String USDEPT_TEXT = "usdept";
    public static final String USBUREAU_TEXT = "usbureau";
    public static final String AGENCY_TEXT = "agency";
    public static final String ADDRESS_TEXT = "address";
    public static final String FURTHER_TEXT = "further";
    public static final String SUMMARY_TEXT = "summary";
    public static final String ACTION_TEXT = "action";
    public static final String SIGNER_TEXT = "signer";
    public static final String SIGNJOB_TEXT = "signjob";
    public static final String SUPPLEM_TEXT = "supplem";
    public static final String BILLING_TEXT = "billing";
    public static final String FRFILING_TEXT = "frfiling";
    public static final String CFRNO_TEXT = "cfrno";
    public static final String RINDOCK_TEXT = "rindock";
    public static final String TABLE_TEXT = "table";
    public static final String FOOTNOTE_TEXT = "footnote";
    public static final String FOOTCITE_TEXT = "footcite";
    public static final String FOOTNAME_TEXT = "footname";
    public static final String PROFILE_TEXT = "profile";
    public static final String PUB_TEXT = "pub";
    public static final String PAGE_TEXT = "page";
    public static final String MISC = "misc";
    public static final String DATE_TEXT = "date";
    public static final int MAX_RETURN_RESULTS = 1000;
    public static final String ITER_NUM = " 0 ";
    public static final String searchResultFile2 = "data/results/query_results";
    public static final String EXECUTION_TIME = " >> Execution Time in minutes: ";
    public static final String INDEXPATH = "index";
    public static final String QUERYFILEPATH = "rawdata/topics";

    // For input taking
    // Colors
    public static final String CYELW_START = "\033[32;1;2m";
    public static final String CYELW_END = "\033[0m";
    public static final String CGRN_START = "\033[34m";
    public static final String CGRN_END = "\033[0m";
    public static final String RED_START = "\033[31;1m";
    public static final String RED_END = "\033[0m";

    //Formatters
    public static final String FORMATTER = "\n ... \n";
    public static final String NEW_LINE = "\n";
    public static final String TAB = "\t";
    //Group Details
    public static final String GRPUPNAME = "\t\t\t\t\t\tBOOSTERS : JUST BOOST YOUR SEARCH!!\n";
    public static final String NAME1 = "Name: Rohan Dilip Bagwe\t\t";
    public static final String TCDID1 = "TCDID: 19314431\t\t";

    public static final String NAME2 = "Name: Sujit Jadhav\t\t";
    public static final String TCDID2 = "TCDID: 19310363\t\t";

    public static final String NAME3 = "Name: Chetan Prasad\t\t";
    public static final String TCDID3 = "TCDID: 19308180\t\t";

    public static final String NAME4 = "Name: Jiawen Lin\t\t";
    public static final String TCDID4 = "TCDID: 19309750\t\t";
    //Course Details
    public static final String COURSE = "COURSE: CS7IS3-A-SEM202-201920: INFORMATION RETRIEVAL AND WEB SEARCH\n";
    public static final String ASMNT = "Assignment 2 - Lucene and News paper publications\n";
    public static final String INVALID_CHOICE = "Please enter correct option.";
    public static final String OUT_OF_RANGE = "Option out of range!";
    public static final String SIMILARITY = "Please Select Similarity: ";
    public static final String ENTER_YOUR_CHOICE = "Enter your choice: ";
    public static final String CHOICE_1 = "1. INDEX USING CUSTOM ANALYZER AND SEARCH USING BM25 SIMILARITY";
    public static final String CHOICE_2 = "2. SEARCHING WITH SIMILARITY OPTIONS";
    public static final String CHOICE_3 = "3. EXIT";

    public static final String CHOOSE_SIMILARITY = "CHOOSE SIMILARITY";
    public static final String CHOICE_BM25 = "1. BM25 SIMILARITY";
    public static final String CHOICE_MULTI = "2. MULTI SIMILARITY";
    public static final String CHOICE_LMJ = "3. LMJ SIMILARITY";
    public static final String CHOICE_LMD = "4. LMD SIMILARITY";
    public static final String CHOICE_CLASSIC = "5. CLASSIC SIMILARITY";

    public static final String DEFAULT_SIMILARITY = "SELECTED DEFAULT SIMILARITY: BM25";
    public static final String DEFAULT_CHOICE = "DEFAULT CHOICE SELECTED -> SEARCHING WITH BM25 SIMILARITY";
}

