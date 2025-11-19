package org.cs7is3;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter; 
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class CustomAnalyzer extends Analyzer {

    // Use standard English stop words (the, a, an, is, etc.)
    private static final CharArraySet STOP_WORDS_SET = EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {

        Tokenizer source = new StandardTokenizer();


        TokenStream result = new EnglishPossessiveFilter(source);


        result = new LowerCaseFilter(result);
        
        result = new ASCIIFoldingFilter(result);

        result = new StopFilter(result, STOP_WORDS_SET);


        result = new KStemFilter(result);

        return new TokenStreamComponents(source, result);
    }
}