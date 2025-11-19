package org.cs7is3;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.util.Arrays;
import java.util.List;

public class SMJAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(String s) {
        // Use StandardTokenizer instead of ClassicTokenizer
        Tokenizer stdTokenizer = new StandardTokenizer();

        // Use StandardFilter instead of ClassicFilter
        TokenStream tokenStream = new StandardFilter(stdTokenizer);

        // ASCIIFolding for accents
        tokenStream = new ASCIIFoldingFilter(tokenStream);

        // Hardcoded stopwords
        List<String> stopWordList = Arrays.asList(
                "a", "about", "above", "after", "again", "against", "all", "am",
                "an", "and", "any", "are", "as", "at", "be", "because", "been",
                "before", "being", "below", "between", "both", "but", "by", "could",
                "did", "do", "does", "doing", "down", "during", "each", "few",
                "for", "from", "further", "had", "has", "have", "having", "he",
                "her", "here", "hers", "herself", "him", "himself", "his", "how",
                "i", "if", "in", "into", "is", "it", "its", "itself", "let's",
                "me", "more", "most", "my", "myself", "nor", "of", "on", "once",
                "only", "or", "other", "ought", "our", "ours", "ourselves", "out",
                "over", "own", "same", "she", "should", "so", "some", "such",
                "than", "that", "the", "their", "theirs", "them", "themselves",
                "then", "there", "these", "they", "this", "those", "through",
                "to", "too", "under", "until", "up", "very", "was", "we", "were",
                "what", "when", "where", "which", "while", "who", "whom", "why",
                "with", "would", "you", "your", "yours", "yourself", "yourselves"
        );

        CharArraySet stopWordSet = new CharArraySet(stopWordList, true);

        // Apply lowercase
        tokenStream = new LowerCaseFilter(tokenStream);

        // Apply stopwords
        tokenStream = new StopFilter(tokenStream, stopWordSet);

        // Apply stemming
        tokenStream = new KStemFilter(tokenStream);
        tokenStream = new PorterStemFilter(tokenStream);

        return new TokenStreamComponents(stdTokenizer, tokenStream);
    }
}
