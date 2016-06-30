package com.test;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;

import java.io.IOException;

/**
 * Created by wangqi08 on 28/6/2016.
 */
public class CJKFilter extends StopwordAnalyzerBase {

    /**
     * File containing default CJK stopwords.
     * <p>
     * Currently it contains some common English words that are not usually
     * useful for searching and some double-byte interpunctions.
     */
    public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";

    /**
     * Returns an unmodifiable instance of the default stop-words set.
     *
     * @return an unmodifiable instance of the default stop-words set.
     */
    public static CharArraySet getDefaultStopSet() {
        return DefaultSetHolder.DEFAULT_STOP_SET;
    }

    private static class DefaultSetHolder {
        static final CharArraySet DEFAULT_STOP_SET;

        static {
            try {
                DEFAULT_STOP_SET = loadStopwordSet(false, CJKAnalyzer.class, DEFAULT_STOPWORD_FILE, "#");
            } catch (IOException ex) {
                // default set should always be present as it is part of the
                // distribution (JAR)
                throw new RuntimeException("Unable to load default stopword set");
            }
        }
    }

    /**
     * Builds an analyzer which removes words in {@link #getDefaultStopSet()}.
     */
    public CJKFilter() {
        this(DefaultSetHolder.DEFAULT_STOP_SET);
    }

    /**
     * Builds an analyzer with the given stop words
     *
     * @param stopwords a stopword set
     */
    public CJKFilter(CharArraySet stopwords) {
        super(stopwords);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new StandardTokenizer1();
        TokenStream tok = new StandardFilter(source);
        return new TokenStreamComponents(source, new StopFilter(tok, stopwords));
    }
}
