package com.skywaet.hadoop.h3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.regex.Pattern;

public class WordMapper extends Mapper<Object, Text, Text, Text> {

    private static final Pattern IS_WORD = Pattern.compile("^([^_\\W]+-*)+$",
            Pattern.UNICODE_CHARACTER_CLASS);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        CharArraySet englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        CharArraySet russianStopWords = RussianAnalyzer.getDefaultStopSet();
        CharArraySet germanStopWords = GermanAnalyzer.getDefaultStopSet();

        CharArraySet stopWords = new CharArraySet(englishStopWords, true);
        stopWords.addAll(englishStopWords);
        stopWords.addAll(frenchStopWords);
        stopWords.addAll(russianStopWords);
        stopWords.addAll(germanStopWords);


        CustomAnalyzer analyzer = new CustomAnalyzer(stopWords);

        TokenStream tokenStream = new StopFilter(analyzer.tokenStream("fieldName", value.toString()),
                stopWords);

        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
        String prev = "";
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            String curr = attr.toString();
            if (!prev.isEmpty() && IS_WORD.matcher(prev).matches() && IS_WORD.matcher(curr).matches()) {
                context.write(new Text(prev.toLowerCase()), new Text(curr.toLowerCase()));
            }
            prev = curr;
        }
    }
}
