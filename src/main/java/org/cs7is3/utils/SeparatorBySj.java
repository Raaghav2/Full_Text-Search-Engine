package com.lucene.indexandsearch.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class SeparatorBySj {

    public static void main(String[] args) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader("/home/sujit/IdeaProjects/LuceneIndexing/data/stopwords")); FileWriter fw = new FileWriter("/home/sujit/IdeaProjects/LuceneIndexing/data/stopwordsFinal.txt")) {
            String line = br.readLine();
            String[] parts = line.split(",");

            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
                parts[i] = parts[i].substring(1, parts[i].length() - 1);
                fw.write(parts[i]);
                fw.write(System.lineSeparator());
            }

        }

    }
}
