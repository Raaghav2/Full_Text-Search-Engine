package org.cs7is3.Parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

public class LATimesParser {
    public static String[] readmeFiles = {"readchg.txt", "readmela.txt"};
    public static ArrayList<Document> LATimesdocuments = new ArrayList<>();

    public static ArrayList<Document> parseLATimes(String filesPath) throws IOException {
        LATimesdocuments.clear();

        Path dirPath = Paths.get(filesPath);
        if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
            System.err.println("LATimesParser: directory does not exist or is not a directory: " + dirPath.toAbsolutePath());
            return LATimesdocuments;
        }

        ArrayList<Path> entries = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dirPath)) {
            for (Path p : ds) {
                entries.add(p);
            }
        }

        System.out.println("LATimesParser: listing directory '" + filesPath + "' -> " + entries.size() + " entries");

        for (Path p : entries) {
            String latimes = p.getFileName().toString();
            System.out.println("LATimesParser: considering file: '" + latimes + "'");
            if (!latimes.equals(readmeFiles[0]) && !latimes.equals(readmeFiles[1])) {
                java.io.File file = p.toFile();
                System.out.println("LATimesParser: file exists=" + file.exists() + ", isFile=" + file.isFile() + ", size=" + (file.exists() ? file.length() : 0));
                int before = LATimesdocuments.size();
                try (BufferedReader localBr = new BufferedReader(new FileReader(file))) {
                    processStream(localBr);
                }
                int after = LATimesdocuments.size();
                System.out.println("LATimesParser: parsed " + (after - before) + " documents from '" + latimes + "'");
            }
        }
        return LATimesdocuments;
    }

    // Read the file line-by-line and extract fields without building whole document text
    private static void processStream(BufferedReader reader) throws IOException {
        String line;
        boolean inDoc = false;
        boolean inDocNo = false;
        boolean inHeadline = false;
        boolean inText = false;

        StringBuilder docNoSb = new StringBuilder();
        StringBuilder headSb = new StringBuilder();
        StringBuilder textSb = new StringBuilder();

        final int MAX_FIELD = 200_000; // per-field cap

        while ((line = reader.readLine()) != null) {
            if (!inDoc && line.contains("<DOC>")) {
                inDoc = true;
                docNoSb.setLength(0);
                headSb.setLength(0);
                textSb.setLength(0);
                inDocNo = inHeadline = inText = false;
            }

            if (inDoc) {
                // DOCNO
                if (!inDocNo && line.contains("<DOCNO>")) {
                    int start = line.indexOf("<DOCNO>") + "<DOCNO>".length();
                    if (line.contains("</DOCNO>")) {
                        int end = line.indexOf("</DOCNO>", start);
                        appendWithCap(docNoSb, line.substring(start, end), MAX_FIELD);
                    } else {
                        appendWithCap(docNoSb, line.substring(start), MAX_FIELD);
                        inDocNo = true;
                    }
                } else if (inDocNo) {
                    if (line.contains("</DOCNO>")) {
                        int end = line.indexOf("</DOCNO>");
                        appendWithCap(docNoSb, line.substring(0, end), MAX_FIELD);
                        inDocNo = false;
                    } else {
                        appendWithCap(docNoSb, line, MAX_FIELD);
                    }
                }

                // HEADLINE
                if (!inHeadline && line.contains("<HEADLINE>")) {
                    int start = line.indexOf("<HEADLINE>") + "<HEADLINE>".length();
                    if (line.contains("</HEADLINE>")) {
                        int end = line.indexOf("</HEADLINE>", start);
                        appendWithCap(headSb, line.substring(start, end), MAX_FIELD);
                    } else {
                        appendWithCap(headSb, line.substring(start), MAX_FIELD);
                        inHeadline = true;
                    }
                } else if (inHeadline) {
                    if (line.contains("</HEADLINE>")) {
                        int end = line.indexOf("</HEADLINE>");
                        appendWithCap(headSb, line.substring(0, end), MAX_FIELD);
                        inHeadline = false;
                    } else {
                        appendWithCap(headSb, line, MAX_FIELD);
                    }
                }

                // TEXT
                if (!inText && line.contains("<TEXT>")) {
                    int start = line.indexOf("<TEXT>") + "<TEXT>".length();
                    if (line.contains("</TEXT>")) {
                        int end = line.indexOf("</TEXT>", start);
                        appendWithCap(textSb, line.substring(start, end), MAX_FIELD);
                    } else {
                        appendWithCap(textSb, line.substring(start), MAX_FIELD);
                        inText = true;
                    }
                } else if (inText) {
                    if (line.contains("</TEXT>")) {
                        int end = line.indexOf("</TEXT>");
                        appendWithCap(textSb, line.substring(0, end), MAX_FIELD);
                        inText = false;
                    } else {
                        appendWithCap(textSb, line, MAX_FIELD);
                    }
                }

                // end of doc -> emit document
                if (line.contains("</DOC>")) {
                    String docNo = collapseAndTrim(docNoSb);
                    String title = collapseAndTrim(headSb);
                    String fullText = collapseAndTrim(textSb);
                    if (!docNo.isEmpty()) {
                        Document doc = new Document();
                        doc.add(new StringField("DOCNO", docNo, Store.YES));
                        doc.add(new TextField("TITLE", title, Store.YES));
                        doc.add(new TextField("TEXT", fullText, Store.YES));
                        LATimesdocuments.add(doc);
                    }
                    inDoc = false;
                    docNoSb.setLength(0);
                    headSb.setLength(0);
                    textSb.setLength(0);
                    inDocNo = inHeadline = inText = false;
                }
            }
        }
    }

    private static void appendWithCap(StringBuilder sb, String s, int cap) {
        if (s == null || s.isEmpty()) return;
        if (sb.length() >= cap) return;
        int toAdd = Math.min(s.length(), cap - sb.length());
        sb.append(s, 0, toAdd);
        if (sb.length() >= cap && toAdd < s.length()) {
            sb.append(" [TRUNCATED]");
        }
    }

    private static String collapseAndTrim(StringBuilder sb) {
        if (sb.length() == 0) return "";
        String raw = sb.toString();
        StringBuilder out = new StringBuilder(Math.min(raw.length(), 1024));
        boolean lastWasSpace = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isWhitespace(c)) {
                if (!lastWasSpace) {
                    out.append(' ');
                    lastWasSpace = true;
                }
            } else {
                out.append(c);
                lastWasSpace = false;
            }
        }
        return out.toString().trim();
    }
}