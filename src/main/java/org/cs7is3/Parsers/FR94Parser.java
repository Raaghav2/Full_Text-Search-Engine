package org.cs7is3.Parsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

public class FR94Parser {
    public static String[] readmeFiles = {"readchg.txt", "readmefr.txt"};
    public static BufferedReader br;
    public static ArrayList<Document> FR94documents = new ArrayList<>();

    private static String extractTagContent(String source, String startTag, String endTag) {
        int startIndex = source.indexOf(startTag);
        if (startIndex == -1) return "";
        startIndex += startTag.length();
        int endIndex = source.indexOf(endTag, startIndex);
        if (endIndex == -1) return "";
        return source.substring(startIndex, endIndex).trim()
            .replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
    }

    public static ArrayList<Document> parseFR94(String filesPath) throws IOException {
        FR94documents.clear();
        File rootDir = Paths.get(filesPath).toFile();
        processDirectory(rootDir); 
        return FR94documents;
    }

    private static void processDirectory(File directory) throws IOException {
        File[] fileList = directory.listFiles(); 
        
        if (fileList == null) {
            return;
        }
        for (File item : fileList) {
            String fileName = item.getName();
            if (item.isDirectory()) {
                processDirectory(item);
            } else if (item.isFile() 
                    && !fileName.equals(readmeFiles[0]) 
                    && !fileName.equals(readmeFiles[1])) {
                    try (BufferedReader localBr = new BufferedReader(new FileReader(item))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = localBr.readLine()) != null) {
                        sb.append(line).append("\n");
                    }
                    String fileContent = sb.toString();
                    processDoc(fileContent); 
                }
            }
        }
    }

    public static void processDoc(String fileContent) throws IOException {
        String[] docs = fileContent.split("(?s)(?=<DOC>)");
        for (int i = 1; i < docs.length; i++) {
            String docText = docs[i];
            String docNo = extractTagContent(docText, "<DOCNO>", "</DOCNO>");
            String title = extractTagContent(docText, "<PARENT>", "</PARENT>");
            String fullText = extractTagContent(docText, "<TEXT>", "</TEXT>");
            if (!docNo.isEmpty()) {
                Document doc = new Document();
                doc.add(new StringField("DOCNO", docNo, Store.YES));
                doc.add(new TextField("TITLE", title, Store.YES));
                doc.add(new TextField("TEXT", fullText, Store.YES));
                FR94documents.add(doc);
            }
        }
    }
}