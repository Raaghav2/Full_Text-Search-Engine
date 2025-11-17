package org.cs7is3.Parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class FBISParser {
    public static String[] readmeFiles = {"readchg.txt", "readmefb.txt"};
    public static BufferedReader br;
    public static ArrayList<Document> FBISdocuments = new ArrayList<>();

    private static String extractTagContent(String source, String startTag, String endTag) {
        int startIndex = source.indexOf(startTag);
        if (startIndex == -1) return "";
        startIndex += startTag.length();
        int endIndex = source.indexOf(endTag, startIndex);
        if (endIndex == -1) return "";
        return source.substring(startIndex, endIndex).trim()
            .replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
    }

    public static ArrayList<Document> parseFBIS(String filesPath) throws IOException {
        FBISdocuments.clear();
        Directory dir = FSDirectory.open(Paths.get(filesPath));

        for(String fbisFile : dir.listAll()) {
            if(!fbisFile.equals(readmeFiles[0]) && !fbisFile.equals(readmeFiles[1])) {
                br = new BufferedReader(new FileReader(Paths.get(filesPath, fbisFile).toFile()));
                processDoc();
            }
        }
        return FBISdocuments;
    }

    public static void processDoc() throws IOException {
        String file = "";
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = br.readLine();
            }
            file = sb.toString();
        } finally {
            if (br != null) {
                br.close();
            }
        }
        String[] docs = file.split("(?s)(?=<DOC>)");
        for (int i = 1; i < docs.length; i++) {
            String docText = docs[i];
            String docNo = extractTagContent(docText, "<DOCNO>", "</DOCNO>");
            String title = extractTagContent(docText, "<TI>", "</TI>");
            String fullText = extractTagContent(docText, "<TEXT>", "</TEXT>");
            if (!docNo.isEmpty()) {
                Document doc = new Document();
                doc.add(new StringField("DOCNO", docNo, Store.YES));
                doc.add(new TextField("TITLE", title, Store.YES));
                doc.add(new TextField("TEXT", fullText, Store.YES));
                FBISdocuments.add(doc);
            }
        }
    }
}