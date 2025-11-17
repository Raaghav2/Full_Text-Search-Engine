package org.cs7is3;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TopicParser {
    public static class Topic {
        public String number;
        public String title;
        public String description;
        public String narrative;
        public Topic(String number, String title, String description, String narrative) {
            this.number = number;
            this.title = title;
            this.description = description;
            this.narrative = narrative;
        }

    }
    

    private static String extractTagContent(String source, String startTag, String endTag) {
        String content = "";
        int startIndex = source.indexOf(startTag);
        if (startIndex != -1) {
            startIndex += startTag.length();
            int endIndex = source.indexOf(endTag, startIndex);
            if (endIndex != -1) {
                content = source.substring(startIndex, endIndex).trim();
            }
        }
        
        if (startTag.equals("<num>")) {
            content = content.replace("Number:", "").trim();
        } else if (startTag.equals("<desc>")) {
            content = content.replace("Description:", "").trim();
        }

        return content.replace('\n', ' ').replace('\r', ' ').replaceAll("\\s+", " ");
    }

    public List<Topic> parse(Path topicsFilePath) throws IOException {
        List<Topic> topicList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(topicsFilePath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        String fileContent = sb.toString();
        String[] topics = fileContent.split("(?s)(?=<top>)");
        
        for (int i = 1; i < topics.length; i++) {
            String topicText = topics[i];

            String num = extractTagContent(topicText, "<num>", "</num>");
            String title = extractTagContent(topicText, "<title>", "</title>");
            String desc = extractTagContent(topicText, "<desc>", "</desc>");
            String narr = extractTagContent(topicText, "<narr>", "</narr>");

            if (!num.isEmpty()) {
                topicList.add(new Topic(num, title, desc, narr));
            }
        }

        return topicList;
    }

}
