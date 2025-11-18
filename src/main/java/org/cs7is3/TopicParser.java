package org.cs7is3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TopicParser {

    public static class Topic {
        public final String number;
        public final String title;
        public final String description;
        public final String narrative;

        public Topic(String number, String title, String description, String narrative) {
            this.number = number;
            this.title = title;
            this.description = description;
            this.narrative = narrative;
        }
    }

    public List<Topic> parse(Path topicsPath) throws IOException {
        List<Topic> topics = new ArrayList<>();
        String content = new String(Files.readAllBytes(topicsPath));

        String[] topicBlocks = content.split("(?m)^<top>$");

        for (String block : topicBlocks) {
            if (block.trim().isEmpty()) {
                continue;
            }

            String num = extractTagContent(block, "<num> Number: ", true);
            String title = extractTagContent(block, "<title> ", false);
            String desc = extractTagContent(block, "<desc> Description:", false);
            String narr = extractTagContent(block, "<narr> Narrative:", false);
            
            if (num != null && !num.isEmpty()) {
                num = num.replaceAll("[^0-9]", "");
            }
            
            if (num != null && !num.isEmpty() && title != null && !title.isEmpty()) {
                topics.add(new Topic(num, title, desc, narr));
            }
        }
        return topics;
    }

    private String extractTagContent(String block, String tag, boolean isNum) {
        int startIndex = block.indexOf(tag);
        if (startIndex == -1) {
            return "";
        }
        startIndex += tag.length();

        int endIndex;
        if (isNum) {
            endIndex = block.indexOf('\n', startIndex);
        } else {
            endIndex = block.indexOf("\n<", startIndex);
        }

        if (endIndex == -1) {
            endIndex = block.indexOf("</top>", startIndex);
            if (endIndex == -1) {
                endIndex = block.length();
            }
        }

        return block.substring(startIndex, endIndex).trim()
                .replace("\n", " ")
                .replaceAll("\\s+", " ");
    }
}