package com.lucene.indexandsearch.query;

import com.lucene.indexandsearch.utils.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class QueryReader {


    private final static String absPathToQueries = String.format(Constants.QUERYFILEPATH);

    public static List<QueryData> loadQueriesFromFile() {
        QueryData QueryData = new QueryData();
        String tempTag = QueryTags.TOP_START.getTag();
        String topTag = QueryTags.TOP_START.getTag();

        List<QueryData> queries = new ArrayList<>();

        try {
            BufferedReader bf = new BufferedReader(new FileReader(absPathToQueries));
            String queryLine;

            int counter = 0;
            while ((queryLine = bf.readLine()) != null) {
                String queryLineTag = checkIfDocLineHasTag(queryLine);

                if (queryLineTag != null && queryLineTag.equals(topTag)) { // if docLineTag isnt null and if it hasnt changed
                    counter++;
                    tempTag = queryLineTag;
                    queries.add(QueryData);
                    QueryData = new QueryData();
                } else if (queryLineTag != null && !queryLineTag.equals(topTag)) { // otherwise, update the tag
                    tempTag = queryLineTag;
                }
                populateQueryFields(tempTag, queryLine, QueryData, counter);
            }
            queries.add(QueryData);
            queries.remove(0);
            bf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }

    private static String checkIfDocLineHasTag(String docLine) {
        for (QueryTags tag : QueryTags.values()) {
            if (docLine.contains(tag.getTag())) {
                return tag.getTag();
            }
        }
        return null;
    }

    private static void populateQueryFields(String queryLineTag, String queryLine, QueryData QueryData, int counter) {
        if (queryLineTag.equals(QueryTags.QUERY_NUMBER.getTag())) {
            QueryData.setQueryNum(queryLine.replaceAll(QueryTags.QUERY_NUMBER.getTag(), ""));
        } else if (queryLineTag.equals(QueryTags.QUERY_TITLE.getTag())) {
            QueryData.setTitle(QueryData.getTitle() + " " + queryLine.replaceAll(QueryTags.QUERY_TITLE.getTag(),
                    "").trim());
        } else if (queryLineTag.equals(QueryTags.QUERY_DESCRIPTION.getTag())) {
            QueryData.setDescription(QueryData.getDescription() + " " + queryLine.replaceAll(
                    QueryTags.QUERY_DESCRIPTION.getTag(), "").trim());
        } else if (queryLineTag.equals(QueryTags.QUERY_NARRATIVE.getTag())) {
            QueryData.setNarrative(QueryData.getNarrative() + " " + queryLine.replaceAll(
                    QueryTags.QUERY_NARRATIVE.getTag(), "").trim());
        } else {
            QueryData.setQueryId(String.valueOf(counter));
        }
    }

}
