package org.cs7is3;

// TODO: Implement your Lucene searcher
// This class should search topics against the index and produce TREC-format results
//
// Requirements:
// 1. Parse topics from the topics file
// 2. Generate queries from topic information (title, description, narrative)
// 3. Execute searches against the Lucene index
// 4. Write TREC-format results: "topic_id Q0 docno rank score run_tag"
// 5. Output exactly 1000 results per topic
//
// The GitHub Actions workflow will call:
//   searcher.searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs)

public class Searcher {
    public void searchTopics(java.nio.file.Path indexPath, java.nio.file.Path topicsPath, 
                           java.nio.file.Path outputRun, int numDocs) throws java.io.IOException {
        // TODO: Implement your search logic
        System.out.println("TODO: Implement your Lucene searcher");
    }
}
