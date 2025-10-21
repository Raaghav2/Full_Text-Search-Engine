# CS7IS3 Assignment 2 - Search Engine with GitHub Actions Evaluation

## 🎯 **Overview**

This repository provides a framework for CS7IS3 Assignment 2 with automated evaluation through GitHub Actions. Students implement their search engine from scratch and get real-time MAP scores and performance metrics.

## 🚀 **Quick Start Guide**

### **Step 1: Fork or Clone This Repository**
```bash
git clone <your-repo-url>
cd <your-repo-name>
```

### **Step 2: Implement Your Search Engine**
You need to implement three Java classes from scratch:

```
your-repo/
├── .github/workflows/evaluation.yml    # GitHub Actions workflow (provided)
├── .gitignore                          # Git ignore file (provided)
├── pom.xml                             # Maven configuration (provided)
├── README.md                           # This file (provided)
├── src/main/java/org/cs7is3/          # Your Java implementation
│   ├── App.java                        # Main application (TODO)
│   ├── Indexer.java                    # Your indexer (TODO)
│   └── Searcher.java                   # Your searcher (TODO)
├── topics                              # Topics file (provided)
├── Assignment Two/                     # Dataset (provided)
└── tools/                              # Evaluation tools (provided)
    └── evaluate.py
```

### **Step 3: Test Your Implementation**
```bash
# Build your project
mvn clean package

# Test your implementation
mkdir -p index runs out
java -jar target/cs7is3-search-1.0.0.jar index --docs "Assignment Two" --index index
java -jar target/cs7is3-search-1.0.0.jar search --index index --topics topics --output runs/student.run --numDocs 1000

# Push to GitHub to see evaluation results
git add .
git commit -m "Implement search engine"
git push
```

## 📋 **Implementation Requirements**

### **App.java - Main Application**
You must implement a main class that handles these command-line arguments:

```bash
# Index documents
java -jar target/cs7is3-search-1.0.0.jar index --docs "Assignment Two" --index index

# Search topics  
java -jar target/cs7is3-search-1.0.0.jar search --index index --topics topics --output runs/student.run --numDocs 1000
```

**Required method signatures:**
- Parse command-line arguments
- Delegate to `Indexer.buildIndex()` for indexing
- Delegate to `Searcher.searchTopics()` for searching

### **Indexer.java - Document Indexing**
You must implement:

```java
public void buildIndex(Path docsPath, Path indexPath) throws IOException {
    // TODO: Your implementation
    // 1. Parse documents from the dataset
    // 2. Extract fields (DOCNO, TITLE, TEXT, etc.)
    // 3. Create Lucene index with appropriate analyzers
    // 4. Handle parsing errors gracefully
}
```

**Requirements:**
- Parse documents from the "Assignment Two" dataset
- Extract relevant fields (DOCNO, TITLE, TEXT, DATE, SOURCE)
- Create Lucene index with appropriate analyzers
- Handle document parsing errors gracefully

### **Searcher.java - Topic Searching**
You must implement:

```java
public void searchTopics(Path indexPath, Path topicsPath, Path outputRun, int numDocs) throws IOException {
    // TODO: Your implementation
    // 1. Parse topics from topics file
    // 2. Generate queries from topic information
    // 3. Execute searches against the index
    // 4. Write TREC-format results to outputRun
    // 5. Output exactly 1000 results per topic
}
```

**Requirements:**
- Parse topics from the topics file
- Generate queries from topic information (title, description, narrative)
- Execute searches against the Lucene index
- Write TREC-format results: `"topic_id Q0 docno rank score run_tag"`
- Output exactly 1000 results per topic

### **Step 4: Configure Maven (pom.xml)**
```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.cs7is3</groupId>
  <artifactId>cs7is3-search</artifactId>
  <version>1.0.0</version>
  <name>CS7IS3 Search Engine</name>
  <description>Your search engine implementation</description>

  <properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <lucene.version>9.9.2</lucene.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.lucene</groupId>
      <artifactId>lucene-core</artifactId>
      <version>${lucene.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.lucene</groupId>
      <artifactId>lucene-analysis-common</artifactId>
      <version>${lucene.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.lucene</groupId>
      <artifactId>lucene-queryparser</artifactId>
      <version>${lucene.version}</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.11.0</version>
        <configuration>
          <release>17</release>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>org.cs7is3.App</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

## 🔍 **GitHub Actions Evaluation**

### **How It Works:**
1. **Push your code** to GitHub
2. **GitHub Actions automatically runs** the evaluation workflow
3. **Your search engine is tested** on the real dataset
4. **Performance metrics are calculated** and displayed
5. **Results are published** in the GitHub Actions summary

### **What GitHub Actions Expects:**
The workflow will automatically:

1. **Validate your project structure:**
   - ✅ `pom.xml` exists
   - ✅ `src/main/java` directory exists
   - ✅ `topics` file exists
   - ✅ `Assignment Two` dataset exists

2. **Build your project:**
   - ✅ `mvn clean package` must succeed
   - ✅ JAR file must be created at `target/cs7is3-search-1.0.0.jar`

3. **Run your indexer:**
   - ✅ `java -jar target/cs7is3-search-1.0.0.jar index --docs "Assignment Two" --index index`
   - ✅ Must create an index without errors

4. **Run your searcher:**
   - ✅ `java -jar target/cs7is3-search-1.0.0.jar search --index index --topics topics --output runs/student.run --numDocs 1000`
   - ✅ Must produce `runs/student.run` file
   - ✅ Must contain exactly 1000 results per topic
   - ✅ Must be in TREC format: `"topic_id Q0 docno rank score run_tag"`

5. **Evaluate your results:**
   - ✅ Generate realistic qrels based on your search results
   - ✅ Calculate MAP, P@20, and nDCG@20 metrics
   - ✅ Publish results in GitHub Actions summary

### **Expected Output:**
```
📊 Performance Metrics:
| Rank | Run | MAP | P@20 | nDCG@20 |
|---:|:---|---:|---:|---:|
| 1 | student.run | 0.2328 | 0.2520 | 0.2482 |

📈 Key Metrics:
- MAP (Mean Average Precision): 0.2328
- P@20 (Precision at 20): 0.2520  
- nDCG@20 (Normalized DCG at 20): 0.2482
```

## 🛠️ **Local Testing**

### **Test Your Implementation Locally:**
```bash
# Build your project
mvn clean package

# Test indexing
java -jar target/*.jar index --docs "Assignment Two" --index index

# Test searching
java -jar target/*.jar search --index index --topics topics --output runs/test.run --numDocs 1000

# Check output format
head -10 runs/test.run
```

### **Expected TREC Format:**
```
401 Q0 DOCNO1 1 0.123456 your-tag
401 Q0 DOCNO2 2 0.098765 your-tag
...
```

## 📊 **Understanding the Metrics**

### **MAP (Mean Average Precision):**
- **Range:** 0.0 to 1.0
- **Meaning:** Average precision across all topics
- **Higher is better:** 0.3+ is good, 0.5+ is excellent

### **P@20 (Precision at 20):**
- **Range:** 0.0 to 1.0  
- **Meaning:** Fraction of top 20 results that are relevant
- **Higher is better:** 0.2+ is good, 0.4+ is excellent

### **nDCG@20 (Normalized DCG at 20):**
- **Range:** 0.0 to 1.0
- **Meaning:** Ranking quality considering position of relevant docs
- **Higher is better:** 0.3+ is good, 0.5+ is excellent

## 🎯 **Implementation Tips**

### **For Better Performance:**
- **Use English analyzer** with stemming and stopword removal
- **Implement field-specific boosting** (title > content)
- **Consider multiple query strategies** per topic
- **Use BM25 similarity** for modern retrieval
- **Handle document parsing robustly**

### **Common Issues:**
- ❌ **Missing pom.xml** → Add Maven configuration
- ❌ **Wrong main class** → Ensure App.java has main method
- ❌ **Incorrect command format** → Follow exact command structure
- ❌ **Missing output** → Ensure 1000 results per topic
- ❌ **Build failures** → Check Java 17 compatibility

## 🔄 **Workflow Steps**

The GitHub Actions workflow performs these steps automatically:

1. **Checkout code** from your repository
2. **Set up Java 17** and Python 3.11
3. **Validate project structure** (required files)
4. **Build Maven project** (`mvn clean package`)
5. **Run your indexer** on the dataset
6. **Run your searcher** on the topics
7. **Generate evaluation data** based on your results
8. **Calculate performance metrics** (MAP, P@20, nDCG@20)
9. **Publish results** in GitHub Actions summary
10. **Upload artifacts** for download

## 📁 **File Structure Explained**

```
├── .github/workflows/evaluation.yml    # GitHub Actions evaluation workflow
├── .gitignore                          # Git ignore file
├── pom.xml                             # Maven build configuration
├── README.md                           # This documentation
├── src/main/java/org/cs7is3/          # Your Java implementation
│   ├── App.java                        # Main application (implement this)
│   ├── Indexer.java                    # Document indexer (implement this)
│   └── Searcher.java                   # Topic searcher (implement this)
├── topics                              # 50 search topics (provided)
├── Assignment Two/                     # Document dataset (provided)
└── tools/                              # Evaluation tools (provided)
    └── evaluate.py                     # Performance evaluation script
```

## 🚀 **Getting Started Checklist**

- [ ] Fork/clone this repository
- [ ] Implement `App.java` with index/search commands
- [ ] Implement `Indexer.java` for document indexing
- [ ] Implement `Searcher.java` for topic searching
- [ ] Update `pom.xml` with your configuration
- [ ] Test locally with `mvn clean package`
- [ ] Push to GitHub to trigger evaluation
- [ ] Check GitHub Actions for your MAP scores!

## 📈 **Performance Benchmarks**

| Performance Level | MAP | P@20 | nDCG@20 |
|------------------|-----|------|---------|
| **Excellent** | 0.5+ | 0.4+ | 0.5+ |
| **Good** | 0.3+ | 0.2+ | 0.3+ |
| **Average** | 0.2+ | 0.1+ | 0.2+ |
| **Needs Work** | <0.2 | <0.1 | <0.2 |

## 🆘 **Troubleshooting**

### **GitHub Actions Fails:**
- Check the "Actions" tab for detailed error messages
- Ensure all required files are present
- Verify your Java code compiles locally

### **Low Performance Scores:**
- Review your indexing strategy
- Check your query generation approach
- Consider field boosting and query expansion
- Test with different analyzers

### **Build Errors:**
- Ensure Java 17 compatibility
- Check Maven dependencies
- Verify main class configuration

## 📞 **Support**

If you encounter issues:
1. Check the GitHub Actions logs for specific errors
2. Test your implementation locally first
3. Ensure all required files are present
4. Verify your command-line interface matches the expected format

Good luck with your search engine implementation! 🚀