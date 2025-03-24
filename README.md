# Word-Based Sentiment Scoring and Trend Analysis on Historical Texts

## **Project Objective**
Through the processing of digitized texts from the 18th and 19th centuries, this project examines sentiment trends in historical literature. There are several books in the collection, and each one has metadata like the book ID, title, and publishing year. Hadoop MapReduce in Java and Hive for data processing, analysis, and visualization are used to create a multi-stage processing pipeline. The objectives include:

- **Data Extraction & Cleaning:** Extract book metadata and preprocess text.
- **Word Frequency Analysis:** Tokenize and lemmatize words.
- **Sentiment Analysis:** Assign sentiment scores using sentiment lexicons.
- **Trend Analysis:** Aggregate scores and word frequencies over time.
- **Bigram Analysis:** Implement a Hive UDF to extract and analyze bigrams.

## **Task 1: Preprocessing MapReduce Job**

### **Objective**
Clean and standardize the raw text data from multiple books while extracting essential metadata (book ID, title, year of publication).

### **Steps to Execute Task 1**

#### **1. Start Docker Containers**
```sh
docker compose up -d
```
This command initializes the Hadoop ecosystem using Docker.

#### **2. Compile Java Files and Create a JAR**
Before running the MapReduce job, compile the Java source files and package them into a JAR file:
```sh
javac -classpath $(hadoop classpath) -d /Task1/classes *.java
jar -cvf preprocessing.jar -C /Task1/classes/ .
```

#### **3. Copy Input Data to HDFS**
```sh
hdfs dfs -mkdir -p /user/root/input
hdfs dfs -mkdir -p /user/root/jars
hdfs dfs -put /Task1/preprocessing.jar /user/root/jars/
hdfs dfs -put /workspaces/word-based-sentiment-scoring-and-trend-analysis-on-texts-skybound-team-8/inputs/* /user/root/input/
```
This uploads the dataset and the JAR file into Hadoop's distributed file system.

#### **4. Run the MapReduce Job**
```sh
hadoop jar /user/root/jars/preprocessing.jar PreprocessingDriver /user/root/input /user/root/output
```
This processes the input text and generates the cleaned dataset.

#### **5. Verify Output in HDFS**
```sh
hdfs dfs -ls /user/root/output/
```
Ensure that output files (`part-r-00000` and `_SUCCESS`) are created.

#### **6. View Results**
```sh
hdfs dfs -cat /user/root/output/part-r-00000
```
This command displays the processed output.

#### **7. Copy Output to Local System**
```sh
docker cp namenode:/user/root/output /workspaces/word-based-sentiment-scoring-and-trend-analysis-on-texts-skybound-team-8/output/task1
docker cp namenode:/tmp/preprocessing.jar /workspaces/word-based-sentiment-scoring-and-trend-analysis-on-texts-skybound-team-8/output/task1/
```
This retrieves the output from HDFS to the local filesystem for further analysis.

## **Next Steps**
The output from Task 1 will serve as input for subsequent tasks.

---


