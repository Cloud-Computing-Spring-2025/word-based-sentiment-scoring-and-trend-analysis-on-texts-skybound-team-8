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
## **Task 2: Word Frequency Analysis with Lemmatization**

### **Objective**
Computing word frequencies by splitting each sentence into words and applying lemmatization to reduce words to their base forms

### **Steps to Execute Task 1**

#### **1. Start Docker Containers**
```sh
docker compose up -d
```
This command initializes the Hadoop ecosystem using Docker.

---

#### **2. Building the Java Code with Maven**
Before running the MapReduce job, compile the Java source files and package them into a JAR file by using maven :
```sh
mvn clean install
```
This command will generate a JAR file in the target/ directory, which contains your MapReduce code.

---

#### **3. Preparing Input Data Files **

The input that is output from Task1 is located in the output/ folder of the repository.

---

#### **4. Moving the Jar and Input Files to the Docker Container**
#### **4.1 Moving the JAR File to the Container**

Copy the built JAR file to the ResourceManager container. 

```bash
docker cp target/word-frequency-lemmatization-1.0-SNAPSHOT-jar-with-dependencies.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **4.2 Moving the Input File to the Container**

Next, copy the Word frequency lemmatization dataset to the ResourceManager container:

```bash
docker cp output/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
---

### 5. **Connect to the ResourceManager Container**

To run the Hadoop commands, we are connected to the ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Once inside the container, navigated to the Hadoop directory where our files were copied:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

---

### 6. **Set Up HDFS for Input File**

To run the MapReduce job, the input file needs to be stored in Hadoop’s distributed file system (HDFS).

#### **6.1 Creating Directories in HDFS**

Created a directory in HDFS for the input file by running the following command:

```bash
hadoop fs -mkdir -p /input/dataset2
```

#### **6.2 Uploading the Input File to HDFS**

Uploaded the output of task1 file to HDFS:

```bash
hadoop fs -put part-r-00000 /input/dataset2/
```

---

### 7. **Executing the Word Frequency Lemmatization MapReduce Jobs**

Run the job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/word-frequency-lemmatization-1.0-SNAPSHOT-jar-with-dependencies.jar Task2.WordFreqLemmatizationDriver /input/dataset2/part-r-00000 /output2
```

This command will execute the MapReduce job with the Cleaned data as the input and store the results in the `/output2` directory in HDFS.

---

### 8. **View the Output of the MapReduce Job**

The output will be stored in multiple directories (one for each task). We can view the output files using the following commands:

```bash
hadoop fs -ls /output
```

```bash
hadoop fs -cat /output/task2/part-r-00000
```
---

### 9. **Copy Output from HDFS to Local OS**

Once you have verified the results, copying the output from HDFS to your local file system.

Use the following command to copy the output from HDFS to the Hadoop directory:

```bash
hdfs dfs -get /output2 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
Now, exit the ResourceManager container:

```bash
exit
```
This retrieves the output from HDFS to the local filesystem for further analysis

---
## **Next Steps**
The output from Task 2 will serve as input for subsequent tasks.

## Task 3: Sentiment Scoring

### **Objective:
Task 3 aims to Assign sentiment scores to texts by mapping words (or their lemmatized forms) to sentiment values using a sentiment lexicon, ensuring that scores are traceable to individual books.
Given Instructions are :
Mapper:
Input: The output from Task 1 or Task 2.
Lexicon Matching: For each token (or lemma), check its presence in the chosen sentiment lexicon (e.g., AFINN or SentiWordNet) and retrieve the corresponding sentiment score.
Output: Emit key-value pairs where the key is a composite of (bookID, year) and the value is the sentiment score for that token.
Reducer:
Aggregation: Sum the sentiment scores for each key to obtain an overall sentiment score for each book and year.
Output: Produce a dataset mapping each book (and its year) to a cumulative sentiment score.


#### **1. Start Docker Containers**
```sh
docker compose up -d
```
This command initializes the Hadoop ecosystem using Docker.

---

#### **2. Building the Java Code with Maven**
Before running the MapReduce job, compile the Java source files and package them into a JAR file by using maven :
```sh
mvn clean install
```
This command will generate a JAR file in the target/ directory, which contains your MapReduce code.

---

#### **3. Preparing Input Data Files **

The input that is output from Task2 is located in the output/ folder of the repository.

---

#### **4. Moving the Jar and Input Files to the Docker Container**
#### **4.1 Moving the JAR File to the Container**

Copy the built JAR file to the ResourceManager container. 

```bash
docker cp target/word-sentiment-score-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **4.2 Moving the Input File to the Container**

Next, copy the Word frequency  dataset to the ResourceManager container:

```bash
docker cp output/task2/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
---

### 5. **Connect to the ResourceManager Container**

To run the Hadoop commands, we are connected to the ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Once inside the container, navigated to the Hadoop directory where our files were copied:

```bash

```
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/

---

### 6. **Set Up HDFS for Input File**

To run the MapReduce job, the input file needs to be stored in Hadoop’s distributed file system (HDFS).

#### **6.1 Creating Directories in HDFS**

Created a directory in HDFS for the input file by running the following command:

```bash
hadoop fs -mkdir -p /input/dataset3
```

#### **6.2 Uploading the Input File to HDFS**

Uploaded the output of task1 file to HDFS:

```bash
hadoop fs -put part-r-00000 /input/dataset3
```

---

### 7. **Executing the Word Frequency Lemmatization MapReduce Jobs**

Run the job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/word-sentiment-score-1.0-SNAPSHOT.jar Task3.WordSentimentScoreDriver /input/dataset3/part-r-00000 /output3
```

This command will execute the MapReduce job with the Cleaned data as the input and store the results in the `/output3` directory in HDFS.

---

### 8. **View the Output of the MapReduce Job**

The output will be stored in multiple directories (one for each task). We can view the output files using the following commands:

```bash
hadoop fs -ls /output
```

```bash
hadoop fs -cat /output/task2/part-r-00000
```
---

### 9. **Copy Output from HDFS to Local OS**

Once you have verified the results, copying the output from HDFS to your local file system.

Use the following command to copy the output from HDFS to the Hadoop directory:

```bash
hdfs dfs -get /output3 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
Now, exit the ResourceManager container:

```bash
exit
```
This retrieves the output from HDFS to the local filesystem for further analysis



---


## Task 4: Trend Analysis & Aggregation

### **Objective**
Task 4 aims to find long-term trends and possible connections with past events by combining sentiment scores and word frequencies over larger time periods (e.g., by decade). The data will be processed and aggregated using a Hadoop MapReduce operation, which will provide sentiment trends over time.

#### **Steps to Execute Task 4**

#### **1. Start Docker Containers**
```sh
docker compose up -d
```

#### **2. Build the Project with Maven**
```sh
mvn install
```

#### **3. Access the Resource Manager**
```sh
docker exec -it resourcemanager bash
```

#### **4. Create Directory for MapReduce Files**

```sh
mkdir -p /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **5. Exit Resource Manager Container**

```sh
exit
```


#### **6. Copy JAR File to Resource Manager**

```sh
docker cp target/word-Trend-Analysis-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

#### **7. Copy Task 3 Output to Resource Manager**

```sh
docker cp output/task3/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **8. Reconnect to Resource Manager**

```sh
docker exec -it resourcemanager bash
```


#### **9. Create Input Directory in HDFS**

```sh
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
hadoop fs -mkdir -p /input/dataset4
```


#### **10. Upload Task 3 Output to HDFS**

```sh
hadoop fs -put part-r-00000 /input/dataset4
```


#### **11. Run the MapReduce Job for Task 4**

```sh
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/word-Trend-Analysis-1.0-SNAPSHOT.jar Task4.WordTrendAnalysisDriver /input/dataset4/part-r-00000 /output4
```


#### **12. Retrieve Output from HDFS**

```sh
hdfs dfs -get /output4 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```


#### **13. Exit the Resource Manager Container**

```sh
exit
```


#### **14. Copy the Output to Local System**

```sh
docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output4/ task4/
```


### Note:
We've used the same XML file for Task 2, Task 3, and Task 4, specifying the appropriate class name for each task within the file.


