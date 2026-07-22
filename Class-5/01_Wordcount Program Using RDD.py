# ==========================================================================================
# WORD COUNT PROGRAM
# Method 1 : Spark RDD
# Method 2 : DataFrame API
# Method 3 : Spark SQL
# ==========================================================================================

# from                     -> Import specific functions from a module.
# pyspark                  -> Main PySpark package.
# sql                      -> Package for DataFrame and Spark SQL operations.
# functions                -> Module containing built-in DataFrame functions.
# split()                  -> Splits a string column into an array of words.
# explode()                -> Converts each array element into a separate row.
# col()                    -> Refers to a DataFrame column by name.

from pyspark.sql.functions import split, explode, col

# ==========================================================================================
# STEP 1 : Create Sample Input Data
# ==========================================================================================

# Normally we read data from ADLS Gen2 using textFile(), but for learning we are using
# a Python List to avoid storage/RDD configuration dependencies.
#
# rdd1 = spark.sparkContext.textFile(
#     "abfss://container@storageaccount.dfs.core.windows.net/input/WORDCOUNT_FILE.txt"
# )

myList = [
    "hello how are you",
    "hello team",
    "hi are you there",
    "I was there",
    "hi how are you",
    "hello how are you",
    "hello"
]

# ==========================================================================================
# METHOD 1 : WORD COUNT USING RDD
# ==========================================================================================

print("\n" + "=" * 100)
print("METHOD 1 : WORD COUNT USING RDD")
print("=" * 100)


#spark is the SparkSession object.
#SparkContext is the core execution engine of Spark responsible for communicating with the cluster and creating RDDs.
#Spark has two major APIs: SparkSession --> Works with structured data (DataFrame,Dataset,SQL,Catalog,Reader API,Writer API)
#                          SparkContext --> Works with low-level distributed data (RDD,parallelize(),textFile(),broadcast(),accumulator())
#						                --> SparkContext is the core execution engine of Apache Spark that communicates with the cluster, manages distributed execution, and creates RDDs
#                                       --> Why are we using SparkContext Because parallelize() belongs to SparkContext, not SparkSession.


# Convert Python List into Distributed Spark RDD.
rdd1 = spark.sparkContext.parallelize(myList)

print(f"Output of type(rdd1): {type(rdd1)}")

print("\nExplanation:")
print("- type() is a built-in Python function that returns the object's data type.")
print("- <class ...> indicates the object belongs to a Python class.")
print("- pyspark      : PySpark library.")
print("- core         : Core module of PySpark.")
print("- rdd          : Module where the RDD implementation exists.")
print("- RDD          : The class of which rdd1 is an object.")

print("\nConclusion:")
print("rdd1 is an object of the pyspark.core.rdd.RDD class, confirming that it is an RDD.")

rdd1.collect()

# Split each sentence into individual words.
rdd2 = rdd1.flatMap(lambda line: line.split(" "))
#rdd2.collect()

# Convert each word into a (word,1) key-value pair.
rdd3 = rdd2.map(lambda word: (word, 1))
#rdd3.collect()

# Aggregate the counts.
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)

# Trigger execution.
result = rdd4.collect()

display(result)

# ==========================================================================================
# METHOD 2 : WORD COUNT USING DATAFRAME API
# ==========================================================================================

print("\n" + "=" * 100)
print("METHOD 2 : WORD COUNT USING DATAFRAME API")
print("=" * 100)

# Normally we read data using DataFrame Reader API.
#
# wordcount_df = (
#     spark.read
#          .text("abfss://container@storageaccount.dfs.core.windows.net/input/WORDCOUNT_FILE.txt")
# )

# For learning purpose create DataFrame from Python List.
# Creating a Spark DataFrame from a Python List
#
# spark                 -> SparkSession object.
# createDataFrame()     -> Method used to create a Spark DataFrame.
#
# [(x,) for x in myList]
#   x                   -> One item (sentence) from myList.
#  (x,)                 -> Converts the sentence into a single-element tuple
#                          because Spark expects each row as a tuple.
#  for x in myList     -> Loops through every sentence in myList.
#
# ["sentence"]          -> Name of the DataFrame column.
#

# Final DataFrame
wordcount_df = spark.createDataFrame([(x,) for x in myList], ["sentence"])

# Split each sentence into words.
word_df = (
    wordcount_df.select(
                       explode(split(col("sentence"), " ")).alias("word")
                       )
)

# Count occurrences.
result_df = (
               word_df
              .groupBy("word")
              .count()
              .orderBy("word")
)

# Databricks
display(result_df)

# ==========================================================================================
# METHOD 3 : WORD COUNT USING SPARK SQL
# ==========================================================================================

print("\n" + "=" * 100)
print("METHOD 3 : WORD COUNT USING SPARK SQL")
print("=" * 100)

# Register DataFrame as Temporary View.
# Registering the DataFrame as a Temporary SQL View
#
# wordcount_df                 -> DataFrame object.
# .                            -> Accesses a method of the DataFrame.
# createOrReplaceTempView()    -> Creates a temporary SQL view or replaces it
#                                 if it already exists.
# "WORDCOUNT_TABLE"            -> Name of the temporary SQL view.
#
# After this, Spark SQL can query the DataFrame using:
#
# SELECT * FROM WORDCOUNT_TABLE
#

# Note:
# This temporary view exists only for the current Spark session.
wordcount_df.createOrReplaceTempView("WORDCOUNT_TABLE")

# Word Count using Spark SQL.
result_sql = spark.sql("""
SELECT
      word
	, COUNT(*)AS count
FROM
(
    SELECT
          EXPLODE(SPLIT(sentence, ' ')) AS word
    FROM  WORDCOUNT_TABLE
)
GROUP BY word
ORDER BY word
""")

# Databricks
display(result_sql)

