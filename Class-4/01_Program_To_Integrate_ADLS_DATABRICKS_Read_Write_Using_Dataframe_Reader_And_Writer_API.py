# ==========================================
# Variable Based Configuration
# ==========================================

print("=" * 80)
print("STEP 1 : Configuring Spark to connect to the Azure Storage Account.")
print("=" * 80)

# storage_account = "<STORAGE_ACCOUNT_NAME>"
# access_key      = "<STORAGE_ACCOUNT_ACCESS_KEY>"
# container_name  = "<CONTAINER_NAME>"

storage_account = "devstorageadls"
access_key = "hm4vfg+xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxF1Pz+AStnL4LaA=="
container_name = "container-3"
input_directory_name = "input_files"
output_directory_name = "output_files"

# This stores a key-value pair inside Spark's configuration.
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

print("✅ Spark configuration has been created successfully.")


# ==========================================
# Verify Spark Configuration
# ==========================================

print("\n" + "=" * 80)
print("STEP 2 : Verifying whether the Spark configuration exists.")
print("=" * 80)

# Reads the Azure Storage Account Access Key stored in the Spark Configuration.
configured_access_key = spark.conf.get(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"
)

print(f"🔑 Here is the Configured Access Key : {configured_access_key}")
print("✅ Spark configuration verified successfully.")


# ==========================================
# Verify Storage Account Connection
# ==========================================

print("\n" + "=" * 80)
print("STEP 3 : Verifying the connection by accessing the Storage Account.")
print("=" * 80)

#This line lists all the files and folders present inside the specified directory in your Azure Data Lake Storage Gen2 (ADLS Gen2) container and displays them in a nice tabular format in Databricks.
display(
        dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{input_directory_name}/")
       )

print("✅ Successfully connected to the Azure Storage Account.")
print("✅ Files and folders are accessible.")


# ==========================================
# DataFrame Reader API
# ==========================================
#Variable: A variable is a named reference that points to a value or an object in memory.
#DataFrame: A DataFrame is a distributed, tabular data structure with rows and named columns, similar to an Excel sheet or a database table.
#wordcount_df: This is a variable that refers to a DataFrame object.
#col("Order_Status"): This creates a Column object representing the Order_Status column, allowing Spark to build expressions such as filters, selections, and transformations.

print("\n" + "=" * 80)
print("STEP 4 : Reading a file from ADLS using the DataFrame Reader API.")
print("=" * 80 + "\n")

input_file_path = (
    f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"
    f"{input_directory_name}/Customer_Dataset.csv"
)

print(f"📂 Input File Path from ADLS : {input_file_path}\n")


#spark.read is not a method. It is a property of the SparkSession object that returns a DataFrameReader object. The DataFrameReader object provides methods such as csv(), json(), parquet(), orc(), text(), format(), and load() to read data from different sources into a Spark DataFrame.
#read acts as an entry point for all file-reading operations.
wordcount_df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load(input_file_path)
)

wordcount_df.show()

print('\n📋 Schema Inferred Automatically by Spark (using .option("inferSchema", "true")):\n')
wordcount_df.printSchema()

# wordcount_df.show(10)
# wordcount_df.show(truncate=False)
# wordcount_df.show(10, truncate=False)
# display(wordcount_df)

print("✅ File has been read successfully using the DataFrame Reader API.")


# ==========================================
# DataFrame Filter Transformation
# ==========================================

#We import it from pyspark.sql.functions -- because col() is not a Python built-in function. It belongs to the PySpark SQL functions library.
#col() is a built-in PySpark function that returns a Column object representing a column in a DataFrame.
#col("Order_Status") tells Spark, "Use the Order_Status column from this DataFrame."

from pyspark.sql.functions import col

print("\n" + "=" * 80)
print("STEP 5 : Filtering records where Order_Status = 'Closed'.")
print("=" * 80 + "\n")

filtered_df = (
                wordcount_df
               .filter(col("Order_Status") == "Closed")
              )

filtered_df.show()

print("✅ Records have been filtered successfully.")


# ==========================================
# DataFrame Writer API
# ==========================================
#When Spark writes data, it creates part-* files containing the actual data and additional metadata files such as _SUCCESS, _started_*, and _committed_*. These metadata files help Spark track the progress and successful completion of distributed write operations, while only the part-* files contain the actual dataset.

print("\n" + "=" * 80)
print("STEP 6 : Writing the filtered data to ADLS using the DataFrame Writer API.")
print("=" * 80 + "\n")

output_file_path = (
    f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"
    f"{output_directory_name}"
)

print(f"📂 File has been written to the ADLS path successfully. Please check this path : {output_file_path}\n")


# df.write is not a method. It is a property of the DataFrame object that returns a DataFrameWriter object. The DataFrameWriter object provides methods such as csv(), json(), parquet(), orc(), save(), saveAsTable(), mode(), partitionBy(), and bucketBy() to write a DataFrame to different storage systems and formats.

(
    filtered_df.write
               .format("csv")
               .mode("overwrite")
               .option("header", "true")
               .save(output_file_path)
)

print("✅ Filtered file has been written successfully using the DataFrame Writer API.")