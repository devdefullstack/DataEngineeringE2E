----------------------------------
#databricks gives spark session object by default
spark

spark.version

#Same as spark session object
#Scenario 1 — Running PySpark from Local Machine
#Scenario 2 — Python Script (.py)
#Scenario 3 — spark-submit
#Scenario 4 — Standalone Spark Cluster
#Scenario 5 — Jupyter Notebook (Local)
#Scenario 6 — VS Code / PyCharm
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
type(spark)

----------------------------------
%python
x = 10
y = 40

print("Hello Python")
print("Result =", x + y)

----------------------------------
%scala
val x = 10
val y = 40

println("Hello Scala")
println("Result = " + (x + y))
----------------------------------
%r 

x <- 10
y <- 40

print("Hello R")
print(paste("Result =", x + y))
print(R.version.string)

----------------------------------
%sql

SELECT 'Hello SQL' AS Message,
       10 + 40     AS Result;

----------------------------------