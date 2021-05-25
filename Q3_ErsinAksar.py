from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkFiles
from pyspark.sql.functions import col, struct, when, split
import findspark

findspark.init()
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Q3_ErsinAksar") \
    .getOrCreate()
data = [
    (("Rebeka", None, "İster"),( "F", "EN", "28"), 28000),
    (("Naz", "Zıplar", "Sever"),( "F", "TR", "29"), 29000),
    (("Deniz", "Mavi", "Kaçar"),( "F", "TR", "30"), 30000),
    (("Çiçek", "Oynar", "Olur"), ("F", "TR", "31"), 31000),
    (("Sude", "", "Bakar"), ("F", "TR", "32"), 32000),
    (("Su", "Bilinmez", "Biner"),( "F", "TR", "33"), 99999)]

schema = StructType([
    StructField("students", StructType([
        StructField("name", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("surname", StringType(), True),
    ])),
    StructField("information", StructType([
        StructField("Gender", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Age", StringType(), True),
    ])),


        StructField("Salary", IntegerType(), True),

])
df = spark.createDataFrame(data=data, schema=schema)


#1. function print dataframe schema
df.printSchema()
#2.function  shows dataframe contens if truncate = false shows full names
df.show(truncate=False)
#3. function select specific nested data to show for all students
df.select("students.surname").show(truncate=False)
#4. function find specific nested data who are from "TR"
df.filter(df.information.Country == "TR").show(truncate=False)
#5. function retrieve data from dataframe
dataCollect = df.collect()
print(dataCollect)
#6. function changes dtatype from int to string for salary
df1= df.withColumn("Salary", col("Salary").cast("String"))
#df1.show(truncate=False)
df1.printSchema()
#7. function alse we can change the column data dtatype from int to string for salary
df2= df.withColumn("Salary", col("Salary")*3.14)
df2.show(truncate=False)
