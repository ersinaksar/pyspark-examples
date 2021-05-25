from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkFiles
from pyspark.sql.functions import col, struct, when, split
import findspark

findspark.init()
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Q5_ErsinAksar") \
    .getOrCreate()

data1 = [("Rebeka", "İster", "US", 49000, 28),
         ("Naz", "Zıplar", "TR", 29230, 43),
         ("Deniz", "Mavi", "FR", 31000, 24),
         ("Sude", "Bakar", "DE", 14000, 21)
         ]

data2 = [("Çiçek", "Oynar", "BS", 49000, 27),
         ("Çiçek", "Oynar", "BS", 49000, 27),
         ("Su", "Bilinmez", "JP", 78000, 34),
         ]

data3 = ["Zaman Çarkı Serisi",
         "Unutulmuş Diyarlar",
         "Ejderha Mızrağı Serisi",
         "Cüceler Serisi",
         "Büyücü Serisi"]
# columns1= ["employee_name","employee_surname","Country","salary","age"]
# columns2= ["employee_name","employee_surname","Country","salary","age"]

schema = StructType([
    StructField("employee_name", StringType(), True),
    StructField("employee_surname", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("age", IntegerType(), True),
])

df1 = spark.createDataFrame(data=data1, schema=schema)
df2 = spark.createDataFrame(data=data2, schema=schema)

# 1. function print dataframe schema
df1.printSchema()
# 2.function  shows dataframe contens if truncate = false shows full names
df1.show(truncate=False)

unionDF = df1.union(df2)
print("#1. function")
# unionDF = df1.union(df2) to merge two dataframe's of the same structure/schema
print("#unionDF = df1.union(df2) to merge two dataframe's of the same structure/schema")
unionDF.show(truncate=False)

disDF = df1.union(df2).distinct()
print("#2. function")
# disDF = df1.union(df2).distinct() to merge two dataframe's of the same structure/schema but return just one record
# when duplicate exist
print("#disDF = df1.union(df2).distinct() to merge two dataframe's of the same structure/schema but return just one "
      "record when duplicate exist")
disDF.show(truncate=False)

rdd = spark.sparkContext.parallelize(data3)
rdd2 = rdd.flatMap(lambda x: x.split(" "))
print("#3. function")
# rdd2=rdd.flatMap(lambda x: x.split(" ")) split all records by space same structure/schema
print("#rdd2=rdd.flatMap(lambda x: x.split(" ")) split all records by space same structure/schema")
for element in rdd2.collect():
    print(element)

df3 = spark.range(79)
print("#4. function")
# df3 = spark.range(79) df3.sample(0.20).collect() get random sample from pyspark
print("#df3 = spark.range(79) df3.sample(0.20).collect() get random sample from pyspark")
print(df3.sample(0.20).collect())

print("#5. function")
# df3.sample(0.46, 43).collect() reproduce same samples from pyspark
print("#df3.sample(0.46, 43).collect() reproduce same samples from pyspark")
print(df3.sample(0.46, 23).collect())

print("#6. function")
# df3.sample(True, 0.20, 42).collect() samples may contain duplicates
print("#df3.sample(True, 0.20, 42).collect() samples may contain duplicates")
print(df3.sample(True, 0.46, 23).collect())
