from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkFiles
from pyspark.sql.functions import col, struct, when, split
import findspark
from pyspark.sql.functions import col

findspark.init()
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Q4_ErsinAksar") \
    .getOrCreate()
data = [
    (("Naz", "Zıplar", "Sever"), ("F", "TR", "29"), 29000),
    (("Naz", "Zıplar", "Sever"), ("F", "TR", "29"), 29000),
    (("Deniz", "Mavi", "Kaçar"), ("F", "TR", "30"), 30000),
    (("Deniz", "Mavi", "Kaçar"), ("F", "TR", "30"), 30000),
    (("Çiçek", "Oynar", "Olur"), ("F", "TR", "31"), 31000),
    (("Çiçek", "Oynar", "Olur"), ("F", "TR", "31"), 31000),
    (("Çiçek", "Oynar", "Olur"), ("F", "TR", "31"), 31000),
    (("Rebeka", None, "İster"), ("F", "EN", "28"), 8787000),
    (("Naz", "Zıplar", "Sever"), ("F", "TR", "29"), 29000),
    (("Naz", "Zıplar", "Sever"), ("F", "TR", "29"), 29000),
    (("Naz", "Zıplar", "Sever"), ("F", "TR", "29"), 29000),
    (("Sude", "", "Bakar"), ("F", "TR", "32"), 32000),
    (("Su", "Bilinmez", "Biner"), ("F", "TR", "33"), 99999)]

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

# function print dataframe schema
df.printSchema()

print("Row count: " + str(df.count()))
# function  shows dataframe contens if truncate = false shows full names
df.show(truncate=False)

# 1. function return new dataframe after deleting duplicated records
distinctDF = df.distinct()
print("Row count: " + str(distinctDF.count()))
# function  shows dataframe contens if truncate = false shows full names
distinctDF.show(truncate=False)

# 2. function return new dataframe after deleting duplicated rows which we select column
dropDisDF = df.dropDuplicates(["students", "Salary"])
print("Distinct count of country and age : " + str(dropDisDF.count()))
# function  shows dataframe contens if truncate = false shows full names
dropDisDF.show(truncate=False)

print("Sorted as 'Salary', 'students.surname' ")
# 3. function table sorted by the first Salary column and then the students.surname column
df.sort("Salary", "students.surname").show(truncate=False)
# print("Sorted as 'col(Salary)', col('students.surname') ")
# df.sort(col("Salary"),col("students.surname")).show(truncate=False)


print("Sorted by 'Salary', as 'students.surname' ")
# 4. function table sorted by the first Salary column and then the students.surname column
df.orderBy("Salary", "students.surname").show(truncate=False)
# print("Sorted by 'Salary', as col('students.surname') ")
# df.sort(col("Salary"),col("students.surname")).show(truncate=False)

print("groupBy('students.surname').sum('Salary')")
# 5. function group students.surname and sum the salary
df.groupBy("students.surname").sum("Salary").show(truncate=False)

print("groupBy('students.surname').avg('Salary')")
# 6. function group students.surname and avg the salary
df.groupBy("students.surname").avg("Salary").show(truncate=False)
