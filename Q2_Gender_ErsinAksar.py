from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkFiles
from pyspark.sql.functions import col,struct,when,split
import findspark
findspark.init()
spark = SparkSession.builder\
    .master("local[1]")\
    .appName("Gender_ErsinAksar")\
    .getOrCreate()

data = [
    (("Rebeka",None, "İster"), "F", "EN", "28", 28000),
    (("Naz", "Zıplar","Sever"), "F", "TR", "29", 29000),
    (("Deniz","Mavi", "Kaçar"), "F", "TR", "30", 30000),
    (("Çiçek", "Oynar","Olur"), "F", "TR", "31", 31000),
    (("Sude", "","Bakar"), "F", "TR", "32", 32000),
    (("Su","Bilinmez", "Biner"), "F", "TR", "33", 99999)]
schema = StructType([
    StructField("students",StructType([
        StructField("name", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("surname", StringType(), True),
    ])),
    StructField("Gender",StringType(),True),
    StructField("Country",StringType(),True),
    StructField("Age",StringType(),True),
    StructField("Salary",IntegerType(),True)
])
df = spark.createDataFrame(data=data,schema=schema)

df.printSchema()
df.show()
df.select("students.surname").show(truncate=False)
df.filter(df.students.surname == "Biner").show(truncate=False)