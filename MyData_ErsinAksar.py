from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkFiles
from pyspark.sql.functions import col,struct,when,split
import findspark
findspark.init()
spark = SparkSession.builder\
    .master("local[1]")\
    .appName("MyData_ErsinAksar")\
    .getOrCreate()
schema = StructType([StructField("Name",StringType(),True),StructField("Surname",StringType(),True),StructField("Gender",StringType(),True),StructField("Country",StringType(),True),StructField("Age",StringType(),True),StructField("Salary",IntegerType(),True)])
data = [("Kara", "Çukur", "M", "TR", "43", 43000),
("Veysel", "Kaya", "M", "TR", "45", 11000),
("ahmet", "Geridönen", "M", "TR", "43", 34000),
("Mehmet", "Kalas", "M", "TR", "42", 53000),
("Osman", "Kuzu", "M", "TR", "33", 32000),
("Haşmet", "Karpuz", "M", "TR", "34", 12000),
("Haşamet", "Salı", "M", "TR", "35", 9000),
("Çelebi", "Döner", "M", "TR", "36", 8763),
("Hayriye", "Uçar", "F", "TR", "37", 32234),
("Begüm", "Kasap", "F", "TR", "36", 44321),
("İrem", "Yolagelir", "F", "TR", "24", 24000),
("Tuğçe", "Gider", "F", "TR", "25", 25000),
("Tuğba", "Yatar", "F", "TR", "26", 26000),
("Alice", "Ağlar", "F", "RS", "27", 2700),
("Rebeka", "İster", "F", "EN", "28", 28000),
("Naz", "Sever", "F", "TR", "29", 29000),
("Remziye", "Kaçar", "F", "TR", "30", 30000),
("Çiçek", "İster", "F", "TR", "31", 31000),
("Sude", "Bakar", "F", "TR", "32", 32000),
("Su", "Biner", "F", "TR", "33", 99999)]
mydataframe = spark.createDataFrame(data=data,schema=schema)
mydataframe.printSchema()
mydataframe.show()
