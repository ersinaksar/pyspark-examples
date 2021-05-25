from pyspark.sql import SparkSession
appName = "AddNumbers_ErsinAksar"
master = "local"
spark = SparkSession.builder\
    .master(master)\
    .appName(appName)\
    .getOrCreate()
#create RDD from parallelize
data=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
#collect sadece print etmek için
rdd = spark.sparkContext.parallelize(data).collect()
#rdd.reduce(lambda a,b: a+b)
print("=========================================================================")
print(rdd)
print("=========================================================================")