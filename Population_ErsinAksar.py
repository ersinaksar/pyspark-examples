import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col,struct,when,split
spark = SparkSession.builder\
    .master("local[1]")\
    .appName("Pyspark Example-30")\
    .getOrCreate()
#Ulke	Population(2020)	YearlyChange	NetChange	Density(P/Km2)	LandAreaKm2	Migrants(net)	Fert.Rate	Med.Age	UrbanPop%	WorldShare

df = spark.read.text("Population_ErsinAksar.txt")
#number = 20
#df.show(number,truncate=False,vertical=False) #truncate=False tüm satırı gösteriyor(burası sayıda olabilir) ,number= kaç tane eleman göstereceğimiz
schema=df.withColumn('Ulke', split(col('value'), '/').getItem(0))\
                .withColumn('Population(2020)', split(col('value'), '/').getItem(1))\
                .withColumn('YearlyChange', split(col('value'), '/').getItem(2))\
                .withColumn('NetChange', split(col('value'), '/').getItem(3))\
                .withColumn('Density(P/Km2)', split(col('value'), '/').getItem(4))\
                .withColumn('LandAreaKm2', split(col('value'), '/').getItem(5))\
                .withColumn('Migrants(net)', split(col('value'), '/').getItem(6))\
                .withColumn('Fert.Rate', split(col('value'), '/').getItem(7))\
                .withColumn('Med.Age', split(col('value'), '/').getItem(8))\
                .withColumn('UrbanPop%', split(col('value'), '/').getItem(9))\
                .withColumn('WorldShare', split(col('value'), '/').getItem(10))\
.drop('value','UrbanPop%','WorldShare','NetChange','YearlyChange','Migrants(net)','Migrants(net)','Fert.Rate')\
.show()
