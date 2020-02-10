from pyspark import SparkConf
#from pyspark.context import SparkContext
from pyspark.context import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys
from pyspark.sql import *
from pyspark.sql.functions import broadcast, col, isnan, when, trim, lit
#from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
#from pyspark.sql.types import *
import psycopg2
import pandas as pd

#def to_null(c):
#    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c) == "")), col(c)))

def to_null(c):
    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "nan")), col(c))




def main():
        spark = SparkSession.builder.appName("Py").config("spark.driver.extraClassPath","/usr/share/java/postgresql.jar").getOrCreate()
        sc = spark.sparkContext
        w_csv = "weather_summary_filtered.csv"
	wdf = spark.read.format('csv').options(header='true', inferSchema='true').load(w_csv)
	wdf1 = wdf.withColumn("listing_url", lit("avgtemperature"))
	#city_list = wdf1.select('city_name').collect()
        #city_list = wdf.select("city_name").map(r => r.getString(0)).collect.toList 
	city_list = list (
                wdf1.select('city_name').toPandas()['city_name']
        )

	schema = StructType([StructField('city_name', StringType(), True),StructField('listing_url', StringType(), True),StructField('Jan', StringType(), True),StructField('Feb', StringType(), True),StructField('Mar', StringType(), True),StructField('Apr', StringType(), True),StructField('May', StringType(), True),StructField('Jun', StringType(), True),StructField('Jul', StringType(), True),StructField('Aug', StringType(), True),StructField('Sep', StringType(), True),StructField('Oct', StringType(), True),StructField('Nov', StringType(), True),StructField('Dec', StringType(), True)])
	sqlContext = SQLContext(sc)
        totaldf = sqlContext.createDataFrame(sc.emptyRDD(), schema)

        #my_city_list = ["paris","san-francisco","london"]
	for city in city_list:
		print("Adding City:",city)

		#if city != "paris" and city != "san-francisco":
		#	continue
                #print("Really Adding City:",city)
		#path = "san-francisco_monthly_price.csv"
		#path = "s3a://bnbcleanedv3/paris*rent*"
		path = 's3a://bnbcleanedv3/' + city + '*renter*'
                print(path)
		df =  spark.read.format('csv').options(header='true', inferSchema='true').load(path)
		#df = df.select([to_null(c).alias(c) for c in df.columns]).na.drop()
		df = df.select("city_name","listing_url","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec").where((col("price_variation")>20) & (col("price_variation")<200)).orderBy(["number_of_reviews"],ascending=False).limit(10)
		df.show()

		wdf2 = wdf1.select("city_name","listing_url","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec").where((col("city_name")==city))
		#.show()
		wdf2.show()

		p_result = wdf2.union(df)
		p_result.show(15)

		totaldf = totaldf.union(p_result)

	totaldf.show()	
	totaldf.write.format("jdbc").option("url", "jdbc:postgresql://10.0.0.8:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","allcities").option("user","postgres").option("password","postgres").save()


if __name__ == "__main__":
    main()
