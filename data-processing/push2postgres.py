from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys
from pyspark.sql import *
from pyspark.sql.functions import broadcast
#from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import psycopg2


spark = SparkSession.builder.appName("Py").config("spark.driver.extraClassPath","/usr/share/java/postgresql.jar").getOrCreate()

#path = "san-francisco_monthly_price.csv"
path = "s3a://bnbcleanedv3/*rent*"
df =  spark.read.format('csv').options(header='true', inferSchema='true').load(path)


#df.write.format("jdbc").option("url", "jdbc:postgresql://5432/ec2-18-191-205-97.us-east-2.compute.amazonaws.com").option("dbtable","sanf").option("user","postgres").option("password","postgres").save()

df.write.format("jdbc").option("url", "jdbc:postgresql://10.0.0.8:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","renters").option("user","postgres").option("password","postgres").save()
#option("driver","org.postgresql.Driver").save()
#jdbc:postgresql://10.0.0.8:5432/postgres
