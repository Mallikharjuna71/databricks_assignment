# Databricks notebook source
import requests
data = requests.get('https://reqres.in/api/users?page=2').json()
print(data)

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([StructField('page', IntegerType()), StructField('per_page', StringType()), StructField('total', StringType()), StructField('total_pages', StringType()), StructField('data', ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)]))), StructField('support', MapType(StringType(), StringType()))])
df = spark.createDataFrame([data], schema)
df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df = df.drop('page', 'per_page', 'total', 'total_pages')
df = df.select(explode('data').alias('data')).select('*', 'data.id','data.email', 'data.first_name', 'data.last_name', 'data.avatar')
df = df.drop('data')
df.display()

# COMMAND ----------

df = df.withColumn('site_address', split('email', '@')[1]).withColumn('load_date', current_date())
df.display()

# COMMAND ----------

df.write.format('delta').save('dbfs:/FileStore/site_info/person_info')
