# Databricks notebook source
# MAGIC %run /Users/arjun37ca@gmail.com/source_to_bronze/driver
# MAGIC

# COMMAND ----------

country_df.printSchema()
department_df.printSchema()
employee_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
country_schema = StructType([StructField('countryCode', StringType()), StructField('countryName', StringType())])
department_schema = StructType([StructField('departmentId', StringType()), StructField('departmentName', StringType())])
employee_schema = StructType([StructField('employeeId', IntegerType()), StructField('employeeName', StringType()), StructField('department', StringType()), StructField('country', StringType()), StructField('salary', IntegerType()), StructField('age', IntegerType())])

# COMMAND ----------

country_df = spark.read.csv('dbfs:/FileStore/source_to_bronze/country_df.csv', schema=country_schema)
department_df = spark.read.csv('dbfs:/FileStore/source_to_bronze/department_df.csv', schema=department_schema)
employee_df = spark.read.csv('dbfs:/FileStore/source_to_bronze/employee_df.csv', schema = employee_schema)

# COMMAND ----------

country_df.display()
department_df.display()
employee_df.display()

# COMMAND ----------

def snake_case(d):
    for i in d.columns:
        sc = ''
        for c in i:
            if c.isupper():
                sc += '_'+c.lower()
            else:
                sc += c
        d = d.withColumnRenamed(i, sc)
    return d

# COMMAND ----------

country_df = snake_case(country_df)
department_df = snake_case(department_df)
employee_df = snake_case(employee_df)


# COMMAND ----------

country_df.printSchema()
department_df.printSchema()
employee_df.printSchema()

# COMMAND ----------

employee_df = employee_df.withColumn('load_date', current_date())
employee_df.write.mode('overwrite').format('delta').save('dbfs:/FileStore/silver/data/employee')

# COMMAND ----------


