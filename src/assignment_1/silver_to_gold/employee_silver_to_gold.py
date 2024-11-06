# Databricks notebook source
# MAGIC %run /Users/arjun37ca@gmail.com/source_to_bronze/driver
# MAGIC

# COMMAND ----------

employee_df = spark.read.format('delta').load('dbfs:/FileStore/silver/data/employee')


# COMMAND ----------

#  Find the salary of each department in descending order.

from pyspark.sql.functions import avg
employee_df.groupBy('department').agg(avg('salary').alias('avg_salary')).orderBy('avg_salary', ascending=False).display()


# COMMAND ----------

#  Find the number of employees in each department located in each country.
employee_df.groupBy('country').agg(count('*').alias('count')).select('country', 'count').display()

# COMMAND ----------

# List the department names along with their corresponding country names.
employee_df.join(country_df, employee_df.country==country_df.CountryCode, 'inner').select('department', 'CountryName').distinct().orderBy('department').display()

# COMMAND ----------

#  What is the average age of employees in each department
employee_df.groupBy('department').agg(avg('age').alias('avg_age')).orderBy('avg_age').display()

# COMMAND ----------


