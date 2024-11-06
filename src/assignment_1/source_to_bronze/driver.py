# Databricks notebook source
country_df = spark.read.csv('dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/Country_Q1.csv', header=True)
department_df = spark.read.csv('dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/Department_Q1.csv')
employee_df = spark.read.csv('dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/Employee_Q1.csv')


# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

country_df.write.mode('overwrite').format('csv').save('dbfs:/FileStore/source_to_bronze/country_df.csv')
department_df.write.mode('overwrite').format('csv').save('dbfs:/FileStore/source_to_bronze/department_df.csv')
employee_df.write.mode('overwrite').format('csv').save('dbfs:/FileStore/source_to_bronze/employee_df.csv')

# COMMAND ----------


