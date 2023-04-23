# create delta table in databricks pyspark

## command

df = spark.read.csv("/mnt/airfloew-test/source-data/adf1-eastus/SalesLT_Address.csv", header=True)

spark.sql('''create schema if not exists airflow_test ''')
# drop the table if it exists
dbutils.fs.rm("/mnt/airfloew-test/delta-tables/saleslt_address/", recurse=True)
spark.sql('''drop table if exists airflow_test.saleslt_address''')

# write the dataframe as delta 
df.write.format("delta").mode("overwrite").save("/mnt/airfloew-test/delta-tables/saleslt_address/")
# create the delta table
spark.sql('''create table airflow_test.saleslt_address using delta location "/mnt/airfloew-test/delta-tables/saleslt_address/"''')


## command

df = spark.read.csv("/mnt/airfloew-test/source-data/adf2-westus/saleslt_customer.csv", header=True)

spark.sql('''create schema if not exists airflow_test ''')
# drop the table if it exists
dbutils.fs.rm("/mnt/airfloew-test/delta-tables/saleslt_customer/", recurse=True)
spark.sql('''drop table if exists airflow_test.saleslt_address''')

# write the dataframe as delta 
df.write.format("delta").mode("overwrite").save("/mnt/airfloew-test/delta-tables/saleslt_customer/")
# create the delta table
spark.sql('''create table airflow_test.saleslt_customer using delta location "/mnt/airfloew-test/delta-tables/saleslt_customer/"''')