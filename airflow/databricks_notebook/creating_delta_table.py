# create delta table in databricks pyspark

## command

dbutils.widgets.text("source_path_table_1", "adf1-eastus/SalesLT_Address.csv", "source_path_table_1")
source_path_table_1 = dbutils.widgets.get("source_path_table_1")

dbutils.widgets.text("source_path_table_2", "adf2-westus/saleslt_customer.csv", "source_path_table_2")
source_path_table_2 = dbutils.widgets.get("source_path_table_2")

dbutils.widgets.text("table1", "saleslt_address", "Table1Name")
Table1Name = dbutils.widgets.get("table1")

dbutils.widgets.text("table2", "saleslt_customer", "Table2Name")
Table2Name = dbutils.widgets.get("table2")


## command

print(source_path_table_1)
print(source_path_table_2)
print(Table1Name)
print(Table2Name)


## command

df = spark.read.csv(f"/mnt/airfloew-test/source-data/{source_path_table_1}", header=True)

spark.sql('''create schema if not exists airflow_test ''')
# drop the table if it exists
dbutils.fs.rm(f"/mnt/airfloew-test/delta-tables/{Table1Name}/", recurse=True)
spark.sql('''drop table if exists airflow_test.{}'''.format(Table1Name))

# write the dataframe as delta 
df.write.format("delta").mode("overwrite").save(f"/mnt/airfloew-test/delta-tables/{Table1Name}/")
# create the delta table
path = f"/mnt/airfloew-test/delta-tables/{Table1Name}"
spark.sql('''create table airflow_test.{} using delta location "{}" '''.format(Table1Name, path))


## command

df = spark.read.csv(f"/mnt/airfloew-test/source-data/{source_path_table_2}", header=True)

spark.sql('''create schema if not exists airflow_test ''')
# drop the table if it exists
dbutils.fs.rm(f"/mnt/airfloew-test/delta-tables/{Table2Name}/", recurse=True)
spark.sql('''drop table if exists airflow_test.{}'''.format(Table2Name))

# write the dataframe as delta 
df.write.format("delta").mode("overwrite").save(f"/mnt/airfloew-test/delta-tables/{Table2Name}/")
# create the delta table
path = f"/mnt/airfloew-test/delta-tables/{Table2Name}"
spark.sql('''create table airflow_test.{} using delta location "{}" '''.format(Table2Name, path))