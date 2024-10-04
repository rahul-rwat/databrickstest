# Databricks notebook source
dbutils.fs.mkdirs("/FileStore/tables/capstone/raw")

# COMMAND ----------


dbutils.fs.mkdirs("/FileStore/tables/capstone/gold")
/FileStore/tables/capstone/raw/Crimes___2022_20240930.csv

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType



schema = StructType([
    StructField("ID", IntegerType(), nullable=True),
    StructField("Case_Number", StringType(), nullable=True),
    StructField("Date", StringType(), nullable=True),
    StructField("Block", StringType(), nullable=True),
    StructField("IUCR", StringType(), nullable=True),
    StructField("Primary_Type", StringType(), nullable=True),
    StructField("Description", StringType(), nullable=True),
    StructField("Location_Description", StringType(), nullable=True),
    StructField("Arrest", BooleanType(), nullable=True),
    StructField("Domestic", BooleanType(), nullable=True),
    StructField("Beat", IntegerType(), nullable=True),
    StructField("District", IntegerType(), nullable=True),
    StructField("Ward", IntegerType(), nullable=True),
    StructField("Community_Area", IntegerType(), nullable=True),
    StructField("FBI_Code", StringType(), nullable=True),
    StructField("X_Coordinate", IntegerType(), nullable=True),
    StructField("Y_Coordinate", IntegerType(), nullable=True),
    StructField("Year", IntegerType(), nullable=True),
    StructField("Updated_On", StringType(), nullable=True),
    StructField("Latitude", DoubleType(), nullable=True),
    StructField("Longitude", DoubleType(), nullable=True),
    StructField("Location", StringType(), nullable=True)
])


csv_file_path = 'dbfs:/FileStore/tables/capstone/raw/'
bronze_table_path= 'dbfs:/FileStore/tables/capstone/bronzenew/'

df = spark.readStream.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(csv_file_path)


query = df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", bronze_table_path + "checkpoint/") \
    .start(bronze_table_path)



# COMMAND ----------

bronze = spark.read.format("delta").load("/FileStore/tables/capstone/bronze/")

bronze.display()

# COMMAND ----------

bronze = bronze.toDF(*[col.replace(" ", "_") for col in df.columns])


# COMMAND ----------

bronze.printSchema()


# COMMAND ----------

bronze_table_path ="/FileStore/tables/capstone/bronze/"
df.write.format("delta").mode("overwrite").save(bronze_table_path)
bronze = spark.read.format("delta").load(bronze_table_path)



# COMMAND ----------

from pyspark.sql.functions import col, when, count, round
total_rows = bronze.count()
print(f"Total Rows: {total_rows}")

null_counts = bronze.select([count(when(col(c).isNull(), c)).alias(c) for c in bronze.columns])
null_percentage = null_counts.select(
    [(round((col(c) / total_rows) * 100, 2)).alias(c + '_null_percentage') for c in bronze.columns]
)

null_percentage.display()


# COMMAND ----------

silver = bronze.select(
    "ID",
    "Case_Number",
    "Date",
    "Primary_Type",
    "Description",
    "Arrest",
    "Domestic",
    "District",
    "Ward",
    "Community_Area",
    "Year",
    "Location",
    "Updated_On"
)

silver.display()

# COMMAND ----------

silver = silver.filter(col("Location").isNotNull())


# COMMAND ----------

silver.display()

# COMMAND ----------

silver_table_path = "/FileStore/tables/capstone/silver/"
silver.write.format("delta").mode("overwrite").save(silver_table_path)


# COMMAND ----------

# MAGIC %md
# MAGIC GOLD 
# MAGIC

# COMMAND ----------

crime_type_counts = silver.groupBy("Primary_Type").agg(count("*").alias("crime_count")).orderBy("crime_count", ascending=False)
crime_type_counts.display()

# COMMAND ----------


domestic_crime_counts = silver.groupBy("Domestic").agg(count("*").alias("domestic crime "))
display(domestic_crime_counts)


# COMMAND ----------

from pyspark.sql import functions as F

silver_with_hour = silver.withColumn("Hour", F.hour(F.to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a")))

crime_hour_counts = silver_with_hour.groupBy("Primary_Type", "Hour").agg(F.count("*").alias("crime_count"))

heatmap_data = crime_hour_counts.groupBy("Hour").pivot("Primary_Type").sum("crime_count").orderBy("Hour", ascending=True).fillna(0)
heatmap_data.display()


# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS silver USING DELTA LOCATION '/FileStore/tables/capstone/silver/'")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT District, COUNT(*) AS crime_count
# MAGIC FROM silver
# MAGIC GROUP BY District
# MAGIC ORDER BY crime_count DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select arrest from silver;
