from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,posexplode,split, when,regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2
import os
from pathlib import Path


spark = SparkSession.builder.appName("OfferListingTransform").config("spark.driver.extraClassPath",Path("Code\postgresql-42.6.0.jar") ) \
    .getOrCreate()


schema =StructType([
    StructField("id", StringType(), True),
    StructField("created", StringType(), True),
    StructField("lead_id", StringType(), True),
    StructField("algorithm", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("partner_id_ranking", StringType(), True),
    StructField("no_valid_offers", StringType(), True)
])


#change csv location as required

df = spark.read.csv("C:\jeff-app\Code\offer_listings_202309130438.csv",header = True, schema=schema)

#Replace empty strings "{}" with None
df = df.withColumn("partner_id_ranking", when(col("partner_id_ranking")=="{}", None).otherwise(col("partner_id_ranking")))


# Split the partner_id_ranking column into an array
df = df.withColumn("partners_array", split(col("partner_id_ranking"), ","))

# Explode the array to create individual rows
df = df.select(
    "id", "created", "lead_id", "algorithm", "country_code",
    posexplode(col("partners_array")).alias("position", "partner"),
    "no_valid_offers"
)
# remove { and } after transformation.


df = df.withColumn("partner", regexp_replace(col("partner"),"[{}]",""))

# Define the output CSV filename
output_csv = "transformed_data.csv"

# Write the DataFrame to a CSV file
df.coalesce(1).write.csv(output_csv, header=True, mode="overwrite")

# Stop the Spark session

spark.stop()

print(f"Transformed data saved to {output_csv}")
