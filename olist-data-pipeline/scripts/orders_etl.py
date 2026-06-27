from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col, year, month, datediff 

### Create Spark Session ###
spark = (
    SparkSession.builder
    .appName('Olist Silver ETL')
    .getOrCreate()
)

print("Spark Version:", spark.version)

### Define Schema Explicitly ###
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True)
])

### Read Bronze Data ###
csv_file_path = "s3://olist-bronze-data-lake-ap-south-1-strawhat/olist/raw_statistics/orders/"

df = (
    spark.read \
    .option("header", "true") \
    #.option("inferSchema", "true") \
    .csv(csv_file_path)
)

# The \ is called the line continuation character in Python.
# This statement continues on the next line. Don't treat the newline as the end of the statement.
# option("inferSchema", "true"): Spark looks at the data and guesses the appropriate data type for each column.
# Why avoid inferSchema in production?
# 1. Spark has to scan the file first : slower.
# 2. Data type inference can be wrong.
# 3. Schema may change unexpectedly if bad data appears.
# Production version: Define an explicit StructType schema for reliability and better performance.

### Remove null order_id ###
df = df.filter(col("order_id").isNotNull())

### Remove duplicates ###
df = df.dropDuplicates(['order_id'])

# Need to build an expression/condition? → use col('column_name')
# Need to specify column names only? → use 'column_name' or ['column_name']

### Fill missing values ###
df = df.fillna({
    "order_status": "UNKNOWN"
})

# Replace NULL values in 'order_status' with 'UNKNOWN' using -
# - a dictionary {column_name: replacement_value, column_name: replacement_value}.

### Type Casting ###
df = df.withColumn(
    "order_purchase_timestamp",
    col("order_purchase_timestamp").cast("timestamp")
)

# Convert 'order_purchase_timestamp' from string to TimestampType for date/time operations.
# withColumn() Creates a new column or replaces an existing one.
# Since you're using the same column name ("order_purchase_timestamp"), the original string -
# - column is replaced with the timestamp column.

### Create Partition Columns ###
df = (
    df.withColumn(
        "purchase_year",
        year("order_purchase_timestamp")
    )
    .withColumn(
        "purchase_month",
        month("order_purchase_timestamp")
    )
)

# Extracting Year and Month from timestamp and creates a column for each.

### Convert CSV to Parquet ###
df.write \
    .mode("overwrite") \
    .partitionBy("purchase_year", "purchase_month") \
    .parquet("s3://olist-silver-data-lake-ap-south-1-strawhat/olist/orders/")

# .mode("overwrite")
# If data already exists at path, Spark deletes it and writes the new data.
# Other modes:
#   overwrite → Replace existing data
#   append → Add new data
#   ignore → Do nothing if data exists
#   error → Throw an error if data exists
# .partitionBy("year", "month"). Creates folders based on the values of year and month.
# Path
# ├── year=2017/
# │   ├── month=10/
# │   └── month=11/
# └── year=2018/
#     └── month=01/
# Each folder contains Parquet files for that partition.
# Why partition? Because queries can read only the required folders instead of scanning the entire dataset.
#Example: 
# SELECT *
# FROM orders
# WHERE year = 2018 AND month = 1;
# Spark/Athena reads only: year=2018/month=01/
# .parquet(...): Writes the data in Parquet format, which is: Columnar, Compressed ,Faster for analytics, -
# - Widely used in data lakes

print("Silver layer created successfully.")

spark.stop()