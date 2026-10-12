"""
Glue Job: mfg-raw-to-bronze-supplier
Raw (JSON with nested array) → Bronze Iceberg table: mfg_bronze_db.bronze_supplier

Key transformation: supplier_details array is exploded and pivoted to columns.
  Input:  { supplier_id, supplier_machine_id, supplier_details: [{detail_key, detail_value}] }
  Output: { supplier_id, supplier_machine_id, company_name, country, contact_email,
            lead_time_days, rating }
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit, explode, col, first

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "LOAD_BATCH"])

BUCKET_NAME  = args["BUCKET_NAME"].strip()
LOAD_BATCH   = args["LOAD_BATCH"].strip()
SOURCE_PATH  = f"s3://{BUCKET_NAME}/raw/{LOAD_BATCH}/supplier/supplier.json"
SOURCE_PATH  = SOURCE_PATH.replace(" ","")
TARGET_TABLE = "glue_catalog.mfg_bronze_db.bronze_supplier"
WAREHOUSE    = f"s3://{BUCKET_NAME}/"

# Iceberg config must be set BEFORE the SparkContext is created
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read JSON array (multiline)
df_raw = spark.read \
    .option("multiline", "true") \
    .json(SOURCE_PATH)

print(f"Raw supplier records: {df_raw.count()}")
df_raw.printSchema()

# Step 1: Explode the supplier_details array into individual rows
df_exploded = df_raw.select(
    col("supplier_id"),
    col("supplier_machine_id"),
    explode(col("supplier_details")).alias("detail")
).select(
    col("supplier_id"),
    col("supplier_machine_id"),
    col("detail.detail_key").alias("detail_key"),
    col("detail.detail_value").alias("detail_value")
)

# Step 2: Pivot key-value pairs to columns
df_pivot = df_exploded \
    .groupBy("supplier_id", "supplier_machine_id") \
    .pivot("detail_key", ["company_name", "country", "contact_email", "lead_time_days", "rating"]) \
    .agg(first("detail_value"))

print(f"After pivot: {df_pivot.count()} records")
df_pivot.show(truncate=False)

# Step 3: Add audit columns
df_final = df_pivot \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit(SOURCE_PATH)) \
    .withColumn("_load_batch", lit(LOAD_BATCH))

# Create Iceberg table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        supplier_id         STRING,
        supplier_machine_id STRING,
        company_name        STRING,
        country             STRING,
        contact_email       STRING,
        lead_time_days      STRING,
        rating              STRING,
        _ingest_timestamp   TIMESTAMP,
        _source_file        STRING,
        _load_batch         STRING
    )
    USING iceberg
    PARTITIONED BY (_load_batch)
    LOCATION 's3://{BUCKET_NAME}/bronze/supplier/'
    TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
""")

df_final.writeTo(TARGET_TABLE).append()
print(f"Loaded {df_final.count()} records to {TARGET_TABLE}")
job.commit()
