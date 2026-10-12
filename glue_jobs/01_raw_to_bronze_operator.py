"""
Glue Job: mfg-raw-to-bronze-operator
Raw (CSV) → Bronze Iceberg table: mfg_bronze_db.bronze_operator
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "LOAD_BATCH"])

BUCKET_NAME = args["BUCKET_NAME"].strip()
LOAD_BATCH  = args["LOAD_BATCH"].strip()
WAREHOUSE   = f"s3://{BUCKET_NAME}/"

# Catalog configs set at SparkContext creation (not static — safe to set here)
conf = SparkConf()
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH  = f"s3://{BUCKET_NAME}/raw/{LOAD_BATCH}/operator/operator.csv"
TARGET_TABLE = "glue_catalog.mfg_bronze_db.bronze_operator"

# Read source CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(SOURCE_PATH)

# Add audit columns (all source columns kept as strings - Bronze contract)
df = df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit(SOURCE_PATH)) \
    .withColumn("_load_batch", lit(LOAD_BATCH))

print(f"Records read from source: {df.count()}")
df.printSchema()

# Create Iceberg table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        operator_id        STRING,
        operator_name      STRING,
        workstation_id     STRING,
        skill_level        STRING,
        hire_date          STRING,
        shift              STRING,
        status             STRING,
        _ingest_timestamp  TIMESTAMP,
        _source_file       STRING,
        _load_batch        STRING
    )
    USING iceberg
    PARTITIONED BY (_load_batch)
    LOCATION 's3://{BUCKET_NAME}/bronze/operator/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy'
    )
""")

# Append to bronze table
df.writeTo(TARGET_TABLE).append()

print(f"Successfully loaded {df.count()} records to {TARGET_TABLE} [batch={LOAD_BATCH}]")
job.commit()
