"""
Glue Job: mfg-raw-to-bronze-machine
Raw (CSV) → Bronze Iceberg table: mfg_bronze_db.bronze_machine
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "LOAD_BATCH"])

BUCKET_NAME  = args["BUCKET_NAME"].strip()
LOAD_BATCH   = args["LOAD_BATCH"].strip()
WAREHOUSE    = f"s3://{BUCKET_NAME}/"

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

SOURCE_PATH  = f"s3://{BUCKET_NAME}/raw/{LOAD_BATCH}/machine/machine.csv"
TARGET_TABLE = "glue_catalog.mfg_bronze_db.bronze_machine"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(SOURCE_PATH)

df = df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit(SOURCE_PATH)) \
    .withColumn("_load_batch", lit(LOAD_BATCH))

print(f"Records read: {df.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        machine_id             STRING,
        machine_name           STRING,
        machine_type           STRING,
        supplier_id            STRING,
        status                 STRING,
        purchase_date          STRING,
        capacity_per_hour      STRING,
        last_maintenance_date  STRING,
        _ingest_timestamp      TIMESTAMP,
        _source_file           STRING,
        _load_batch            STRING
    )
    USING iceberg
    PARTITIONED BY (_load_batch)
    LOCATION 's3://{BUCKET_NAME}/bronze/machine/'
    TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
""")

df.writeTo(TARGET_TABLE).append()
print(f"Loaded {df.count()} records to {TARGET_TABLE}")
job.commit()
