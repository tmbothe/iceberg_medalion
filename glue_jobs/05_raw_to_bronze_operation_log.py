"""
Glue Job: mfg-raw-to-bronze-oplog
Raw (JSON with embedded object) → Bronze Iceberg table: mfg_bronze_db.bronze_operation_log

Key transformation: work_order_details embedded object is flattened to top-level columns.
  Input:  { oplog_id, ..., work_order_details: { operator_id, operation_type, ... } }
  Output: { oplog_id, ..., operator_id, operation_type, quantity_processed, ... }
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "LOAD_BATCH"])

BUCKET_NAME  = args["BUCKET_NAME"].strip()
LOAD_BATCH   = args["LOAD_BATCH"].strip()
SOURCE_PATH  = f"s3://{BUCKET_NAME}/raw/{LOAD_BATCH}/operation_log/operation_log.json"
SOURCE_PATH  = SOURCE_PATH.replace(" ","")
TARGET_TABLE = "glue_catalog.mfg_bronze_db.bronze_operation_log"
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

print(f"Raw operation log records: {df_raw.count()}")
df_raw.printSchema()

# Flatten work_order_details struct to top-level columns
# Cast all to STRING (Bronze contract: no type changes)
df_flat = df_raw.select(
    col("oplog_id").cast(StringType()),
    col("work_order_id").cast(StringType()),
    col("machine_id").cast(StringType()),
    col("workstation_id").cast(StringType()),
    col("material_id").cast(StringType()),
    col("start_time").cast(StringType()),
    col("end_time").cast(StringType()),
    col("status").cast(StringType()),
    col("work_order_details.operator_id").alias("operator_id").cast(StringType()),
    col("work_order_details.operation_type").alias("operation_type").cast(StringType()),
    col("work_order_details.quantity_processed").alias("quantity_processed").cast(StringType()),
    col("work_order_details.defects_found").alias("defects_found").cast(StringType()),
    col("work_order_details.defect_type").alias("defect_type").cast(StringType()),
    col("work_order_details.quality_score").alias("quality_score").cast(StringType()),
    col("work_order_details.notes").alias("notes").cast(StringType())
)

# Add audit columns
df_final = df_flat \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit(SOURCE_PATH)) \
    .withColumn("_load_batch", lit(LOAD_BATCH))

print(f"Flattened records: {df_final.count()}")
df_final.show(5, truncate=False)

# Create Iceberg table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        oplog_id            STRING,
        work_order_id       STRING,
        machine_id          STRING,
        workstation_id      STRING,
        material_id         STRING,
        start_time          STRING,
        end_time            STRING,
        status              STRING,
        operator_id         STRING,
        operation_type      STRING,
        quantity_processed  STRING,
        defects_found       STRING,
        defect_type         STRING,
        quality_score       STRING,
        notes               STRING,
        _ingest_timestamp   TIMESTAMP,
        _source_file        STRING,
        _load_batch         STRING
    )
    USING iceberg
    PARTITIONED BY (_load_batch)
    LOCATION 's3://{BUCKET_NAME}/bronze/operation_log/'
    TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
""")

df_final.writeTo(TARGET_TABLE).append()
print(f"Loaded {df_final.count()} records to {TARGET_TABLE}")
job.commit()
