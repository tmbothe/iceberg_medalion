"""
One-shot cleanup job: drops all mfg Iceberg tables from Glue catalog
so they can be recreated pointing to the correct S3 bucket.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])

BUCKET_NAME = args["BUCKET_NAME"].strip()
WAREHOUSE   = f"s3://{BUCKET_NAME}/"

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

TABLES = [
    "glue_catalog.mfg_bronze_db.bronze_operator",
    "glue_catalog.mfg_bronze_db.bronze_machine",
    "glue_catalog.mfg_bronze_db.bronze_material",
    "glue_catalog.mfg_bronze_db.bronze_supplier",
    "glue_catalog.mfg_bronze_db.bronze_operation_log",
    "glue_catalog.mfg_silver_db.silver_operator",
    "glue_catalog.mfg_silver_db.silver_machine",
    "glue_catalog.mfg_silver_db.silver_material",
    "glue_catalog.mfg_silver_db.silver_supplier",
    "glue_catalog.mfg_silver_db.silver_operation_log",
    "glue_catalog.mfg_gold_db.gold_operation_summary",
    "glue_catalog.mfg_gold_db.gold_defect_analysis",
    "glue_catalog.mfg_gold_db.gold_supplier_performance",
]

for table in TABLES:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"Dropped: {table}")
    except Exception as e:
        print(f"Could not drop {table}: {e}")

print("Cleanup complete.")
job.commit()
