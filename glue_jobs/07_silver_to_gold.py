"""
Glue Job: mfg-silver-to-gold
Silver -> Gold layer: computes 3 aggregated report tables.

Parameters:
  --BUCKET_NAME  : S3 bucket name
  --REPORT_DATE  : Date string YYYY-MM-DD (used to partition gold_supplier_performance)

Required job parameter:
  --datalake-formats iceberg
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    current_timestamp, lit, col, to_date,
    count, sum as spark_sum, avg, round as spark_round, when,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "REPORT_DATE"])

BUCKET_NAME = args["BUCKET_NAME"].strip()
REPORT_DATE = args["REPORT_DATE"].strip()
WAREHOUSE   = f"s3://{BUCKET_NAME}/"

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

NOW = current_timestamp()

OPS_SUMMARY_TABLE  = "glue_catalog.mfg_gold_db.gold_operation_summary"
DEFECT_TABLE       = "glue_catalog.mfg_gold_db.gold_defect_analysis"
SUPPLIER_PERF_TABLE = "glue_catalog.mfg_gold_db.gold_supplier_performance"


def align_to_table(df, table):
    """Select and cast columns to match the target table's schema exactly."""
    target = spark.table(table).schema
    return df.select([col(f.name).cast(f.dataType) for f in target.fields])


print(f"Starting Silver -> Gold | report_date={REPORT_DATE}")

# Load silver tables
ops       = spark.table("glue_catalog.mfg_silver_db.silver_operation_log")
suppliers = spark.table("glue_catalog.mfg_silver_db.silver_supplier").filter("is_current = true")
machines  = spark.table("glue_catalog.mfg_silver_db.silver_machine").filter("is_current = true")
materials = spark.table("glue_catalog.mfg_silver_db.silver_material").filter("is_current = true")

print(f"Operation logs:    {ops.count()}")
print(f"Current suppliers: {suppliers.count()}")
print(f"Current machines:  {machines.count()}")
print(f"Current materials: {materials.count()}")


# ─────────────────────────────────────────────
# GOLD TABLE 1: gold_operation_summary
# ─────────────────────────────────────────────
print("\n--- Building gold_operation_summary ---")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {OPS_SUMMARY_TABLE} (
        workstation_id        STRING,
        operation_type        STRING,
        report_date           DATE,
        total_work_orders     BIGINT,
        total_quantity        BIGINT,
        total_defects         BIGINT,
        avg_quality_score     DECIMAL(5,2),
        defect_rate_pct       DECIMAL(5,2),
        completed_orders      BIGINT,
        failed_orders         BIGINT,
        avg_duration_minutes  DECIMAL(8,2),
        _computed_at          TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (report_date)
    LOCATION 's3://{BUCKET_NAME}/gold/operation_summary/'
    TBLPROPERTIES ('format-version'='2')
""")

ops_summary = ops \
    .withColumn("report_date", to_date(col("start_time"))) \
    .groupBy("workstation_id", "operation_type", "report_date") \
    .agg(
        count("oplog_id").alias("total_work_orders"),
        spark_sum("quantity_processed").alias("total_quantity"),
        spark_sum("defects_found").alias("total_defects"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score"),
        spark_round(
            spark_sum(col("defects_found").cast("double")) /
            spark_sum(col("quantity_processed").cast("double")) * 100, 2
        ).alias("defect_rate_pct"),
        spark_sum(when(col("status") == "Completed", 1).otherwise(0)).alias("completed_orders"),
        spark_sum(when(col("status") == "Failed", 1).otherwise(0)).alias("failed_orders"),
        spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes"),
    ) \
    .withColumn("_computed_at", NOW)

ops_summary = align_to_table(ops_summary, OPS_SUMMARY_TABLE).cache()
ops_summary.writeTo(OPS_SUMMARY_TABLE).overwritePartitions()
print(f"  Wrote {ops_summary.count()} rows to gold_operation_summary")


# ─────────────────────────────────────────────
# GOLD TABLE 2: gold_defect_analysis
# ─────────────────────────────────────────────
print("\n--- Building gold_defect_analysis ---")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DEFECT_TABLE} (
        defect_type           STRING,
        workstation_id        STRING,
        machine_id            STRING,
        report_date           DATE,
        occurrence_count      BIGINT,
        total_defective_units BIGINT,
        avg_quality_score     DECIMAL(5,2),
        pct_of_total_defects  DECIMAL(5,2),
        _computed_at          TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (report_date)
    LOCATION 's3://{BUCKET_NAME}/gold/defect_analysis/'
    TBLPROPERTIES ('format-version'='2')
""")

# Percentage is each group's share of that day's total defects
day_window = Window.partitionBy("report_date")

defect_df = ops.filter(col("defects_found") > 0) \
    .withColumn("report_date", to_date(col("start_time"))) \
    .groupBy("defect_type", "workstation_id", "machine_id", "report_date") \
    .agg(
        count("oplog_id").alias("occurrence_count"),
        spark_sum("defects_found").alias("total_defective_units"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score"),
    ) \
    .withColumn("pct_of_total_defects",
        spark_round(
            col("total_defective_units").cast("double")
            / spark_sum("total_defective_units").over(day_window) * 100,
            2,
        )
    ) \
    .withColumn("_computed_at", NOW)

defect_df = align_to_table(defect_df, DEFECT_TABLE).cache()
defect_df.writeTo(DEFECT_TABLE).overwritePartitions()
print(f"  Wrote {defect_df.count()} rows to gold_defect_analysis")


# ─────────────────────────────────────────────
# GOLD TABLE 3: gold_supplier_performance
# ─────────────────────────────────────────────
print("\n--- Building gold_supplier_performance ---")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SUPPLIER_PERF_TABLE} (
        supplier_id         STRING,
        company_name        STRING,
        country             STRING,
        total_machines      BIGINT,
        active_machines     BIGINT,
        inactive_machines   BIGINT,
        avg_lead_time_days  DECIMAL(5,1),
        avg_rating          DECIMAL(3,1),
        total_materials     BIGINT,
        avg_material_cost   DECIMAL(10,2),
        report_date         DATE,
        _computed_at        TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (report_date)
    LOCATION 's3://{BUCKET_NAME}/gold/supplier_performance/'
    TBLPROPERTIES ('format-version'='2')
""")

# Supplier base: aggregated attributes
supplier_agg = suppliers \
    .groupBy("supplier_id", "company_name", "country") \
    .agg(
        spark_round(avg(col("lead_time_days").cast("double")), 1).alias("avg_lead_time_days"),
        spark_round(avg(col("rating").cast("double")), 1).alias("avg_rating"),
    )

# Machine stats per supplier
machine_stats = machines \
    .groupBy("supplier_id") \
    .agg(
        count("machine_id").alias("total_machines"),
        spark_sum(when(col("status") == "Active", 1).otherwise(0)).alias("active_machines"),
        spark_sum(when(col("status") != "Active", 1).otherwise(0)).alias("inactive_machines"),
    )

# Material stats per supplier
material_stats = materials \
    .groupBy("supplier_id") \
    .agg(
        count("material_id").alias("total_materials"),
        spark_round(avg(col("unit_cost").cast("double")), 2).alias("avg_material_cost"),
    )

# Join all
perf_df = supplier_agg \
    .join(machine_stats, "supplier_id", "left") \
    .join(material_stats, "supplier_id", "left") \
    .withColumn("report_date", to_date(lit(REPORT_DATE))) \
    .withColumn("_computed_at", NOW) \
    .fillna({"total_machines": 0, "active_machines": 0, "inactive_machines": 0,
             "total_materials": 0, "avg_material_cost": 0.0})

perf_df = align_to_table(perf_df, SUPPLIER_PERF_TABLE).cache()
perf_df.writeTo(SUPPLIER_PERF_TABLE).overwritePartitions()
print(f"  Wrote {perf_df.count()} rows to gold_supplier_performance")

print("\nSilver -> Gold complete.")
job.commit()