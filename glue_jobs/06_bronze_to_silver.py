"""
Glue Job: mfg-bronze-to-silver
Bronze → Silver layer with SCD2 for dimension tables and append for operation_log.

Parameters:
  --BUCKET_NAME  : S3 bucket name
  --LOAD_BATCH   : 'data_1' or 'data_2'
  --LOAD_TYPE    : 'initial' (data_1) or 'incremental' (data_2)
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    current_timestamp, lit, col, md5, concat_ws,
    to_date, to_timestamp, unix_timestamp, when,
    round as spark_round
)
from pyspark.sql.types import IntegerType, DecimalType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "LOAD_BATCH", "LOAD_TYPE"])

BUCKET_NAME = args["BUCKET_NAME"].strip()
LOAD_BATCH  = args["LOAD_BATCH"].strip()
LOAD_TYPE   = args["LOAD_TYPE"].strip()
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

NOW = current_timestamp()

print(f"Starting Bronze → Silver | batch={LOAD_BATCH} | type={LOAD_TYPE}")


# ─────────────────────────────────────────────
# SCD2 helper function
# ─────────────────────────────────────────────
def run_scd2(bronze_table, silver_table, silver_location, pk_col, track_cols,
             create_ddl, incoming_df):
    """
    Implements SCD2 merge pattern for dimension tables.
    """
    print(f"\n--- SCD2: {bronze_table} → {silver_table} ---")

    # Create silver table if not exists
    spark.sql(create_ddl)

    # Deduplicate incoming by PK before any comparison (guards against bronze re-runs)
    incoming = incoming_df.dropDuplicates([pk_col]).withColumn(
        "record_hash",
        md5(concat_ws("|", *[col(c) for c in track_cols]))
    )

    if LOAD_TYPE == "initial":
        silver_df = incoming \
            .withColumn("effective_from", NOW) \
            .withColumn("effective_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("_created_at", NOW) \
            .withColumn("_updated_at", NOW)
        silver_df.writeTo(silver_table).createOrReplace()
        print(f"  Initial load: inserted {incoming.count()} records")

    else:  # incremental
        existing = spark.table(silver_table).filter("is_current = true")

        joined = incoming.alias("inc").join(
            existing.select(pk_col, col("record_hash").alias("_ex_hash"),
                            col("is_current").alias("_exists")).alias("sil"),
            pk_col, "left"
        ).select(
            col("inc.*"),
            col("sil._ex_hash"),
            col("sil._exists")
        )

        new_recs  = joined.filter(col("_exists").isNull())
        unchanged = joined.filter(
            col("_exists").isNotNull() & (col("record_hash") == col("_ex_hash"))
        )
        changed   = joined.filter(
            col("_exists").isNotNull() & (col("record_hash") != col("_ex_hash"))
        )

        n_new       = new_recs.count()
        n_unchanged = unchanged.count()
        n_changed   = changed.count()
        print(f"  New: {n_new} | Changed: {n_changed} | Unchanged (skip): {n_unchanged}")

        # Expire changed records using Iceberg MERGE INTO
        if n_changed > 0:
            changed.select(pk_col).distinct().createOrReplaceTempView("changed_pks")
            spark.sql(f"""
                MERGE INTO {silver_table} t
                USING changed_pks s
                ON t.{pk_col} = s.{pk_col} AND t.is_current = true
                WHEN MATCHED THEN UPDATE SET
                    t.is_current   = false,
                    t.effective_to = current_timestamp(),
                    t._updated_at  = current_timestamp()
            """)

        # Insert new current rows for both new and changed records
        to_insert = new_recs.union(changed) \
            .drop("_ex_hash", "_exists") \
            .dropDuplicates([pk_col]) \
            .withColumn("effective_from", NOW) \
            .withColumn("effective_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("_created_at", NOW) \
            .withColumn("_updated_at", NOW)

        if (n_new + n_changed) > 0:
            to_insert.writeTo(silver_table).append()

    print(f"  Done: {silver_table}")


# ─────────────────────────────────────────────
# 1. OPERATOR (SCD2)
# ─────────────────────────────────────────────
operator_bronze = spark.table("glue_catalog.mfg_bronze_db.bronze_operator") \
    .filter(f"_load_batch = '{LOAD_BATCH}'") \
    .select("operator_id", "operator_name", "workstation_id",
            "skill_level", "hire_date", "shift", "status")

run_scd2(
    bronze_table    = "mfg_bronze_db.bronze_operator",
    silver_table    = "glue_catalog.mfg_silver_db.silver_operator",
    silver_location = f"s3://{BUCKET_NAME}/silver/operator/",
    pk_col          = "operator_id",
    track_cols      = ["operator_name", "workstation_id", "skill_level", "shift", "status"],
    create_ddl      = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.mfg_silver_db.silver_operator (
            operator_id    STRING,
            operator_name  STRING,
            workstation_id STRING,
            skill_level    STRING,
            hire_date      STRING,
            shift          STRING,
            status         STRING,
            effective_from TIMESTAMP,
            effective_to   TIMESTAMP,
            is_current     BOOLEAN,
            record_hash    STRING,
            _created_at    TIMESTAMP,
            _updated_at    TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (is_current)
        LOCATION 's3://{BUCKET_NAME}/silver/operator/'
        TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
    """,
    incoming_df = operator_bronze
)


# ─────────────────────────────────────────────
# 2. MACHINE (SCD2)
# ─────────────────────────────────────────────
machine_bronze = spark.table("glue_catalog.mfg_bronze_db.bronze_machine") \
    .filter(f"_load_batch = '{LOAD_BATCH}'") \
    .select("machine_id", "machine_name", "machine_type", "supplier_id",
            "status", "purchase_date", "capacity_per_hour", "last_maintenance_date")

run_scd2(
    bronze_table    = "mfg_bronze_db.bronze_machine",
    silver_table    = "glue_catalog.mfg_silver_db.silver_machine",
    silver_location = f"s3://{BUCKET_NAME}/silver/machine/",
    pk_col          = "machine_id",
    track_cols      = ["machine_name", "machine_type", "supplier_id", "status",
                       "capacity_per_hour", "last_maintenance_date"],
    create_ddl      = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.mfg_silver_db.silver_machine (
            machine_id             STRING,
            machine_name           STRING,
            machine_type           STRING,
            supplier_id            STRING,
            status                 STRING,
            purchase_date          STRING,
            capacity_per_hour      STRING,
            last_maintenance_date  STRING,
            effective_from         TIMESTAMP,
            effective_to           TIMESTAMP,
            is_current             BOOLEAN,
            record_hash            STRING,
            _created_at            TIMESTAMP,
            _updated_at            TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (is_current)
        LOCATION 's3://{BUCKET_NAME}/silver/machine/'
        TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
    """,
    incoming_df = machine_bronze
)


# ─────────────────────────────────────────────
# 3. MATERIAL (SCD2)
# ─────────────────────────────────────────────
material_bronze = spark.table("glue_catalog.mfg_bronze_db.bronze_material") \
    .filter(f"_load_batch = '{LOAD_BATCH}'") \
    .select("material_id", "material_name", "material_type", "supplier_id",
            "unit_cost", "unit_of_measure", "stock_quantity", "reorder_level")

run_scd2(
    bronze_table    = "mfg_bronze_db.bronze_material",
    silver_table    = "glue_catalog.mfg_silver_db.silver_material",
    silver_location = f"s3://{BUCKET_NAME}/silver/material/",
    pk_col          = "material_id",
    track_cols      = ["material_name", "material_type", "supplier_id",
                       "unit_cost", "unit_of_measure", "stock_quantity"],
    create_ddl      = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.mfg_silver_db.silver_material (
            material_id       STRING,
            material_name     STRING,
            material_type     STRING,
            supplier_id       STRING,
            unit_cost         STRING,
            unit_of_measure   STRING,
            stock_quantity    STRING,
            reorder_level     STRING,
            effective_from    TIMESTAMP,
            effective_to      TIMESTAMP,
            is_current        BOOLEAN,
            record_hash       STRING,
            _created_at       TIMESTAMP,
            _updated_at       TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (is_current)
        LOCATION 's3://{BUCKET_NAME}/silver/material/'
        TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
    """,
    incoming_df = material_bronze
)


# ─────────────────────────────────────────────
# 4. SUPPLIER (SCD2) — composite PK: supplier_id + supplier_machine_id
# ─────────────────────────────────────────────
supplier_bronze = spark.table("glue_catalog.mfg_bronze_db.bronze_supplier") \
    .filter(f"_load_batch = '{LOAD_BATCH}'") \
    .withColumn("supplier_pk",
        concat_ws("~", col("supplier_id"), col("supplier_machine_id"))
    ) \
    .select("supplier_pk", "supplier_id", "supplier_machine_id",
            "company_name", "country", "contact_email",
            "lead_time_days", "rating")

run_scd2(
    bronze_table    = "mfg_bronze_db.bronze_supplier",
    silver_table    = "glue_catalog.mfg_silver_db.silver_supplier",
    silver_location = f"s3://{BUCKET_NAME}/silver/supplier/",
    pk_col          = "supplier_pk",
    track_cols      = ["company_name", "country", "contact_email",
                       "lead_time_days", "rating"],
    create_ddl      = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.mfg_silver_db.silver_supplier (
            supplier_pk         STRING,
            supplier_id         STRING,
            supplier_machine_id STRING,
            company_name        STRING,
            country             STRING,
            contact_email       STRING,
            lead_time_days      STRING,
            rating              STRING,
            effective_from      TIMESTAMP,
            effective_to        TIMESTAMP,
            is_current          BOOLEAN,
            record_hash         STRING,
            _created_at         TIMESTAMP,
            _updated_at         TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (is_current)
        LOCATION 's3://{BUCKET_NAME}/silver/supplier/'
        TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
    """,
    incoming_df = supplier_bronze
)


# ─────────────────────────────────────────────
# 5. OPERATION LOG (append-only + type cast + derived columns)
# ─────────────────────────────────────────────
print("\n--- Append: bronze_operation_log → silver_operation_log ---")

bronze_log = spark.table("glue_catalog.mfg_bronze_db.bronze_operation_log") \
    .filter(f"_load_batch = '{LOAD_BATCH}'")

silver_log = bronze_log \
    .withColumn("start_time_ts",    to_timestamp(col("start_time"))) \
    .withColumn("end_time_ts",      to_timestamp(col("end_time"))) \
    .withColumn("qty",              col("quantity_processed").cast(IntegerType())) \
    .withColumn("defects",          col("defects_found").cast(IntegerType())) \
    .withColumn("q_score",          col("quality_score").cast(DecimalType(5, 2))) \
    .withColumn("duration_minutes",
        (unix_timestamp(col("end_time_ts")) - unix_timestamp(col("start_time_ts"))) / 60.0
    ) \
    .withColumn("defect_rate",
        when(col("qty") > 0,
             spark_round(col("defects").cast("double") / col("qty") * 100, 2)
        ).otherwise(lit(0.0))
    ) \
    .select(
        col("oplog_id"),
        col("work_order_id"),
        col("machine_id"),
        col("workstation_id"),
        col("material_id"),
        col("operator_id"),
        col("operation_type"),
        col("start_time_ts").alias("start_time"),
        col("end_time_ts").alias("end_time"),
        col("duration_minutes"),
        col("status"),
        col("qty").alias("quantity_processed"),
        col("defects").alias("defects_found"),
        col("defect_rate"),
        col("defect_type"),
        col("q_score").alias("quality_score"),
        col("notes"),
        NOW.alias("_ingest_timestamp")
    )

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.mfg_silver_db.silver_operation_log (
        oplog_id           STRING,
        work_order_id      STRING,
        machine_id         STRING,
        workstation_id     STRING,
        material_id        STRING,
        operator_id        STRING,
        operation_type     STRING,
        start_time         TIMESTAMP,
        end_time           TIMESTAMP,
        duration_minutes   DOUBLE,
        status             STRING,
        quantity_processed INTEGER,
        defects_found      INTEGER,
        defect_rate        DECIMAL(5,2),
        defect_type        STRING,
        quality_score      DECIMAL(5,2),
        notes              STRING,
        _ingest_timestamp  TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(start_time))
    LOCATION 's3://{BUCKET_NAME}/silver/operation_log/'
    TBLPROPERTIES ('table_type'='ICEBERG','format-version'='2')
""")

silver_log.writeTo("glue_catalog.mfg_silver_db.silver_operation_log").append()
print(f"  Appended {silver_log.count()} operation log records")

print("\nBronze → Silver complete.")
job.commit()
