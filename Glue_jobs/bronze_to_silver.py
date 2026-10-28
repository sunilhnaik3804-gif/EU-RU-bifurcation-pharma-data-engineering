# ============================================================
# PROJECT: EU-RU PHARMA DATA ENGINEERING
# STEP 5: AWS GLUE PYSPARK ETL
# SOURCE: S3 BRONZE (EU)
# TARGET: S3 SILVER + GOLD
# ============================================================

import sys
import json
import logging
from datetime import datetime, timezone

import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


# ============================================================
# 1. AWS GLUE JOB ARGUMENTS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "EU_RAW_BUCKET",
        "PROCESSED_BUCKET",
        "BATCH_ID"
    ]
)

JOB_NAME = args["JOB_NAME"]
EU_RAW_BUCKET = args["EU_RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
BATCH_ID = args["BATCH_ID"]


# ============================================================
# 2. SPARK INITIALIZATION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EU_Pharma_Data_Processing")
    .config(
        "spark.sql.adaptive.enabled",
        "true"
    )
    .config(
        "spark.sql.adaptive.skewJoin.enabled",
        "true"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "200"
    )
    .getOrCreate()
)

glue_context = GlueContext(spark.sparkContext)

job = Job(glue_context)

job.init(JOB_NAME, args)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

logger.info("EU Pharma Glue Spark job started")


# ============================================================
# 3. S3 SOURCE AND TARGET PATHS
# ============================================================

BRONZE_BASE = (
    f"s3://{EU_RAW_BUCKET}/bronze/region=EU"
)

SILVER_BASE = (
    f"s3://{PROCESSED_BUCKET}/silver/region=EU"
)

GOLD_BASE = (
    f"s3://{PROCESSED_BUCKET}/gold/region=EU"
)


def source_path(dataset):

    return (
        f"{BRONZE_BASE}/{dataset}/"
        f"batch_id={BATCH_ID}/{dataset}.json"
    )


# ============================================================
# 4. READ RAW DATA FROM S3
# ============================================================

hcp_df = spark.read.json(
    source_path("hcp_profile")
)

sales_df = spark.read.json(
    source_path("sales")
)

visits_df = spark.read.json(
    source_path("visits")
)

territory_df = spark.read.json(
    source_path("territory")
)

logger.info("All EU datasets loaded from S3 Bronze")


# ============================================================
# 5. SOURCE DATA COUNTS
# ============================================================

raw_hcp_count = hcp_df.count()
raw_sales_count = sales_df.count()
raw_visits_count = visits_df.count()
raw_territory_count = territory_df.count()

logger.info("Raw HCP count: %s", raw_hcp_count)
logger.info("Raw Sales count: %s", raw_sales_count)
logger.info("Raw Visits count: %s", raw_visits_count)
logger.info("Raw Territory count: %s", raw_territory_count)


# ============================================================
# 6. REGION VALIDATION
# ============================================================

def validate_eu_region(df, dataset):

    if "region" not in df.columns:
        raise ValueError(
            f"Region column missing in {dataset}"
        )

    invalid_count = (
        df.filter(
            F.col("region").isNull()
            |
            (
                F.upper(
                    F.trim(F.col("region"))
                ).isin("EU", "EUR", "EUROPE") == False
            )
        )
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{dataset} contains "
            f"{invalid_count} non-EU records"
        )

    return df


hcp_df = validate_eu_region(
    hcp_df, "hcp_profile"
)

sales_df = validate_eu_region(
    sales_df, "sales"
)

visits_df = validate_eu_region(
    visits_df, "visits"
)

territory_df = validate_eu_region(
    territory_df, "territory"
)


# ============================================================
# 7. STANDARDIZE REGION VALUES
# ============================================================

def standardize_region(df):

    return df.withColumn(
        "region",
        F.when(
            F.upper(
                F.trim(F.col("region"))
            ).isin("EU", "EUR", "EUROPE"),
            F.lit("EU")
        ).otherwise(
            F.upper(
                F.trim(F.col("region"))
            )
        )
    )


hcp_df = standardize_region(hcp_df)
sales_df = standardize_region(sales_df)
visits_df = standardize_region(visits_df)
territory_df = standardize_region(territory_df)


# ============================================================
# 8. HCP DATA CLEANING
# ============================================================

hcp_df = (
    hcp_df

    .withColumn(
        "hcpid",
        F.col("hcpid").cast("string")
    )

    .withColumn(
        "hcp_name",
        F.upper(
            F.trim(F.col("hcp_name"))
        )
    )

    .withColumn(
        "city",
        F.upper(
            F.trim(F.col("city"))
        )
    )

    .withColumn(
        "specialization",
        F.upper(
            F.trim(F.col("specialization"))
        )
    )

    .withColumn(
        "specialization",
        F.when(
            F.col("specialization").isin(
                "CARD",
                "CARDIOLOGY"
            ),
            "CARDIOLOGY"
        ).otherwise(
            F.col("specialization")
        )
    )

    .filter(
        F.upper(
            F.trim(F.col("status"))
        ) == "ACTIVE"
    )

    .filter(
        F.col("hcpid").isNotNull()
    )

    .dropDuplicates(["hcpid"])
)

logger.info("HCP cleansing completed")


# ============================================================
# 9. SALES DATA CLEANING
# ============================================================

sales_df = (
    sales_df

    .withColumn(
        "salesid",
        F.col("salesid").cast("string")
    )

    .withColumn(
        "hcpid",
        F.col("hcpid").cast("string")
    )

    .withColumn(
        "repid",
        F.col("repid").cast("string")
    )

    .withColumn(
        "sales_date",
        F.to_date(
            F.col("sales_date"),
            "yyyy-MM-dd"
        )
    )

    .withColumn(
        "qty",
        F.when(
            F.col("qty").cast("double") < 0,
            0
        ).otherwise(
            F.coalesce(
                F.col("qty").cast("double"),
                F.lit(0.0)
            )
        )
    )

    .withColumn(
        "amount",
        F.when(
            F.col("amount").cast("double") < 0,
            0
        ).otherwise(
            F.coalesce(
                F.col("amount").cast("double"),
                F.lit(0.0)
            )
        )
    )

    .filter(
        F.col("sales_date").isNotNull()
    )

    .filter(
        F.col("salesid").isNotNull()
    )

    .dropDuplicates(["salesid"])
)

logger.info("Sales cleansing completed")


# ============================================================
# 10. VISIT DATA CLEANING
# ============================================================

visits_df = (
    visits_df

    .withColumn(
        "visit_id",
        F.col("visit_id").cast("string")
    )

    .withColumn(
        "hcpid",
        F.col("hcpid").cast("string")
    )

    .withColumn(
        "repid",
        F.col("repid").cast("string")
    )

    .withColumn(
        "visit_date",
        F.to_date(
            F.col("visit_date"),
            "yyyy-MM-dd"
        )
    )

    .filter(
        F.col("visit_date").isNotNull()
    )

    .filter(
        F.col("visit_id").isNotNull()
    )

    .dropDuplicates(["visit_id"])
)

logger.info("Visit cleansing completed")


# ============================================================
# 11. TERRITORY STANDARDIZATION
# ============================================================

territory_df = (
    territory_df

    .withColumn(
        "hcpid",
        F.col("hcpid").cast("string")
    )

    .withColumn(
        "territory_id",
        F.upper(
            F.trim(F.col("territory_id"))
        )
    )

    .withColumn(
        "territory_name",
        F.upper(
            F.trim(F.col("territory_name"))
        )
    )

    .filter(
        F.col("hcpid").isNotNull()
    )

    .dropDuplicates(["hcpid"])
)

logger.info("Territory standardization completed")


# ============================================================
# 12. WRITE CLEAN DATA TO SILVER LAYER
# ============================================================

def write_silver(df, dataset):

    output_path = (
        f"{SILVER_BASE}/{dataset}/"
        f"batch_id={BATCH_ID}/"
    )

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .save(output_path)
    )

    logger.info(
        "Silver dataset written: %s",
        output_path
    )


write_silver(hcp_df, "hcp_profile")
write_silver(sales_df, "sales")
write_silver(visits_df, "visits")
write_silver(territory_df, "territory")


# ============================================================
# 13. MONTHLY SALES AGGREGATION
# ============================================================

sales_monthly_df = (
    sales_df

    .withColumn(
        "year",
        F.year("sales_date")
    )

    .withColumn(
        "month",
        F.month("sales_date")
    )

    .groupBy(
        "hcpid",
        "year",
        "month"
    )

    .agg(
        F.sum("amount").alias("total_sales"),
        F.sum("qty").alias("total_quantity"),
        F.countDistinct("salesid").alias(
            "sales_count"
        )
    )
)


# ============================================================
# 14. MONTHLY VISIT AGGREGATION
# ============================================================

visits_monthly_df = (
    visits_df

    .withColumn(
        "year",
        F.year("visit_date")
    )

    .withColumn(
        "month",
        F.month("visit_date")
    )

    .groupBy(
        "hcpid",
        "year",
        "month"
    )

    .agg(
        F.countDistinct("visit_id").alias(
            "visit_count"
        ),

        F.max("visit_date").alias(
            "last_visit_date"
        )
    )
)


# ============================================================
# 15. AVERAGE GAP BETWEEN DOCTOR VISITS
# ============================================================

visit_window = (
    Window
    .partitionBy("hcpid")
    .orderBy("visit_date", "visit_id")
)

visit_gap_df = (
    visits_df

    .withColumn(
        "previous_visit_date",
        F.lag("visit_date").over(
            visit_window
        )
    )

    .withColumn(
        "gap_days",
        F.datediff(
            F.col("visit_date"),
            F.col("previous_visit_date")
        )
    )
)

avg_visit_gap_df = (
    visit_gap_df

    .groupBy("hcpid")

    .agg(
        F.avg("gap_days").alias(
            "avg_gap_days"
        )
    )
)


# ============================================================
# 16. JOIN HCP AND TERRITORY
# ============================================================

hcp_territory_df = (
    hcp_df.alias("h")

    .join(
        territory_df.alias("t"),
        F.col("h.hcpid") == F.col("t.hcpid"),
        "left"
    )

    .select(
        F.col("h.hcpid").alias("hcpid"),
        F.col("h.hcp_name"),
        F.col("h.specialization"),
        F.col("h.city"),
        F.col("h.region"),
        F.col("t.territory_id"),
        F.col("t.territory_name")
    )
)


# ============================================================
# 17. MONTHLY HCP BUSINESS KPIs
# ============================================================

monthly_keys_df = (
    sales_monthly_df.select(
        "hcpid", "year", "month"
    )

    .unionByName(
        visits_monthly_df.select(
            "hcpid", "year", "month"
        )
    )

    .distinct()
)

hcp_kpi_df = (
    monthly_keys_df.alias("k")

    .join(
        sales_monthly_df.alias("s"),
        ["hcpid", "year", "month"],
        "left"
    )

    .join(
        visits_monthly_df.alias("v"),
        ["hcpid", "year", "month"],
        "left"
    )

    .join(
        hcp_territory_df.alias("h"),
        "hcpid",
        "left"
    )

    .join(
        avg_visit_gap_df.alias("g"),
        "hcpid",
        "left"
    )

    .filter(
        F.col("hcp_name").isNotNull()
    )

    .fillna({
        "total_sales": 0.0,
        "total_quantity": 0.0,
        "sales_count": 0,
        "visit_count": 0
    })
)


# ============================================================
# 18. VISIT FREQUENCY RECOMMENDATION
# ============================================================

hcp_kpi_df = (
    hcp_kpi_df

    .withColumn(
        "recommended_frequency",

        F.when(
            F.col("avg_gap_days").isNull(),
            "REVIEW"
        )

        .when(
            F.col("avg_gap_days") <= 7,
            "WEEKLY"
        )

        .when(
            F.col("avg_gap_days") <= 14,
            "BIWEEKLY"
        )

        .otherwise("MONTHLY")
    )
)


# ============================================================
# 19. SPARK SQL BUSINESS LOGIC
# ============================================================

hcp_kpi_df.createOrReplaceTempView(
    "hcp_monthly_kpi"
)

doctor_performance_df = spark.sql("""

    SELECT
        hcpid,
        hcp_name,
        specialization,
        city,
        region,
        territory_id,
        territory_name,
        year,
        month,

        total_sales,
        total_quantity,
        sales_count,
        visit_count,

        last_visit_date,
        avg_gap_days,
        recommended_frequency,

        CASE
            WHEN total_sales >= 10000
                THEN 'HIGH'

            WHEN total_sales >= 5000
                THEN 'MEDIUM'

            ELSE 'LOW'
        END AS sales_performance

    FROM hcp_monthly_kpi

""")


# ============================================================
# 20. TERRITORY PERFORMANCE AGGREGATION
# ============================================================

doctor_performance_df.createOrReplaceTempView(
    "doctor_performance"
)

territory_performance_df = spark.sql("""

    SELECT
        territory_id,
        territory_name,
        year,
        month,

        COUNT(DISTINCT hcpid) AS total_doctors,

        SUM(total_sales) AS territory_sales,

        SUM(visit_count) AS total_visits,

        SUM(total_quantity) AS total_quantity

    FROM doctor_performance

    GROUP BY
        territory_id,
        territory_name,
        year,
        month

""")


# ============================================================
# 21. WRITE GOLD DATA AS PARTITIONED PARQUET
# ============================================================

doctor_output_path = (
    f"{GOLD_BASE}/doctor_performance/"
)

territory_output_path = (
    f"{GOLD_BASE}/territory_performance/"
)

(
    doctor_performance_df.write

    .mode("overwrite")

    .format("parquet")

    .partitionBy("year", "month")

    .save(doctor_output_path)
)

(
    territory_performance_df.write

    .mode("overwrite")

    .format("parquet")

    .partitionBy("year", "month")

    .save(territory_output_path)
)

logger.info("Gold datasets written successfully")


# ============================================================
# 22. PROCESSING METRICS
# ============================================================

metrics = {
    "project": "EU-RU Pharma Bifurcation",
    "region": "EU",
    "batch_id": BATCH_ID,
    "raw_hcp_count": raw_hcp_count,
    "raw_sales_count": raw_sales_count,
    "raw_visits_count": raw_visits_count,
    "raw_territory_count": raw_territory_count,
    "silver_hcp_count": hcp_df.count(),
    "silver_sales_count": sales_df.count(),
    "silver_visits_count": visits_df.count(),
    "silver_territory_count": territory_df.count(),
    "gold_doctor_kpi_count": doctor_performance_df.count(),
    "gold_territory_count": territory_performance_df.count(),
    "completed_at": datetime.now(
        timezone.utc
    ).isoformat()
}

metrics_key = (
    f"logs/glue_metrics/"
    f"batch_id={BATCH_ID}/metrics.json"
)

s3.put_object(
    Bucket=PROCESSED_BUCKET,
    Key=metrics_key,
    Body=json.dumps(metrics, indent=2),
    ContentType="application/json"
)

logger.info(
    "Processing metrics: %s",
    json.dumps(metrics)
)


# ============================================================
# 23. COMMIT AWS GLUE JOB
# ============================================================

job.commit()

logger.info(
    "EU Pharma Glue ETL job completed successfully"
)
