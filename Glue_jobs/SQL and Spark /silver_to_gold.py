# ==========================================================
# PROJECT: EU-RU PHARMA DATA ENGINEERING
# STEP 4: AWS GLUE SILVER TO GOLD PROCESSING
# ==========================================================

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ==========================================================
# 1. GLUE JOB PARAMETERS
# ==========================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "PROCESSED_BUCKET",
        "BATCH_ID"
    ]
)

PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
BATCH_ID = args["BATCH_ID"]


# ==========================================================
# 2. INITIALIZE SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("EU_Pharma_Gold_Processing")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate()
)

glue_context = GlueContext(spark.sparkContext)

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ==========================================================
# 3. DEFINE S3 PATHS
# ==========================================================

SILVER_BASE = f"s3://{PROCESSED_BUCKET}/silver"

GOLD_BASE = f"s3://{PROCESSED_BUCKET}/gold"


def silver_path(dataset):
    return (
        f"{SILVER_BASE}/{dataset}/"
        f"batch_id={BATCH_ID}/"
    )


# ==========================================================
# 4. READ SILVER PARQUET DATA
# ==========================================================

hcp_df = spark.read.parquet(
    silver_path("hcp_profile")
)

sales_df = spark.read.parquet(
    silver_path("sales")
)

visits_df = spark.read.parquet(
    silver_path("visits")
)

territory_df = spark.read.parquet(
    silver_path("territory")
)

logger.info("EU Silver datasets loaded successfully")


# ==========================================================
# 5. SAFETY: PROCESS ONLY EU RECORDS
# ==========================================================

hcp_df = hcp_df.filter(F.col("region") == "EU")
sales_df = sales_df.filter(F.col("region") == "EU")
visits_df = visits_df.filter(F.col("region") == "EU")
territory_df = territory_df.filter(F.col("region") == "EU")


# ==========================================================
# 6. MONTHLY DOCTOR-WISE SALES
# ==========================================================

monthly_sales_df = (
    sales_df
    .groupBy("hcpid", "year", "month")
    .agg(
        F.sum("amount").alias("total_sales"),
        F.sum("qty").alias("total_quantity"),
        F.countDistinct("salesid").alias("sales_count")
    )
)


# ==========================================================
# 7. MONTHLY DOCTOR VISITS
# ==========================================================

monthly_visits_df = (
    visits_df
    .groupBy("hcpid", "year", "month")
    .agg(
        F.countDistinct("visit_id").alias("visit_count"),
        F.max("visit_date").alias("last_visit_date")
    )
)


# ==========================================================
# 8. AVERAGE GAP BETWEEN VISITS
# ==========================================================

visit_window = (
    Window
    .partitionBy("hcpid")
    .orderBy("visit_date", "visit_id")
)

visit_gap_df = (
    visits_df
    .withColumn(
        "previous_visit_date",
        F.lag("visit_date").over(visit_window)
    )
    .withColumn(
        "gap_days",
        F.datediff(
            F.col("visit_date"),
            F.col("previous_visit_date")
        )
    )
)

avg_gap_df = (
    visit_gap_df
    .groupBy("hcpid")
    .agg(
        F.avg("gap_days").alias("avg_gap_days")
    )
)


# ==========================================================
# 9. HCP AND TERRITORY ENRICHMENT
# ==========================================================

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


# ==========================================================
# 10. BUILD MONTHLY KPI KEYS
# ==========================================================

monthly_keys_df = (
    monthly_sales_df
    .select("hcpid", "year", "month")
    .unionByName(
        monthly_visits_df.select("hcpid", "year", "month")
    )
    .distinct()
)


# ==========================================================
# 11. JOIN MONTHLY KPIs
# ==========================================================

hcp_kpi_df = (
    monthly_keys_df
    .join(
        monthly_sales_df,
        ["hcpid", "year", "month"],
        "left"
    )
    .join(
        monthly_visits_df,
        ["hcpid", "year", "month"],
        "left"
    )
    .join(
        hcp_territory_df,
        "hcpid",
        "inner"
    )
    .join(
        avg_gap_df,
        "hcpid",
        "left"
    )
    .fillna({
        "total_sales": 0.0,
        "total_quantity": 0.0,
        "sales_count": 0,
        "visit_count": 0
    })
)


# ==========================================================
# 12. VISIT FREQUENCY RECOMMENDATIONS
# ==========================================================

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


# ==========================================================
# 13. SALES PERFORMANCE CLASSIFICATION
# ==========================================================

hcp_kpi_df = (
    hcp_kpi_df
    .withColumn(
        "sales_performance",
        F.when(F.col("total_sales") >= 10000, "HIGH")
        .when(F.col("total_sales") >= 5000, "MEDIUM")
        .otherwise("LOW")
    )
)


# ==========================================================
# 14. TERRITORY PERFORMANCE USING SPARK SQL
# ==========================================================

hcp_kpi_df.createOrReplaceTempView("hcp_kpi")

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
    FROM hcp_kpi
    GROUP BY
        territory_id,
        territory_name,
        year,
        month
""")


# ==========================================================
# 15. PRODUCT-LEVEL SALES RANKINGS
# ==========================================================

product_sales_df = (
    sales_df
    .groupBy("product", "year", "month")
    .agg(
        F.sum("amount").alias("product_sales"),
        F.sum("qty").alias("product_quantity")
    )
)

product_window = (
    Window
    .partitionBy("year", "month")
    .orderBy(F.desc("product_sales"))
)

product_ranking_df = (
    product_sales_df
    .withColumn(
        "product_rank",
        F.dense_rank().over(product_window)
    )
)


# ==========================================================
# 16. WRITE GOLD DATA TO S3
# ==========================================================

def write_gold(df, dataset):

    output_path = (
        f"{GOLD_BASE}/{dataset}/"
        f"batch_id={BATCH_ID}/"
    )

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("year", "month")
        .save(output_path)
    )

    logger.info("Gold dataset saved: %s", output_path)


write_gold(hcp_kpi_df, "hcp_performance")

write_gold(territory_performance_df, "territory_performance")

write_gold(product_ranking_df, "product_rankings")


# ==========================================================
# 17. COMPLETE GLUE JOB
# ==========================================================

logger.info("EU Pharma Gold Layer completed successfully")

job.commit()
