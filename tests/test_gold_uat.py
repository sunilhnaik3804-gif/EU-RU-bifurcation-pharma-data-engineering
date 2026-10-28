# ============================================================
# PROJECT: EU-RU PHARMA DATA ENGINEERING
# STEP 8: GOLD LAYER UAT VALIDATION
# ============================================================

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
BATCH_ID = os.environ["BATCH_ID"]

SILVER_BASE = f"s3://{PROCESSED_BUCKET}/silver"
GOLD_BASE = f"s3://{PROCESSED_BUCKET}/gold"


# ============================================================
# 2. INITIALIZE SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("EU_Pharma_Gold_UAT")
    .getOrCreate()
)


# ============================================================
# 3. READ SILVER AND GOLD DATA
# ============================================================

def read_silver(dataset):
    return spark.read.parquet(
        f"{SILVER_BASE}/{dataset}/batch_id={BATCH_ID}/"
    )


def read_gold(dataset):
    return spark.read.parquet(
        f"{GOLD_BASE}/{dataset}/batch_id={BATCH_ID}/"
    )


sales_df = read_silver("sales")
visits_df = read_silver("visits")
hcp_df = read_silver("hcp_profile")
territory_df = read_silver("territory")

hcp_gold_df = read_gold("hcp_performance")
territory_gold_df = read_gold("territory_performance")
product_gold_df = read_gold("product_rankings")


# ============================================================
# 4. TEST EU REGION COMPLIANCE
# ============================================================

def test_eu_region():

    for dataset_name, df in [
        ("hcp_profile", hcp_df),
        ("sales", sales_df),
        ("visits", visits_df),
        ("territory", territory_df),
        ("hcp_performance", hcp_gold_df)
    ]:

        invalid_count = df.filter(
            F.col("region").isNull()
            | (F.col("region") != "EU")
        ).count()

        assert invalid_count == 0, (
            f"{dataset_name}: Found {invalid_count} "
            "non-EU records"
        )

    logger.info("PASS: EU region validation")


# ============================================================
# 5. TEST MONTHLY SALES RECONCILIATION
# ============================================================

def test_monthly_sales():

    expected_df = (
        sales_df
        .groupBy("hcpid", "year", "month")
        .agg(
            F.sum("amount").alias("expected_sales")
        )
    )

    actual_df = (
        hcp_gold_df
        .select(
            "hcpid",
            "year",
            "month",
            "total_sales"
        )
    )

    comparison_df = (
        expected_df.alias("e")
        .join(
            actual_df.alias("a"),
            ["hcpid", "year", "month"],
            "left"
        )
    )

    mismatch_count = comparison_df.filter(
        F.col("total_sales").isNull()
        | (
            F.abs(
                F.col("expected_sales")
                - F.col("total_sales")
            ) > 0.01
        )
    ).count()

    assert mismatch_count == 0, (
        f"Monthly sales mismatches: {mismatch_count}"
    )

    logger.info("PASS: Monthly sales reconciliation")


# ============================================================
# 6. TEST PRODUCT SALES RECONCILIATION
# ============================================================

def test_product_sales():

    expected_df = (
        sales_df
        .groupBy("product", "year", "month")
        .agg(
            F.sum("amount").alias("expected_sales")
        )
    )

    comparison_df = (
        expected_df
        .join(
            product_gold_df,
            ["product", "year", "month"],
            "left"
        )
    )

    mismatch_count = comparison_df.filter(
        F.col("product_sales").isNull()
        | (
            F.abs(
                F.col("expected_sales")
                - F.col("product_sales")
            ) > 0.01
        )
    ).count()

    assert mismatch_count == 0, (
        f"Product sales mismatches: {mismatch_count}"
    )

    logger.info("PASS: Product sales reconciliation")


# ============================================================
# 7. TEST DUPLICATE GOLD RECORDS
# ============================================================

def test_gold_duplicates():

    duplicate_count = (
        hcp_gold_df
        .groupBy("hcpid", "year", "month")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_count == 0, (
        f"Duplicate HCP KPI records: {duplicate_count}"
    )

    logger.info("PASS: Gold duplicate validation")


# ============================================================
# 8. TEST NEGATIVE SALES
# ============================================================

def test_negative_sales():

    negative_count = hcp_gold_df.filter(
        F.col("total_sales") < 0
    ).count()

    assert negative_count == 0, (
        f"Negative Gold sales values: {negative_count}"
    )

    logger.info("PASS: Negative sales validation")


# ============================================================
# 9. TEST TERRITORY SALES RECONCILIATION
# ============================================================

def test_territory_sales():

    expected_df = (
        hcp_gold_df
        .groupBy(
            "territory_id",
            "territory_name",
            "year",
            "month"
        )
        .agg(
            F.sum("total_sales").alias(
                "expected_territory_sales"
            )
        )
    )

    comparison_df = (
        expected_df
        .join(
            territory_gold_df,
            [
                "territory_id",
                "territory_name",
                "year",
                "month"
            ],
            "left"
        )
    )

    mismatch_count = comparison_df.filter(
        F.col("territory_sales").isNull()
        | (
            F.abs(
                F.col("expected_territory_sales")
                - F.col("territory_sales")
            ) > 0.01
        )
    ).count()

    assert mismatch_count == 0, (
        f"Territory sales mismatches: {mismatch_count}"
    )

    logger.info("PASS: Territory sales reconciliation")


# ============================================================
# 10. EXECUTE UAT TESTS
# ============================================================

def main():

    tests = [
        test_eu_region,
        test_monthly_sales,
        test_product_sales,
        test_gold_duplicates,
        test_negative_sales,
        test_territory_sales
    ]

    failures = []

    for test in tests:

        try:
            test()
            logger.info("PASSED: %s", test.__name__)

        except Exception as error:
            logger.exception(
                "FAILED: %s",
                test.__name__
            )

            failures.append(
                {
                    "test": test.__name__,
                    "error": str(error)
                }
            )

    if failures:
        raise RuntimeError(
            f"UAT failed: {failures}"
        )

    logger.info(
        "ALL GOLD LAYER UAT TESTS PASSED"
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
