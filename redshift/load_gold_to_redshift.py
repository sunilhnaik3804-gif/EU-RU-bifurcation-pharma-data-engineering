# ============================================================
# PROJECT: EU-RU PHARMA DATA ENGINEERING
# STEP 7: AMAZON REDSHIFT GOLD LAYER LOADING
# ============================================================

import os
import logging
import re

import boto3
import redshift_connector


# ============================================================
# 1. CONFIGURATION
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]

REDSHIFT_PORT = int(
    os.environ.get("REDSHIFT_PORT", "5439")
)

PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
BATCH_ID = os.environ["BATCH_ID"]
REDSHIFT_IAM_ROLE = os.environ["REDSHIFT_IAM_ROLE"]
AWS_REGION = os.environ["AWS_REGION"]

if not re.fullmatch(r"\d{14}", BATCH_ID):
    raise ValueError("BATCH_ID must contain 14 digits")

GOLD_BASE = f"s3://{PROCESSED_BUCKET}/gold"

DATASETS = {
    "hcp_performance": "gold_hcp_performance",
    "territory_performance": "gold_territory_performance",
    "product_rankings": "gold_product_rankings"
}

s3 = boto3.client("s3")


# ============================================================
# 2. CONNECT TO REDSHIFT
# ============================================================

def get_connection():

    return redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        port=REDSHIFT_PORT
    )


# ============================================================
# 3. CREATE REPORTING TABLES
# ============================================================

CREATE_TABLES = [

    """
    CREATE TABLE IF NOT EXISTS gold_hcp_performance (
        hcpid VARCHAR(100),
        total_sales DOUBLE PRECISION,
        total_quantity DOUBLE PRECISION,
        sales_count BIGINT,
        visit_count BIGINT,
        last_visit_date DATE,
        hcp_name VARCHAR(255),
        specialization VARCHAR(255),
        city VARCHAR(255),
        region VARCHAR(20),
        territory_id VARCHAR(100),
        territory_name VARCHAR(255),
        avg_gap_days DOUBLE PRECISION,
        recommended_frequency VARCHAR(50),
        sales_performance VARCHAR(50),
        year INTEGER,
        month INTEGER
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS gold_territory_performance (
        territory_id VARCHAR(100),
        territory_name VARCHAR(255),
        total_doctors BIGINT,
        territory_sales DOUBLE PRECISION,
        total_visits BIGINT,
        total_quantity DOUBLE PRECISION,
        year INTEGER,
        month INTEGER
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS gold_product_rankings (
        product VARCHAR(255),
        product_sales DOUBLE PRECISION,
        product_quantity DOUBLE PRECISION,
        product_rank INTEGER,
        year INTEGER,
        month INTEGER
    )
    """
]


# ============================================================
# 4. CREATE BATCH AUDIT TABLE
# ============================================================

CREATE_AUDIT_TABLE = """
CREATE TABLE IF NOT EXISTS gold_load_audit (
    dataset VARCHAR(100),
    batch_id VARCHAR(14),
    loaded_rows BIGINT,
    loaded_at TIMESTAMP DEFAULT GETDATE()
)
"""


# ============================================================
# 5. LOAD PARQUET FILES
# ============================================================

def load_dataset(cursor, dataset, table):

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM gold_load_audit
        WHERE dataset = %s
          AND batch_id = %s
        """,
        (dataset, BATCH_ID)
    )

    if cursor.fetchone()[0] > 0:
        logger.info(
            "Skipping previously loaded batch: %s",
            dataset
        )
        return

    s3_path = (
        f"{GOLD_BASE}/{dataset}/"
        f"batch_id={BATCH_ID}/"
    )

    # Stage data to support repeatable batch loading.
    stage_table = f"stage_{table}"

    cursor.execute(
        f"CREATE TEMP TABLE {stage_table} "
        f"(LIKE {table})"
    )

    copy_sql = f"""
        COPY {stage_table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET
    """

    cursor.execute(copy_sql)

    cursor.execute(
        f"SELECT COUNT(*) FROM {stage_table}"
    )

    stage_count = cursor.fetchone()[0]

    if stage_count == 0:
        raise ValueError(
            f"No Gold records loaded for {dataset}"
        )

    cursor.execute(
        f"INSERT INTO {table} "
        f"SELECT * FROM {stage_table}"
    )

    cursor.execute(
        f"""
        INSERT INTO gold_load_audit
            (dataset, batch_id, loaded_rows)
        VALUES (%s, %s, %s)
        """,
        (dataset, BATCH_ID, stage_count)
    )

    logger.info(
        "Loaded %s rows into %s",
        stage_count,
        table
    )


# ============================================================
# 6. MAIN EXECUTION
# ============================================================

def main():

    connection = get_connection()
    connection.autocommit = False

    try:
        cursor = connection.cursor()

        for statement in CREATE_TABLES:
            cursor.execute(statement)

        cursor.execute(CREATE_AUDIT_TABLE)

        for dataset, table in DATASETS.items():
            load_dataset(cursor, dataset, table)

        connection.commit()

        logger.info(
            "Gold Layer Redshift loading completed"
        )

    except Exception:
        connection.rollback()
        logger.exception(
            "Redshift loading failed"
        )
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()
