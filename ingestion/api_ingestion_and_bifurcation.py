import json
import os
import boto3

from datetime import datetime, timezone
from api_auth import get_api_data


# ==========================================
# AWS S3 CONFIGURATION
# ==========================================

s3 = boto3.client("s3")

EU_BUCKET = os.getenv("EU_RAW_BUCKET", "eu-pharma-raw")
RU_BUCKET = os.getenv("RU_RAW_BUCKET", "ru-pharma-raw")


# ==========================================
# THIRD-PARTY REST API ENDPOINTS
# ==========================================

API_ENDPOINTS = {
    "sales": "https://example.invalid/api/sales",
    "hcp_profile": "https://example.invalid/api/hcp",
    "territory": "https://example.invalid/api/territory",
    "visits": "https://example.invalid/api/visits",
    "marketing": "https://example.invalid/api/marketing"
}


# ==========================================
# EU-RU DATA BIFURCATION
# ==========================================

def bifurcate_data(records):

    eu_data = []
    ru_data = []

    for record in records:

        region = str(
            record.get("region", "")
        ).strip().upper()

        if region == "EU":
            eu_data.append(record)

        elif region == "RU":
            ru_data.append(record)

        else:
            raise ValueError(
                f"Invalid or missing region: {region}"
            )

    return eu_data, ru_data


# ==========================================
# UPLOAD DATA TO AMAZON S3
# ==========================================

def upload_to_s3(bucket, region, dataset, data, batch_id):

    if not data:
        print(f"No {region} records for {dataset}")
        return

    s3_key = (
        f"bronze/region={region}/"
        f"{dataset}/"
        f"batch_id={batch_id}/"
        f"{dataset}.json"
    )

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

    print(f"{region} data uploaded: s3://{bucket}/{s3_key}")


# ==========================================
# MAIN INGESTION PIPELINE
# ==========================================

def ingest_data():

    batch_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d%H%M%S")

    for dataset, api_url in API_ENDPOINTS.items():

        print(f"Extracting {dataset} data...")

        # Step 1: Extract combined EU + RU data
        records = get_api_data(api_url)

        if not isinstance(records, list):
            raise ValueError(
                f"{dataset} API must return a list of records"
            )

        # Step 2: Separate EU and RU
        eu_data, ru_data = bifurcate_data(records)

        # Step 3: Upload EU data to Bronze Layer
        upload_to_s3(
            EU_BUCKET,
            "EU",
            dataset,
            eu_data,
            batch_id
        )

        # Step 4: Store RU data separately
        upload_to_s3(
            RU_BUCKET,
            "RU",
            dataset,
            ru_data,
            batch_id
        )

        print(
            f"{dataset}: EU={len(eu_data)}, "
            f"RU={len(ru_data)}"
        )


if __name__ == "__main__":
    ingest_data()
