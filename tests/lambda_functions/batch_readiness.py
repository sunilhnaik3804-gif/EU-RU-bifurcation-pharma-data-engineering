# ============================================================
# PROJECT: EU-RU PHARMA DATA ENGINEERING
# STEP 9: AWS LAMBDA BATCH READINESS VALIDATION
# ============================================================

import os
import json
import re
import logging
import hashlib

import boto3
from botocore.exceptions import ClientError


# ============================================================
# 1. CONFIGURATION
# ============================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sfn = boto3.client("stepfunctions")

EU_RAW_BUCKET = os.environ["EU_RAW_BUCKET"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

REQUIRED_DATASETS = [
    "sales",
    "hcp_profile",
    "visits",
    "territory"
]


# ============================================================
# 2. CHECK IF DATASET EXISTS
# ============================================================

def check_dataset(batch_id, dataset):

    key = (
        f"bronze/region=EU/{dataset}/"
        f"batch_id={batch_id}/{dataset}.json"
    )

    try:
        response = s3.head_object(
            Bucket=EU_RAW_BUCKET,
            Key=key
        )

        file_size = response["ContentLength"]

        if file_size <= 0:
            logger.warning(
                "Empty file: %s",
                key
            )
            return False

        logger.info(
            "Found dataset: %s",
            dataset
        )

        return True

    except ClientError as error:

        error_code = error.response[
            "Error"
        ]["Code"]

        if error_code in [
            "404",
            "NoSuchKey",
            "NotFound"
        ]:
            logger.info(
                "Dataset not available: %s",
                dataset
            )
            return False

        raise


# ============================================================
# 3. VERIFY ALL REQUIRED DATASETS
# ============================================================

def verify_batch(batch_id):

    available = []
    missing = []

    for dataset in REQUIRED_DATASETS:

        if check_dataset(batch_id, dataset):
            available.append(dataset)
        else:
            missing.append(dataset)

    return {
        "batch_id": batch_id,
        "available_datasets": available,
        "missing_datasets": missing,
        "ready": len(missing) == 0
    }


# ============================================================
# 4. START STEP FUNCTIONS EXECUTION
# ============================================================

def start_pipeline(batch_id):

    # Deterministic execution name prevents duplicate
    # Standard Workflow executions for the same batch.
    execution_name = f"eu-pharma-{batch_id}"

    execution_input = {
        "bucket": EU_RAW_BUCKET,
        "batch_id": batch_id,
        "region": "EU",
        "datasets": REQUIRED_DATASETS
    }

    try:

        response = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,
            input=json.dumps(execution_input)
        )

        logger.info(
            "Pipeline started: %s",
            response["executionArn"]
        )

        return {
            "status": "STARTED",
            "execution_arn": response["executionArn"]
        }

    except ClientError as error:

        if error.response["Error"]["Code"] == (
            "ExecutionAlreadyExists"
        ):

            logger.info(
                "Pipeline already started for batch %s",
                batch_id
            )

            return {
                "status": "ALREADY_STARTED",
                "batch_id": batch_id
            }

        raise


# ============================================================
# 5. LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):

    logger.info(
        "Batch readiness event: %s",
        json.dumps(event)
    )

    batch_id = event.get("batch_id")

    if not batch_id:
        raise ValueError(
            "batch_id is required"
        )

    if not re.fullmatch(r"\d{14}", batch_id):
        raise ValueError(
            "Invalid batch_id format"
        )

    result = verify_batch(batch_id)

    if not result["ready"]:

        logger.info(
            "Batch %s is not ready. Missing: %s",
            batch_id,
            result["missing_datasets"]
        )

        return {
            "status": "WAITING",
            **result
        }

    pipeline_result = start_pipeline(batch_id)

    return {
        **result,
        **pipeline_result
    }
