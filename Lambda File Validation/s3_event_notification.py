import json
import os
import boto3

from urllib.parse import unquote_plus

sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):

    for record in event.get("Records", []):

        bucket = record["s3"]["bucket"]["name"]

        key = unquote_plus(
            record["s3"]["object"]["key"]
        )

        if not key.startswith("bronze/region=EU/"):
            continue

        message = {
            "project": "EU-RU Pharma Bifurcation",
            "status": "FILE_RECEIVED",
            "region": "EU",
            "bucket": bucket,
            "file_path": key
        }

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="EU Pharma Raw File Received",
            Message=json.dumps(message)
        )

        print(f"SNS notification sent: {key}")

    return {
        "statusCode": 200,
        "body": "EU file arrival notification completed"
    }
