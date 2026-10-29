# ============================================================
# PROJECT: EU-RU PHARMA DATA ENGINEERING
# STEP 20: TERRAFORM OUTPUTS
# ============================================================

# ------------------------------------------------------------
# S3 BUCKETS
# ------------------------------------------------------------

output "eu_raw_bucket_name" {
  description = "EU raw data S3 bucket"
  value       = aws_s3_bucket.pharma["eu_raw"].bucket
}

output "ru_raw_bucket_name" {
  description = "RU raw data S3 bucket"
  value       = aws_s3_bucket.pharma["ru_raw"].bucket
}

output "processed_bucket_name" {
  description = "Processed S3 bucket"
  value       = aws_s3_bucket.pharma["processed"].bucket
}

output "error_bucket_name" {
  description = "Error S3 bucket"
  value       = aws_s3_bucket.pharma["error"].bucket
}

# ------------------------------------------------------------
# SNS
# ------------------------------------------------------------

output "pipeline_alerts_topic_arn" {
  description = "SNS topic ARN for pipeline alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}

# ------------------------------------------------------------
# LAMBDA FUNCTIONS
# ------------------------------------------------------------

output "s3_notification_lambda_arn" {
  description = "S3 notification Lambda ARN"
  value       = aws_lambda_function.s3_notification.arn
}

output "file_validator_lambda_arn" {
  description = "File validator Lambda ARN"
  value       = aws_lambda_function.file_validator.arn
}

output "batch_readiness_lambda_arn" {
  description = "Batch readiness Lambda ARN"
  value       = aws_lambda_function.batch_readiness.arn
}

# ------------------------------------------------------------
# GLUE JOBS
# ------------------------------------------------------------

output "bronze_to_silver_glue_job" {
  description = "Bronze to Silver Glue job"
  value       = aws_glue_job.bronze_to_silver.name
}

output "data_quality_glue_job" {
  description = "Silver data quality Glue job"
  value       = aws_glue_job.data_quality.name
}

output "silver_to_gold_glue_job" {
  description = "Silver to Gold Glue job"
  value       = aws_glue_job.silver_to_gold.name
}

# ------------------------------------------------------------
# STEP FUNCTIONS
# ------------------------------------------------------------

output "step_functions_state_machine_name" {
  description = "Step Functions state machine name"
  value       = aws_sfn_state_machine.pharma_pipeline.name
}

output "step_functions_state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.pharma_pipeline.arn
}
