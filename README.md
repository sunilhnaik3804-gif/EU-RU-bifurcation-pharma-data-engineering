
# EU–RU Bifurcation Pharma Data Engineering Pipeline
End-to-end AWS Data Engineering pipeline for EU_RU_bifurcation pharmaceutical data using Python, PySpark, AWS Glue, Lambda, S3 and step Functions.

An end-to-end data engineering portfolio project demonstrating pharmaceutical data ingestion, EU–RU data segregation, data validation, ETL processing, data quality checks, and analytics using Python, PySpark, and AWS services.

## Project Overview

Pharmaceutical companies process multiple datasets containing sales transactions, healthcare professional (HCP) profiles, territory alignments, field representative visits, and marketing information.

This project demonstrates how to build a cloud-based data pipeline that separates EU and Russia (RU) datasets and processes EU data through a structured Bronze, Silver, and Gold architecture.

The pipeline is designed around data segregation, validation, incremental processing, monitoring, and analytics.

**Project type:** Data Engineering Portfolio Project
**Domain:** Pharmaceutical Analytics
**Architecture:** AWS-based Lakehouse / ETL Pipeline
**Primary language:** Python
**Processing:** PySpark and AWS Glue

## Business Problem

Pharmaceutical datasets arrive from external systems and APIs in different formats. Before these datasets can be used for reporting and analytics, they require:

* Region-based data segregation.
* File format and schema validation.
* Handling of invalid or incomplete files.
* Data cleansing and deduplication.
* Standardization of dates and business fields.
* Data quality checks before downstream processing.
* Aggregation of pharmaceutical sales and HCP engagement metrics.
* Monitoring, error handling, and notifications.

The objective is to demonstrate a repeatable and maintainable pipeline that addresses these engineering challenges.

## Technology Stack

| Technology         | Purpose                                            |
| ------------------ | -------------------------------------------------- |
| Python             | API ingestion and pipeline utilities               |
| SQL                | Data querying and analytical transformations       |
| PySpark            | Distributed data processing                        |
| Amazon S3          | Data lake storage                                  |
| AWS Lambda         | Event-driven validation and orchestration triggers |
| Amazon EventBridge | Scheduled ingestion                                |
| AWS Glue           | ETL processing                                     |
| AWS Step Functions | Workflow orchestration                             |
| Amazon SNS         | Pipeline notifications                             |
| Amazon CloudWatch  | Logs and monitoring                                |
| Amazon Redshift    | Planned analytical data warehouse integration      |
| Terraform          | Infrastructure as Code                             |
| GitHub             | Version control and project documentation          |

## High-Level Architecture

```text
          External Pharmaceutical Data API
                        |
                        v
              API Authentication
                        |
                        v
              Python Data Ingestion
                        |
                        v
                 Region Bifurcation
                   /           \
                  /             \
                 v               v
          EU Raw S3 Bucket   RU Raw S3 Bucket
                 |
                 v
          S3 Object Event
                 |
                 v
           AWS Lambda
          File Validation
                 |
          +------+------+
          |             |
          v             v
      Valid File    Invalid File
          |             |
          v             v
      PASS Marker   Error S3 Bucket
          |             |
          |             v
          |         SNS Alert
          v
    Batch Readiness Check
          |
          v
     AWS Step Functions
          |
          v
       AWS Glue
     Bronze to Silver
          |
          v
    Silver Data Quality
          |
          v
       AWS Glue
     Silver to Gold
          |
          v
     Gold Analytics
          |
          v
  Planned Redshift Integration
          |
          v
  Planned BI / CRM Consumption
```

This diagram describes the intended workflow. The actual deployed architecture and service integrations must be verified separately.

## Data Sources

The pipeline is designed to process the following pharmaceutical datasets.

| Dataset     | Description                            | Example fields                              |
| ----------- | -------------------------------------- | ------------------------------------------- |
| Sales       | Pharmaceutical product transactions    | sales_id, hcp_id, product, quantity, amount |
| HCP Profile | Healthcare professional information    | hcp_id, hcp_name, specialization, city      |
| Territory   | Geographic and sales territory mapping | hcp_id, territory_id, region                |
| Visits      | Field representative interactions      | rep_id, hcp_id, visit_date, discussion      |
| Marketing   | Pharmaceutical marketing activities    | campaign_id, product, campaign_date         |

Supported input formats include CSV, JSON, and Parquet, subject to the implementation of the corresponding validation and ingestion logic.

All data shown in portfolio demonstrations should be synthetic or otherwise authorized for use.

## Data Lake Architecture

### Bronze Layer — Raw Data

The Bronze layer stores the original ingested datasets.

Responsibilities:

* Preserve source data for traceability.
* Organize data by region, dataset, and batch.
* Record batch identifiers.
* Support investigation of ingestion failures.

Example layout:

```text
bronze/
└── region=EU/
    ├── sales/
    │   └── batch_id=20261029/
    │       └── sales.json
    ├── hcp_profile/
    ├── territory/
    ├── visits/
    └── marketing/
```

The exact file paths depend on the ingestion implementation.

### Silver Layer — Cleansed Data

The Silver layer is intended to contain validated and standardized data.

Transformations include:

* Data type conversion.
* Missing-value handling.
* Duplicate removal.
* Date standardization.
* Schema validation.
* Business rule validation.
* Consistent identifiers for HCPs, products, and territories.

### Gold Layer — Analytics-Ready Data

The Gold layer is intended to provide aggregated datasets for reporting and analytical use cases.

Potential outputs include:

* Pharmaceutical sales by product.
* Sales by territory.
* HCP engagement metrics.
* Field representative visit summaries.
* Monthly sales trends.
* Product performance indicators.

These outputs depend on the business rules implemented in the Gold transformation.

## Pipeline Processing Steps

### Step 1 — API Authentication

Authenticate with the source API before requesting pharmaceutical datasets.

### Step 2 — Data Ingestion

Use Python to retrieve source data and associate records with the appropriate region and batch.

### Step 3 — EU–RU Bifurcation

Separate EU and RU data into their designated storage locations.

EU data continues through the EU processing workflow. RU data must remain outside the EU processing path unless an explicitly authorized design permits otherwise.

### Step 4 — S3 Event Trigger

An uploaded EU file can trigger a Lambda function through an S3 event notification.

### Step 5 — File Validation

Validate file metadata and content, including:

* Expected file format.
* File naming conventions.
* Required fields and schema.
* File readability.
* Required dataset and batch information.

Invalid files should be routed to an error location and reported through notifications.

### Step 6 — Batch Readiness

Verify that the required datasets for a batch have passed validation before starting downstream processing.

### Step 7 — Bronze-to-Silver Processing

Use AWS Glue and PySpark to read raw data and apply cleansing and standardization rules.

### Step 8 — Silver Data Quality

Check the transformed data against the required schema and business rules.

A failed quality check should stop the workflow before Gold processing.

### Step 9 — Silver-to-Gold Processing

Apply analytical transformations and generate reporting-oriented datasets.

### Step 10 — Orchestration and Monitoring

Use Step Functions to coordinate processing stages and SNS to send notifications. CloudWatch can be used for operational logs and monitoring.

### Step 11 — Analytical Consumption

Amazon Redshift integration and downstream BI or CRM delivery are planned extensions unless their end-to-end implementation has been verified.

## Repository Structure

The repository is organized around ingestion, serverless validation, Spark transformations, orchestration, testing, and infrastructure configuration.

```text
EU-RU-bifurcation-pharma-data-engineering/
│
├── README.md
├── .gitignore
│
├── config/
│   └── config.py
│
├── ingestion/
│   ├── api_auth.py
│   └── api_ingestion.py
│
├── sample_data/
│   ├── sales.json
│   └── hcp_profile.json
│
├── lambda_functions/
│   ├── s3_event_notification.py
│   ├── file_validator.py
│   └── batch_readiness.py
│
├── glue_jobs/
│   ├── bronze_to_silver.py
│   ├── data_quality.py
│   └── silver_to_gold.py
│
├── orchestration/
│   └── step_functions.json
│
├── redshift/
│   └── load_gold_to_redshift.py
│
├── tests/
│   └── test_gold_uat.py
│
└── terraform/
    ├── providers.tf
    ├── variables.tf
    ├── terraform.tfvars.example
    ├── s3.tf
    ├── iam.tf
    ├── sns.tf
    ├── lambda.tf
    ├── glue.tf
    ├── step_functions.tf
    └── outputs.tf
```

This is the intended project structure. Check the repository to confirm every listed file exists before treating the tree as a complete inventory.

## Security Considerations

This project demonstrates security considerations relevant to pharmaceutical data processing.

* Keep EU and RU datasets in appropriately separated storage locations.
* Apply least-privilege IAM permissions.
* Block public access to S3 buckets.
* Require encrypted transport.
* Enable S3 encryption and versioning.
* Never commit API credentials, access keys, or passwords.
* Use synthetic data for public demonstrations.
* Review data access and retention requirements before handling real pharmaceutical or personal data.

**Important:** Infrastructure configuration alone does not establish GDPR compliance. Compliance requires an appropriate legal basis, governance, access controls, retention policies, and organizational safeguards.

## Infrastructure as Code

Terraform configuration is provided for the planned AWS resources, including:

* S3 buckets.
* IAM roles and policies.
* Lambda functions.
* SNS notifications.
* AWS Glue jobs.
* Step Functions orchestration.
* Terraform outputs.

Before deployment, validate the configuration, check all resource references and IAM permissions, choose globally unique S3 bucket names, and review expected AWS costs.

Terraform files in a repository do not mean the infrastructure has already been deployed.

## Validation and Testing

The project includes a location for UAT tests and data quality checks.

Recommended test scenarios include:

1. Valid EU file passes validation.
2. Unsupported file format is rejected.
3. Missing required fields cause validation failure.
4. Duplicate records are handled correctly.
5. Invalid files trigger an alert.
6. Downstream processing waits until the required batch datasets pass validation.
7. Failed data quality checks stop Gold processing.
8. Reprocessing a batch does not unexpectedly duplicate output data.
9. RU data does not enter the EU processing workflow.
10. Aggregated Gold metrics match independently calculated expected results.

Record test results after actually executing the tests.

## Future Enhancements

Potential improvements include:

* Incremental processing using reliable batch identifiers and source watermarks.
* Idempotent processing and safe retry handling.
* Improved schema evolution support.
* Data quality metrics and audit tables.
* CloudWatch dashboards and operational alarms.
* CI/CD using GitHub Actions.
* Terraform validation and automated tests.
* Redshift loading and reconciliation.
* SCD Type 2 history tracking where business requirements justify it.
* Spark performance tuning for large datasets.
* Integration tests using representative synthetic datasets.

## Skills Demonstrated

This portfolio project is designed to demonstrate practical knowledge of:

* Python-based data ingestion.
* REST API integration.
* ETL and ELT concepts.
* AWS S3 data lake design.
* Event-driven processing with Lambda.
* Distributed processing with PySpark.
* AWS Glue job development.
* Workflow orchestration with Step Functions.
* Data validation and quality checks.
* SNS-based notifications.
* IAM and cloud security fundamentals.
* Infrastructure as Code with Terraform.
* GitHub version control and technical documentation.

## Project Status

**Portfolio implementation — work in progress until validated.**

The repository contains code and infrastructure configuration intended to demonstrate the architecture described above. Individual components, resource references, permissions, and integrations must be validated before claiming that the entire pipeline runs end to end.

## Disclaimer

This is an independent Data Engineering portfolio project intended for educational and demonstration purposes. It is not a representation of a deployed pharmaceutical production system or a certification of GDPR compliance.

---

**Author:** Sunil
**Project:** EU–RU Bifurcation Pharma Data Engineering
**Repository:** EU-RU-bifurcation-pharma-data-engineering

