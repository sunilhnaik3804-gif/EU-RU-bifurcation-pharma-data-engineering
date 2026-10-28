# ============================================================
# PROJECT: EU-RU PHARMA DATA ENGINEERING
# STEP 14: AWS S3 STORAGE CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# 1. CREATE FOUR S3 BUCKETS
# ------------------------------------------------------------

locals {
  pharma_buckets = {
    eu_raw    = var.eu_raw_bucket
    ru_raw    = var.ru_raw_bucket
    processed = var.processed_bucket
    error     = var.error_bucket
  }
}

resource "aws_s3_bucket" "pharma" {
  for_each = local.pharma_buckets

  bucket = each.value

  tags = {
    Name    = each.value
    Purpose = each.key
  }
}

# ------------------------------------------------------------
# 2. BLOCK PUBLIC ACCESS
# ------------------------------------------------------------

resource "aws_s3_bucket_public_access_block" "pharma" {
  for_each = aws_s3_bucket.pharma

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------
# 3. ENABLE BUCKET VERSIONING
# ------------------------------------------------------------

resource "aws_s3_bucket_versioning" "pharma" {
  for_each = aws_s3_bucket.pharma

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ------------------------------------------------------------
# 4. ENABLE SERVER-SIDE ENCRYPTION
# ------------------------------------------------------------

resource "aws_s3_bucket_server_side_encryption_configuration" "pharma" {
  for_each = aws_s3_bucket.pharma

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ------------------------------------------------------------
# 5. REQUIRE HTTPS FOR S3 ACCESS
# ------------------------------------------------------------

data "aws_iam_policy_document" "require_tls" {
  for_each = aws_s3_bucket.pharma

  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      each.value.arn,
      "${each.value.arn}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "require_tls" {
  for_each = aws_s3_bucket.pharma

  bucket = each.value.id
  policy = data.aws_iam_policy_document.require_tls[each.key].json

  depends_on = [
    aws_s3_bucket_public_access_block.pharma
  ]
}
