resource "aws_s3_bucket" "this" {
  bucket        = var.bucket-name
  acl           = var.bucket-acl
  force_destroy = var.force-destroy

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.this.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }

  versioning {
    enabled = var.enable-versioning
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = var.private
  block_public_policy     = var.private
  ignore_public_acls      = var.private
  restrict_public_buckets = var.private
}

