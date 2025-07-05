variable "region_id" {
  default = "us-east-1"
}

variable "prefix" {
  default = "case-ifood-de"
}

variable "account" {
  default = 593793061865
}

# Prefix configuration and project common tags
locals {
  prefix = "${var.prefix}-${terraform.workspace}"
  common_tags = {
    Project      = "case-ifood-de"
    ManagedBy    = "Terraform"
    Department   = "TI",
    Owner        = "Data Engineering"
    BusinessUnit = "Data"
    Billing      = "Infrastructure"
    Environment  = terraform.workspace
    UserEmail    = "sebastiao553@gmail.com"
  }
}

variable "bucket_names" {
  description = "Create S3 buckets with these names"
  type        = list(string)
  default = [
    "landing-zone",
    "bronze-zone",
    "silver-zone",
    "gold-zone"
  ]
}
