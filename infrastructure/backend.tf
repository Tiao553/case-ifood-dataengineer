# Backend configuration require a AWS storage bucket.
terraform {
  backend "s3" {
    bucket = "case-ifood-backup-tf"
    key    = "state/terraform.tfstate"
    region = "us-east-1"
  }
}
