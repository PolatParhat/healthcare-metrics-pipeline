terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Fill these in from bootstrap's outputs after running `terraform apply`
  # in ../../bootstrap, then run `terraform init` here. Terraform won't let
  # you interpolate variables into a backend block, which is why this can't
  # just read bootstrap's outputs automatically.
  backend "s3" {
    bucket         = "healthcare-metrics-tfstate-<your-account-id>"
    key            = "healthcare-metrics/dev/terraform.tfstate"
    region         = "us-west-1"
    dynamodb_table = "healthcare-metrics-tfstate-lock"
    encrypt        = true
  }
}
