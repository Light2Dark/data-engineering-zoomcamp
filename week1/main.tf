terraform {
  required_version = ">= 1.0"
  backend "local" {} # where we will store state. local means on our local machine. "gcs" means google cloud storage while "aws" means aws s3
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)

  project = var.project
  region  = var.region
  zone    = var.zone
}


# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data_lake_bucket" {
  name = "${local.data_lake_bucket}_${var.project}" # concatenating DL bucket & project name
  location = var.region

  # optional but recommended settings
  storage_class = var.storage_class
  # uniform_bucket_level_access = true # got errors

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  force_destroy = true
}

# Data warehouse
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project = var.project
  location = var.region
}


# # actual infra
# # resource <resource_type> <resource_name>. ID for this infra resource is google_compute_network.vpc_network
# resource "google_compute_network" "vpc_network" {
#   name = "terraform-network"
# }

# resource "google_compute_instance" "vm_instance" {
#   name         = "terraform-instance"
#   machine_type = "f1-micro"

#   boot_disk {
#     initialize_params {
#       image = "debian-cloud/debian-11"
#     }
#   }

#   network_interface {
#     network = "google_compute_network.vpc_network"
#     # eventho empty, this access_config gives our vm an external ip to connect to internet
#     access_config {

#     }
#   }
# }