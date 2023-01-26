locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "The project ID to deploy to"
  type        = string
}

variable "credentials_file" {
  description = "The path to the credentials file for the service account"
  type        = string
}

variable "region" {
  description = "The region this resource will be deployed to"
  default     = "asia-southeast1"
}

variable "zone" {
  description = "The zone this resource will be deployed to"
  default     = "asia-southeast1-a"
}

variable "storage_class" {
  description = "The storage class for the data lake bucket"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "ny_trips"
}

# transfer service
# variable "access_key_id" {
#   description = "AWS Access Key"
#   type = string
# }

# variable "secret_key" {
#   description = "AWS Secret Key"
#   type = string
# }

