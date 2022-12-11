locals {
  data_lake_bucket = "openaq_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-southeast1" # GCP bucket is located at asia-southeast1(Singapore)
  type = string
}

# Ref: https://cloud.google.com/storage/docs/storage-classes
variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "openaq"
}

# variable "TABLE_NAME" {
#   description = "BigQuery Table"
#   type = string
#   default = "air_quality"
  
# }
