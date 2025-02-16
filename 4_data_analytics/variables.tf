variable "credentials" {
  description = "My Credentials"
  default     = "./secrets/zoomcamp-week-4-key.json"
}

variable "project" {
  description = "Project"
  default     = "zoomcamp-week-4"

}

variable "region" {
  description = "Region"
  default     = "us-central1"

}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "zoomcamp_week_4_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "zoomcamp-week-4-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
