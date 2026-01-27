variable "db_password" {
  description = "The password for Postgres"
  default     = "root"
}

variable "bucket_name" {
  description = "The name of your local bucket folder (Volume)"
  default     = "minio_data" 
}

variable "minio_password" {
  description = "The password for MinIO"
  default     = "password"
}