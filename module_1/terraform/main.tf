terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.1"
    }
  }
}

provider "docker" {}

# ---------------------------------------------------------
# 1. The Network
# ---------------------------------------------------------
resource "docker_network" "data_network" {
  name = "data_network"
}

# ---------------------------------------------------------
# 2. "GCP Storage" Replacement (MinIO)
# ---------------------------------------------------------
resource "docker_image" "minio_image" {
  name         = "minio/minio"
  keep_locally = true
}

resource "docker_container" "minio" {
  name  = "gcp_storage_clone"
  image = docker_image.minio_image.image_id
  
  networks_advanced {
    name = docker_network.data_network.name
  }

  ports {
    internal = 9000
    external = 9000
  }
  ports {
    internal = 9001
    external = 9001
  }

  env = [
    "MINIO_ROOT_USER=admin",
    "MINIO_ROOT_PASSWORD=${var.minio_password}" # <--- USING VARIABLE
  ]

  # --- PERSISTENCE ---
  volumes {
    # Uses the variable to name the folder on your laptop
    host_path      = "/workspaces/data-engineering-zoomcamp/terraform/${var.bucket_name}" 
    container_path = "/data"
  }

  command = ["server", "/data", "--console-address", ":9001"]
}

# ---------------------------------------------------------
# 3. "BigQuery" Replacement (Postgres)
# ---------------------------------------------------------
resource "docker_image" "postgres_image" {
  name         = "postgres:13"
  keep_locally = true
}

resource "docker_container" "data_warehouse" {
  name  = "bigquery_clone"
  image = docker_image.postgres_image.image_id

  networks_advanced {
    name = docker_network.data_network.name
  }

  ports {
    internal = 5432
    external = 5433 
  }

  env = [
    "POSTGRES_USER=root",
    "POSTGRES_PASSWORD=${var.db_password}", # <--- USING VARIABLE
    "POSTGRES_DB=ny_taxi"
  ]
}