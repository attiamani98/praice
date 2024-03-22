terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.17.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Enable api's

resource "google_project_service" "cloud_run" {
  service                    = "run.googleapis.com"
  disable_dependent_services = true
}



# Create cloud run service
resource "google_cloud_run_service" "default" {
  name       = "my-service"
  location   = var.region

  template {
    spec {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/main-repo/praice_model:latest"
        ports {
         container_port = 8000
        }
      }
    }
  }
}

output "service_url" {
  description = "URL of the Cloud Run service"
  value       = google_cloud_run_service.default.status[0].url
}
