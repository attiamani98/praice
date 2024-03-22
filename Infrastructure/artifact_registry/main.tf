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

# Enable api for artifact registry
resource "google_project_service" "artifact_registry" {
  service                    = "artifactregistry.googleapis.com"
  disable_dependent_services = true
}

# Create artifact registry
resource "google_artifact_registry_repository" "main" {
  repository_id = "main-repo"
  format        = "DOCKER"
  depends_on    = [google_project_service.artifact_registry]
}