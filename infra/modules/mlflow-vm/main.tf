
# vm для развертывания docker, а в нём ml_flow, postgres и образа для бэкапов
resource "yandex_compute_disk" "boot_disk" {
  name     = "boot-disk"
  zone     = var.provider_config.zone
  image_id = var.image_id
  size     = 30
}

locals {
  docker_compose  = file("${path.module}/docker/docker-compose.yml")
  mlflow_docker  = file("${path.module}/docker/mlflow_docker/DockerFile")
  postgres_docker  = file("${path.module}/docker/postgres_docker/DockerFile")
}

resource "yandex_compute_instance" "mlflow" {
  name                      = var.instance_name
  allow_stopping_for_update = true
  platform_id               = "standard-v3"
  zone                      = var.provider.zone
  service_account_id        = var.service_account_id

  metadata = {
    //ssh-keys = "ubuntu:${file(var.ssh_public_key)}"
    user-data = templatefile("${path.module}/templates/cloud-init.yaml", {
      
      public_key = "${file(var.public_key_path)}"

      docker_compose = indent(6, local.docker_compose)
      postgres_docker = indent(6, local.postgres_docker)
      mlflow_docker = indent(6, local.mlflow_docker)
      
      access_key = var.access_key
      secret_key = var.secret_key
      s3_bucket = var.bucket_name

      pg_user = var.pg_user
      pg_password = var.pg_password
      pg_db_name = var.pg_db_name
      backup_interval = var.backup_interval
    })
  }
  
  scheduling_policy {
    preemptible = true
  }

  resources {
    cores  = 2
    memory = 16
  }

  boot_disk {
    disk_id = yandex_compute_disk.boot_disk.id
  }

  network_interface {
    subnet_id = var.subnet_id
    nat       = true
    ip_address = var.ip_address
  }

  metadata_options {
    gce_http_endpoint = 1
    gce_http_token    = 1
  }
}


#preserve_hostname: false