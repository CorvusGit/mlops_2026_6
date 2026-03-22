variable "provider_config" {
  description = "Yandex Cloud configuration"
  type = object({
    zone      = string
    folder_id = string
    token     = string
    cloud_id  = string
  })
}

variable "image_id" {
  description = "image"
  type        = string
}

variable "instance_name" {
  description = "Name of the compute instance"
  type        = string
}

variable "service_account_id" {
  description = "ID of the service account"
  type        = string
}

variable "subnet_id" {
  description = "ID of the subnet"
  type        = string
}

variable "public_key_path" {
  description = "for access to VM"
  type    = string
}

variable "ip_address" {
  description = "Local ip address for mlflow"
  type        = string
}

variable "access_key" {
  description = "access_key"
  type        = string
}

variable "secret_key" {
  description = "secret_key"
  type        = string
}

variable "bucket_name" {
  description = "bucket for mlflow artifacts and backups postgres mlflow db"
  type        = string
}

variable "pg_db_name" {
  type    = string
}

variable "pg_user" {
  type    = string
}

variable "pg_password" {
  type    = string
}

variable "backup_interval" {
  type    = string
}
