variable "yc_instance_user" {
  type = string  
}

variable "yc_instance_name" {
  type = string  
}

variable "yc_network_name" {
  type = string
}

variable "yc_subnet_name" {
  type = string
}

variable "yc_service_account_name" {
  type    = string
}

variable "yc_bucket_name" {
  type = string
}

variable "yc_storage_endpoint_url" {
  type = string
  default = "https://storage.yandexcloud.net"
}

#variable "ubuntu_image_id" {
#  type    = string
#}

variable "public_key_path" {
  type = string
}

# variable "private_key_path" {
#   type = string
# }

variable "admin_password" {
  type = string
  description = "Admin password for the Airflow web interface"
}

variable "yc_config" {
  type = object({
    zone      = string
    folder_id = string
    token     = string
    cloud_id  = string
  })
  description = "Yandex Cloud configuration"
}

variable "airflow_db_conn_default" {
  type = string
}
variable "admin_login" {  
  type = string
}


variable "mlflow_instance_name" {
  type    = string
}

variable "mlfloe_image_id" {
  type    = string
}

variable "mlflow_public_key_path" {
  type    = string
}

variable "mlflow_user_name" {
  type    = string
}

variable "mlflow_ip_address" {
  type    = string
}

variable "for_mlflow_service_account_id" {
  type    = string
}

variable "mlflow_buket_name" {
  type    = string
}

variable "mlflow_access_key" {
  type    = string
}

variable "mlflow_secret_key" {
  type    = string
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

variable "mlflow_backup_interval" {
  type    = string
}

variable "mlflow_port" {
  type = string
  default = "5005"
}

variable "learning_sample_fraq" {
  type = number
  default = 0.1
}

