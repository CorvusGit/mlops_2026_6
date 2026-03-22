# Выходные данные
output "mlflow_public_ip" {
  value = yandex_compute_instance.mlflow.network_interface[0].nat_ip_address
}