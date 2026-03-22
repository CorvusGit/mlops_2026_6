output "mlflow_vm_public_ip" {
  description = "Публичный IP VM из модуля mlflow_vm"
  value       = module.mlflow-vm.mlflow_public_ip
}