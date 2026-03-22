resource "random_id" "bucket_id" {
  byte_length = 8
}

resource "yandex_storage_bucket" "bucket" {
  bucket        = "${var.name}-${random_id.bucket_id.hex}"
  access_key    = var.access_key
  secret_key    = var.secret_key
  force_destroy = true
}

# resource "yandex_storage_object" "input_folder" {
#   bucket = yandex_storage_bucket.bucket.bucket
#   key    = "input_folder/"   # обратный слэш в конце делает это префиксом
#   content = ""            # пустой объект
# }

# resource "yandex_storage_object" "output_folder" {
#   bucket = yandex_storage_bucket.bucket.bucket
#   key    = "output_folder/"   # обратный слэш в конце делает это префиксом
#   content = ""            # пустой объект
# }

# resource "yandex_storage_object" "output_folder_for_ml" {
#   bucket = yandex_storage_bucket.bucket.bucket
#   key    = "output_folder_for_ml/"   # обратный слэш в конце делает это префиксом
#   content = ""            # пустой объект
# }