#!/bin/bash
set -e

MARKER="/var/lib/postgresql/data/.restore_complete"

export PGPASSWORD=$POSTGRES_PASSWORD

echo "Checking if restore is needed..."

if [ -f "$MARKER" ]; then
  echo "Restore already completed ранее — пропускаем"
  exit 0
fi

echo "Проверяем данные..."

TAB_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
"SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='experiments';")

if [ "$TAB_EXISTS" = "1" ]; then
  COUNT=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
  "SELECT count(*) FROM experiments;")
else
  COUNT=0
fi

echo "Experiments count: $COUNT"

if [ "$COUNT" -le 1 ]; then
  echo "Restoring from S3..."

  if aws --endpoint-url=https://storage.yandexcloud.net \
    s3 cp s3://$S3_BUCKET/backups/mlflow_table.sql.gz /tmp/backup.gz; then

    echo "Reset schema..."
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
      "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

    echo "Applying dump..."
    gunzip -c /tmp/backup.gz | psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"

    rm /tmp/backup.gz

    echo "Restore SUCCESS"
    touch "$MARKER"

  else
    echo "S3 download FAILED — restore skipped"
  fi
else
  echo "Database already contains data — skip restore"
  touch "$MARKER"
fi