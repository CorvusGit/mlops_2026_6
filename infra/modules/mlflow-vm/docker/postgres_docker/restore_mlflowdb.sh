#!/bin/bash
set -e


# 1. Если файл-маркер есть — просто запускаем базу и выходим из скрипта

MARKER=/var/lib/postgresql/data/.restore_complete

if [ -f \"$$MARKER\" ]; then
    echo 'База уже была проверена ранее. Запуск...';
    exec docker-entrypoint.sh postgres;
fi;

# 2. Запуск в фоне
docker-entrypoint.sh postgres & until pg_isready -h localhost -U $POSTGRES_USER; do sleep 2; done;

echo 'Анализ данных в базе...';
TAB_EXISTS=$$(psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB -tAc \"SELECT 1 FROM pg_tables WHERE tablename='experiments'\");
if [ \"$$TAB_EXISTS\" = \"1\" ]; then
    COUNT=$$(psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB -tAc \"SELECT count(*) FROM experiments\");
else
    COUNT=0;
fi;

echo \"Найдено экспериментов: $$COUNT\";

# 3. Восстановление
if [ \"$$COUNT\" -le 1 ]; then
    echo 'База пуста или только Default. Начинаем восстановление...';
    if aws --endpoint-url=https://storage.yandexcloud.net s3 cp s3://$S3_BUCKET/backups/mlflow_table.sql.gz /tmp/backup.gz; then
    echo 'Очистка и заливка данных...';
    # Важно: выполняем всё одной цепочкой через &&
    psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB -c \"DROP SCHEMA public CASCADE; CREATE SCHEMA public; && \
    gunzip -c /tmp/backup.gz | psql -h localhost -U $$POSTGRES_USER -d $$POSTGRES_DB && \
    echo 'Восстановление успешно завершено.' && \
    rm /tmp/backup.gz;
    fi;
fi;

# Создаем маркер, чтобы при следующем рестарте скрипт не выполнялся
touch $$MARKER;

# 4. КОРРЕКТНОЕ ЗАВЕРШЕНИЕ
echo "Перезапуск сервера...";
su postgres -c "pg_ctl stop -m fast -w" || pkill -9 postgres;
rm -f /var/lib/postgresql/data/postmaster.pid;
sleep 1;
echo "Запуск контейнера";
exec docker-entrypoint.sh postgres