FROM postgres:14

# Устанавливаем AWS CLI
RUN apt-get update && apt-get install -y awscli

# Копируем restore скрипт
#COPY restore_mlflow.sh /restore_mlflow.sh
#RUN chmod +x /restore_mlflow.sh

#ENTRYPOINT ["/restore_mlflow.sh"]