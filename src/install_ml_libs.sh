# Остановить выполнение при любой ошибке
set -e
# Используем pip3, так как в Dataproc по умолчанию Python 3
sudo /usr/bin/pip3 install matplotlib pandas mlflow seaborn
echo "Библиотеки успешно установлены."