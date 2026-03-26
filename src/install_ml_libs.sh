#!/bin/bash
set -e
# # Остановить выполнение при любой ошибке
# /usr/bin/python3 -m pip install --upgrade pip
# /usr/bin/python3 -m pip install mlflow seaborn

# Находим путь к conda, если /opt/conda вдруг не там
CONDA_BIN="/opt/conda/bin/conda"
PIP_BIN="/opt/conda/bin/pip"

# Устанавливаем библиотеки через pip, встроенный в conda
# Это гарантирует, что они попадут в базовое окружение (base)
$PIP_BIN install --upgrade pip
$PIP_BIN install mlflow seaborn