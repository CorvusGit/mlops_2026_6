#!/bin/bash
# Обновляем pip и устанавливаем нужные библиотеки в среду Spark
sudo /opt/conda/bin/pip install --upgrade pip
sudo /opt/conda/bin/pip install mlflow scikit-learn pandas matplotlib seaborn