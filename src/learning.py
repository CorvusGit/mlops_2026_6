import sys
import argparse
import math
import logging
import re
from datetime import datetime

#import findspark
#findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
#from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
#from pyspark.ml.feature import StandardScaler, PCA

import mlflow
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
import json

import matplotlib
matplotlib.use('Agg') # Переключает matplotlib в режим записи в файл, без GUI
import matplotlib.pyplot as plt
import seaborn as sns

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")

from synapse.ml.lightgbm import LightGBMClassifier
#from yv add import XGBoostClassifier

#import os
#os.environ["OMP_NUM_THREADS"] = "1"

def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description='Скрипт для очистки данных из HDFS и сохранения в S3'
    )

    parser.add_argument(
        '--in-path',
        type=str,
        required=True,
        help='Путь к папке в HDFS с исходными данными'
    )

    parser.add_argument(
        '--out-path',
        type=str,
        required=False,
        help='Имя S3 бакета для сохранения результатов'
    )

    parser.add_argument(
        '--master-conn',
        type=str,
        required=False,
        default="",
        help='подключение к мастер ноде'
    )

    parser.add_argument(
        '--mlflow-conn',
        type=str,
        required=False,
        default="http://127.0.0.1:5005",
        help='подключение к mlflow'
    )

    parser.add_argument(
        '--log-stats',
        action='store_true',
        default=False,
        help='Включить логирование статистики по отфильтрованным данным'
    )

    parser.add_argument(
        '--local',
        action='store_true',
        default=False,
        help='признак локального запуска'
    )

    return parser.parse_args()

#функция подсчёта количества выходных файлов
def get_coalesce_number(
    spark,
    logger,
    in_path: str,
    target_size_mb: int = 256,
    zip_coeff: int = 3
) -> int:
    """
    Вычисляет N для coalesce на основе суммарного размера данных
    Работает с HDFS и s3a://
    """

    sc = spark.sparkContext
    jvm = sc._gateway.jvm

    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem
    URI = jvm.java.net.URI

    hadoop_conf = sc._jsc.hadoopConfiguration()

    uri = URI.create(in_path)
    fs = FileSystem.get(uri, hadoop_conf)

    path = Path(in_path)

    if not fs.exists(path):
        raise FileNotFoundError(f"Путь не найден: {in_path}")

    # ContentSummary работает и для HDFS, и для s3a
    content_summary = fs.getContentSummary(path)

    total_bytes = content_summary.getLength()
    file_count = content_summary.getFileCount()

    if total_bytes == 0:
        logger.warning("Источник пустой, будет создан 1 файл")
        return 1

    # учитываем сжатие (parquet + snappy и т.п.)
    total_mb = (total_bytes / zip_coeff) / (1024 * 1024)

    n_coalesce = int(math.ceil(total_mb / target_size_mb))
    n_coalesce = max(1, n_coalesce)

    logger.warning(f"Источник: {in_path}")
    logger.warning(f"Файлов во входных данных: {file_count}")
    logger.warning(f"Суммарный размер (оценка): {total_mb:.2f} MB")
    logger.warning(f"Рекомендованный coalesce({n_coalesce}) для target_size_mb={target_size_mb}")

    return n_coalesce

# ФУНКЦИИ ПОДГОТОВКИ ПРИЗНАКОВ

def add_ratio_features_simple(df, target_col, hist_agg_cols,drop_hist=False):
    """
    df: DataFrame
    target_col: колонка для сравнения напр. 'tx_amount')
    hist_agg_cols: колонки, с которыми будет сравниваться текущее значение
    drop_hist: удалять ли hist_agg_cols по заверщении
    """
    
    for col_name in hist_agg_cols:
        ratio_col_name = f"ratio_{col_name}"
    
        # Используем coalesce, чтобы избежать деления на NULL или 0
        df = df.withColumn(
            ratio_col_name, 
            F.col(target_col) / F.when(F.col(col_name) != 0, F.col(col_name)).otherwise(F.col(target_col))
        )
    
    # Удаляем промежуточнык колонки с агрегатом
    df = df.drop(*hist_agg_cols)
        
    return df
    
def split_train_test_by_time(df, train_ratio=0.8, time_col='tx_datetime'):
    """
    Разделяет датасет на train/test по времени без перемешивания.
    """
    # Находим минимальное и максимальное время (в секундах для точности)
    stats = df.select(
        F.min(time_col).alias('min_t'),
        F.max(time_col).alias('max_t')
    ).collect()[0]
    
    start_t = stats['min_t']
    end_t = stats['max_t']
    
    # Вычисляем точку отсечения (threshold) через перцентиль
    threshold = df.stat.approxQuantile(time_col, [train_ratio], 0.01)[0]
    
    # Разделяем данные
    train_df = df.filter(F.col(time_col) <= threshold)
    test_df = df.filter(F.col(time_col) > threshold)
    
    return train_df, test_df

def get_waighted_data(df,eval_col="tx_fraud"):
    # Считаем количество строк каждого класса
    counts = df.groupBy(eval_col).count().collect()
    dict_counts = {row[eval_col]: row["count"] for row in counts}

    # Рассчитываем коэффициент (Negative / Positive)
    ratio = round(dict_counts[False] / dict_counts[True])
    #print(ratio)
    # Создаем колонку с весами: 
    # Если фрод — вес равен ratio, если нет — вес равен 1
    train_weighted = df.withColumn("weight", F.when(F.col(eval_col) == True, ratio).otherwise(1.0))
    return train_weighted

def get_metrics_and_log(predictions,prefix=''):
    metrics_raw = predictions.select(
        F.sum(F.when((F.col("prediction") == 1) & (F.col("tx_fraud") == 1), 1).otherwise(0)).alias("TP"),
        F.sum(F.when((F.col("prediction") == 1) & (F.col("tx_fraud") == 0), 1).otherwise(0)).alias("FP"),
        F.sum(F.when((F.col("prediction") == 0) & (F.col("tx_fraud") == 1), 1).otherwise(0)).alias("FN")
    ).collect()[0]
    
    tp, fp, fn = metrics_raw["TP"], metrics_raw["FP"], metrics_raw["FN"]
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    result_fp = predictions.select(
        F.count(F.when((F.col("prediction") == 1) & (F.col("tx_fraud") == 0), 1)).alias("total_fp"),
        F.min("TX_DATETIME").alias("first_tx"),
        F.max("TX_DATETIME").alias("last_tx") 
    ).collect()[0]
    
    total_weeks = (result_fp["last_tx"].timestamp() - result_fp["first_tx"].timestamp()) / 604800
    avg_fp_per_week = result_fp["total_fp"] / total_weeks if total_weeks > 0 else 0

    # Логируем метрики в MLflow
    mlflow.log_metrics({
        f"{prefix}precision": precision,
        f"{prefix}recall": recall,
        f"{prefix}f1_score": f1,
        f"{prefix}avg_fp_per_week": avg_fp_per_week
    })
    
    #print(f"Metrics logged: F1={f1:.4f}, FP/week={avg_fp_per_week:.2f}")
    return precision, recall, f1, avg_fp_per_week

def get_confusion_matrix(predictions):
    
    #Логируем Confusion Matrix как рисунок
    conf_matrix = predictions.groupBy("tx_fraud", "prediction").count().toPandas()
    matrix_df = conf_matrix.pivot(index='tx_fraud', columns='prediction', values='count').fillna(0)

    plt.figure(figsize=(8, 6))
    sns.heatmap(matrix_df, annot=True, fmt='g', cmap='Blues')
    plt.xlabel('Предсказание')
    plt.ylabel('Реальность (tx_fraud)')
    plt.title('Confusion Matrix')
    
    # Сохраняем во временный файл и отправляем в MLflow
    temp_plot_path = "confusion_matrix.png"
    plt.savefig(temp_plot_path)
    mlflow.log_artifact(temp_plot_path)
    plt.close() # закрываем фигуру, чтобы не дублировать в notebook

def learning(
        in_path: str,
        out_path: str,
        master_conn: str = "", #IP:PORT
        mlflow_conn: str = "", #IP:PORT
        log_stats: bool = False,
        local: bool = False):

    # =====================================================
    # Конфигурация
    # =====================================================
    INPUT_PATH = in_path
    OUTPUT_PATH = out_path
    MASTER_CONN = master_conn
    MLFLOW_CONN = mlflow_conn
    LOG = log_stats
    LOCAL_RUN = local

    SPECIAL_TERMINAL_ID = -1
    SPECIAL_CUSTOMER_ID = -1
    SPECIAL_SCENARIO_ID = -1
    SPECIAL_TIME_SECONDS = -1
    SPECIAL_TIME_DAYS = -1

    # =====================================================
    # Создание сессии
    # =====================================================
    if 'spark' in locals() or 'spark' in globals():
        spark.stop()

    spark  = ""
    logger = ""

    if LOCAL_RUN:
        print("LOCAL RUN !!!")
        spark = (
                       SparkSession.builder
                .appName("Spark ML Learning")
                .master("local[14]")
                .config("spark.driver.memory", "24g")
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.memory.fraction", "0.6") # 60% памяти под вычисления
                .config("spark.memory.storageFraction", "0.5")
                #.config("spark.memory.offHeap.enabled", "true")
                #.config("spark.memory.offHeap.size", "10g") # Явно выделяем место под нативную память
                #.config("spark.executor.memory", "16g")
                #.config("spark.default.parallelism", "6")
                #.config("spark.sql.shuffle.partitions", "16")
                #.config("spark.driver.maxResultSize", "4g")
                .config("spark.local.dir", "/media/rk/2TB/spark_tmp")
                # Адаптивность
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Склеивать мелкие части
                .config("spark.sql.adaptive.skewJoin.enabled", "true")           # Обработка перекосов данных
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb") # целевой размер раздела при склейке
                #.config("spark.jars.packages", "ml.dmlc:xgboost4j-spark_2.12:1.4.1")
                #.config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
                #.config("spark.jars.packages","com.microsoft.azure:synapseml_2.12:0.11.1")
                #.config("spark.sql.execution.arrow.pyspark.enabled", "true")
                #.config("spark.jars.excludes", "io.netty:netty-transport-native-kqueue,io.netty:netty-resolver-dns-native-macos")
                #.config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") 
                .getOrCreate()
        )

        #spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

    else:
        spark = (
            SparkSession.builder
                .appName("Spark ML Learning")
                # Пути к Python (чтобы не было конфликтов версий)
                #.config("spark.pyspark.python", "/usr/bin/python3")
                #.config("spark.pyspark.driver.python", "/usr/bin/python3")
                
                # Драйвер
                .config("spark.driver.memory", "10g")
                .config("spark.driver.maxResultSize", "2g") 

                # Исполнители
                .config("spark.executor.cores", "4")
                .config("spark.executor.memory", "10g")
                .config("spark.executor.memoryOverhead", "2g")

                # параллелизм
                .config("spark.sql.shuffle.partitions", "96")
                .config("spark.default.parallelism", "96")

                # Адаптивность
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Склеивать мелкие части
                .config("spark.sql.adaptive.skewJoin.enabled", "true")           # Обработка перекосов данных
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb") # целевой размер раздела при склейке
                # сеть
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.network.timeout", "600s")
                .getOrCreate()
        )
        
    spark.conf.set("spark.sql.ansi.enabled", "false")

    logger = logging.getLogger("ModelLearning")
    logger.setLevel(logging.WARN)

    #sc = spark.sparkContext
    #sc.setLogLevel("WARN")

    #вычисляем максимальное количество выходных файлов
    n_out = get_coalesce_number(spark,logger,INPUT_PATH, target_size_mb=128,zip_coeff=1)

    # =====================================================
    # Чтение данных
    # =====================================================

    df = (
        spark.read
        .parquet(INPUT_PATH)
        #.persist(StorageLevel.MEMORY_AND_DISK)
    )#.sample(fraction=0.2, seed=42)

    #инициализируем mlflow
    #os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    mlflow.set_tracking_uri(MLFLOW_CONN)

    # =====================================================
    # Генерация дополнительных простых признаков, 
    # чтобы не занимать лишнее место + fillna
    # =====================================================
    PI = math.pi
    #df = df.limit(100000)
    df = df \
        .withColumn("hour", F.hour("tx_datetime")) \
        .withColumn("is_night", F.when((F.col("hour") >= 0) & (F.col("hour") < 6), 1).otherwise(0)) \
        .withColumn("day_of_week", F.dayofweek("tx_datetime")) \
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0)) \
        .withColumn("is_rovn_sum", ((F.col("tx_amount") % 100 == 0) | (F.col("tx_amount") % 1000 == 0)).cast("int")) \
        .withColumn("is_unknonw_terminal", (F.col("terminal_id") == -1).cast("int")) \
        .withColumn("is_unknonw_customer", (F.col("customer_id") == -1).cast("int")) \
        .withColumn("hour_sin", F.sin(2 * PI * F.col("hour") / 24)) \
        .withColumn("hour_cos", F.cos(2 * PI * F.col("hour") / 24)) \
        .withColumn("day_sin", F.sin(2 * PI * F.col("day_of_week") / 7)) \
        .withColumn("day_cos", F.cos(2 * PI * F.col("day_of_week") / 7))  


    #print(df.columns)
    # считаем поля -отношения
    rate_cols = ["term_tx_amount_avg_30d_hist","term_tx_amount_avg_7d_hist",
             "term_tx_amount_std_30d_hist","term_tx_amount_std_7d_hist",
             'tx_amount_avg_cust_30d_hist','tx_amount_std_cust_30d_hist',
             'tx_amount_avg_cust_7d_hist','tx_amount_std_cust_7d_hist']

    df = add_ratio_features_simple(df, "tx_amount", rate_cols,drop_hist=False)

    #заполняем null значения
    df = df.fillna(0)
    
    # =====================================================
    # Разделение даных  train/test, добавление весов
    # =====================================================

    # разделяем на тренировочный и тестовый датасеты
    train, test = split_train_test_by_time(df, train_ratio=0.8, time_col="unix_time")
    if LOG:
        logger.warning(f"Train count: {train.count()}")
        logger.warning(f"Test count: {test.count()}")

    #считаем и добавляем в датасет веса
    train_weighted = get_waighted_data(train,eval_col="tx_fraud")
    #train_weighted = train
    #train_weighted = train_weighted.repartition(140)

    #a = train_weighted.limit(1)
    #logger.warning(str(a))

    # =====================================================
    # Подготовка вектора признаков и инициализация модели
    # =====================================================
    
    #определяем признаки на удаление:
    to_remove_cols = [  'terminal_id',
                        'transaction_id',
                        'tx_datetime',
                        'customer_id',
                        'tx_time_seconds',
                        'tx_time_days',
                        'tx_fraud',
                        'tx_fraud_scenario',
                        'unix_time',
                        'hour',
                        'day_of_week',
                    ]
    
    num_cols = [column for column in df.columns if column not in to_remove_cols]

    # Собираем всё в один вектор (формат для Spark ML)
    assembler = VectorAssembler(
        inputCols=num_cols ,#+ [f"{c}_vec" for c in cat_cols],
        outputCol="features"
    )

    # модель градиентного бустинга
    gbt = GBTClassifier(
        labelCol="tx_fraud", 
        featuresCol="features", 
        maxIter=50,      # количество итераций (деревьев)
        maxDepth=5,      # для бустинга лучше небольшая глубина
        stepSize=0.1,    # скорость обучения (learning rate)
        seed=42,
        weightCol="weight",
        #maxMemoryInMB = 1024,
        #maxBins = 64,
        #subsamplingRate = 0.3,
        #checkpointInterval=10
        )

    # gbt = LightGBMClassifier(
    #         labelCol="tx_fraud",
    #         featuresCol="features",
    #         numIterations=300,
    #         learningRate=0.03,
    #         numLeaves=64,
    #         maxDepth=-1,
    #         minDataInLeaf=50,
    #         featureFraction=0.8,
    #         baggingFraction=0.8,
    #         baggingFreq=1,
    #         lambdaL2=1.0,
    #         isUnbalance=True,
    #         #seed=42
    #     )

    # gbt = LightGBMClassifier(
    #     labelCol="tx_fraud",
    #     featuresCol="features",
    #     numIterations=50,
    #     maxDepth=5,
    #     learningRate=0.1,
    #     numTasks = 2,
    #     numThreads=1,
    #     maxBin=63,
    #     #useBarrierExecutionMode=True
    # )

    # gbt = XGBoostClassifier(
    #     featuresCol="features", 
    #     labelCol="label",
    #     # Основные настройки ресурсов
    #     num_workers=4,          # Должно совпадать с local[4]
    #     nthread=1,              # 1 поток на каждый воркер (чтобы не перегрузить CPU)
        
    #     # Параметры модели
    #     max_depth=6,
    #     eta=0.1,
    #     num_round=100,
    #     objective="binary:logistic",
        
    #     # Трюки для экономии памяти
    #     use_external_memory=True, # Если данных много, будет использовать диск
    #     tree_method="hist",       # Гистограммный метод (быстрее и меньше ест RAM)
    #     missing=0.0               # Явно укажите значение для пропусков
    # )
    
    # =====================================================
    # Обучение, подсчёт метрик и сохранение модели
    # =====================================================
    
    #инициализация клиента, подключаемся к уже существующему эксперименту, если он есть
    client = MlflowClient()
    experiment_name = f"Anti-Fraud-System_Validation2"
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment and not isinstance(experiment,str):
        experiment_id = experiment.experiment_id
    else:
        experiment = client.create_experiment(experiment_name)
        experiment = client.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id

    # Включаем автологирование (запишет параметры GBTClassifier и должна саму модель, 
    # но не записывает)
    mlflow.pyspark.ml.autolog(log_models=False)

    # Формируем имя запуска
    runs = client.search_runs(experiment_ids=[experiment_id],order_by=["metrics.test_f1_score DESC"])
    run_number = len(runs) + 1  # Номер текущего запуска
    current_run_name = f"Fraud_Detection_v_{run_number}"

    # для сравнения после обучения новой модели
    f1_best_run = 0
    if len(runs) > 0:
        best_run = runs[0]
        if 'test_f1_score' in  best_run.data.metrics:
            f1_best_run = best_run.data.metrics['test_f1_score']


    with mlflow.start_run(
        experiment_id=experiment_id,
        run_name=current_run_name
    ) as run:
        
        # Логируем список признаков (как артефакт)

        features_config = {"num_cols": num_cols}
        with open("features.json", "w") as f:
            json.dump(features_config, f)
        
        mlflow.log_artifact("features.json")

        # Обучение
        pipeline = Pipeline(stages=[assembler, gbt])
        model = pipeline.fit(train_weighted)

       
        # Оценка на тренировочном наборе
        # Получение предсказаний 
        predictions = model.transform(train)
        #подсчёт метрик
        precision, recall, f1, avg_fp_per_week = get_metrics_and_log(predictions,prefix='train_')

        if LOG:
            logger.info(f'''precision: {precision}, 
                        recall: {recall}, 
                        f1: {f1}, 
                        avg_fp_per_week: {avg_fp_per_week}''') 
        
        # Оценка на тестовом наборе
        # Получение предсказаний 
        predictions = model.transform(test)
        #подсчёт метрик
        precision, recall, f1, avg_fp_per_week = get_metrics_and_log(predictions,prefix='test_')

        if LOG:
            logger.info(f'''precision: {precision}, 
                        recall: {recall}, 
                        f1: {f1}, 
                        avg_fp_per_week: {avg_fp_per_week}''') 
            
        get_confusion_matrix(predictions)
        
        # Создаем сигнатуру
        input_example = train_weighted.select(num_cols).limit(5).toPandas()
        prediction_sample = model.transform(train_weighted.limit(5)).select("prediction").toPandas()
        signature = infer_signature(input_example, prediction_sample)

        # Записываем модель
        mlflow.spark.log_model(
            spark_model=model, 
            artifact_path="gb_model",
            signature=signature,
            pip_requirements=["pandas==2.3.3", "pyspark==3.0.3"]) 

        if LOG:
            logger.info(f"Run finished. ID: {run.info.run_id}")
            if f1 > f1_best_run:
                logger.info(f"This is best run !")

    spark.stop()

    return

def main() -> None:
    try:
        # Парсинг аргументов
        args = parse_arguments()

        # Логирование параметров запуска
        print("=" * 50)
        print("Запуск скрипта обучения")
        print(f"in path: {args.in_path}")
        print(f"MLFLOW подключение: {args.mlflow_conn}")
        print(f"Логирование: {'Включено' if args.log_stats else 'Выключено'}")
        print(f"Локальный запуск: {'Включено' if args.local else 'Выключено'}")
        print("=" * 50)

        # Запуск обработки данных
        learning(
            in_path=args.in_path,
            out_path=args.out_path,
            master_conn=args.master_conn,
            mlflow_conn=args.mlflow_conn,
            log_stats=args.log_stats,
            local=args.local
        )


    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

#python ./learning.py --in-path=./out_folder_for_ml --log-stats --local &> output.log