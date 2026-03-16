import sys
import argparse
import math
import logging

#import findspark
#findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel



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
        required=True,
        help='Имя S3 бакета для сохранения результатов'
    )

    parser.add_argument(
        '--master-conn',
        type=str,
        required=False,
        default="",
        help='Имя S3 бакета для сохранения результатов'
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
        help='Включить логирование статистики по отфильтрованным данным'
    )

    return parser.parse_args()

# def get_coalesce_number(spark, logger, in_path, target_size_mb=512,zip_coeff=3):
#     """
#     Вычисляет N для coalesce, исходя из размера данных в HDFS.
#     """
#     # Получаем Hadoop конфигурацию и файловую систему
#     sc = spark.sparkContext
#     Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
#     FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
#     conf = sc._jsc.hadoopConfiguration()
#     fs = FileSystem.get(conf)

#     # Получаем информацию о папке
#     path = Path(in_path)
#     if not fs.exists(path):
#         raise FileNotFoundError(f"Путь {in_path} не найден")

#     # Считаем общий размер (ContentSummary учитывает все файлы внутри)
#     content_summary = fs.getContentSummary(path)
#     total_bytes = content_summary.getLength()
#     file_count = content_summary.getFileCount()

#     total_mb = (total_bytes/zip_coeff) / (1024 * 1024)

#     # Вычисляем необходимое количество файлов
#     # Округляем вверх (ceil), чтобы размер файла был НЕ БОЛЕЕ target_size_mb
#     n_coalesce = int(math.ceil(total_mb / target_size_mb))

#     # Если данных слишком мало, всё равно нужен хотя бы 1 файл
#     n_coalesce = max(1, n_coalesce)

#     logger. warning(f"Количество файлов в источнике: {file_count}")
#     logger.warning(f"Общий размер: {total_mb:.2f} MB")
#     logger.warning(f"Рекомендованный coalesce({n_coalesce}) для целевого размера {target_size_mb} MB")

#     return n_coalesce



def get_coalesce_number(
    spark,
    logger,
    in_path: str,
    target_size_mb: int = 512,
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


def clear_data(
        in_path: str,
        out_path: str,
        master_conn: str = "", #IP:PORT
        log_stats: bool = False,
        local: bool = False):

    # =====================================================
    # Конфигурация
    # =====================================================
    INPUT_PATH = in_path
    OUTPUT_PATH = out_path
    MASTER_CONN = master_conn
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
        spark = (
            SparkSession.builder
                .appName("Spark ML Clean Data")
                # 1. Используем все ядра (16), но оставляем 1-2 для системы
                .master("local[14]")
                # 2. Память Драйвера (в локальном режиме это основная настройка)
                # Выделяем 16-20 ГБ, чтобы спокойно делать .toPandas() и обучать модели
                .config("spark.driver.memory", "18g")
                # 3. Лимит на размер объектов, собираемых на драйвере (увеличиваем для тяжелых операций)
                .config("spark.driver.maxResultSize", "8g")
                # 4. Включаем современные оптимизации 2025 года (Adaptive Query Execution)
                .config("spark.sql.adaptive.enabled", "true")
                # 5. Оптимизация работы с памятью при передаче данных в Pandas
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .getOrCreate()
        )

        #spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

    else:
        spark = (
            SparkSession.builder
                .appName("Spark ML Clean Data")
                #.master(f"spark://{MASTER_CONN}")
                #.config("spark.executor.instances", "3")
                #.config("spark.executor.cores", "3")
                #.config("spark.executor.memory", "10g")
                #.config("spark.executor.memoryOverhead", "2g")
                .config("spark.driver.memory", "12g")
                .config("spark.driver.cores", "3")
                #.config("spark.sql.shuffle.partitions", "72")
                #.config("spark.sql.files.maxPartitionBytes", "256m")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate()
        )

    spark.conf.set("spark.sql.ansi.enabled", "false")

    logger = logging.getLogger("DataQuality")
    logger.setLevel(logging.WARN)

    #ch = logging.StreamHandler()
    #formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    #ch.setFormatter(formatter)
    #logger.addHandler(ch)

    #logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("DataQuality")
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    #spark.sparkContext.setLogLevel("WARN")

    #вычисляем максимальное количество выходных файлов
    n_out = get_coalesce_number(spark,logger,INPUT_PATH, target_size_mb=512,zip_coeff=3)

    # =====================================================
    # Чтение данных
    # =====================================================
    schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("tx_datetime", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", IntegerType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True),
        StructField("_corrupt", StringType(), True)
    ])

    df_raw = (
        spark.read
        .option("header", False)
        .option("comment", "#")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .schema(schema)
        .csv(INPUT_PATH)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # =====================================================
    #  Считаем corrupt строки
    # =====================================================
    if LOG:
        dq_corrupt = df_raw.select(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("_corrupt").isNotNull(), 1).otherwise(0)).alias("corrupt_rows")
        )
    #df.limit(5)

    # =====================================================
    # Предобработка данных
    # =====================================================

    # Удаляем битые строки
    df = df_raw.filter(F.col("_corrupt").isNull()).drop("_corrupt")

    # Дубликаты
    window = Window.partitionBy("transaction_id").orderBy("transaction_id")
    df = df.withColumn("rn", F.row_number().over(window))

    # tx_datetime
    df = (
        df.withColumn(
            "tx_datetime",
            F.regexp_replace("tx_datetime", "24:00:00", "23:59:59")
        )
        .withColumn("tx_datetime", F.col("tx_datetime").cast(TimestampType()))
    )

    # customer_id
    df = df.withColumn(
        "customer_id",
        F.when(
            (F.col("customer_id") >= 0) & F.col("customer_id").isNotNull(),
            F.col("customer_id")
        ).otherwise(SPECIAL_CUSTOMER_ID)
    )

    # terminal_id
    df = df.withColumn(
        "terminal_id",
        F.when(
            (F.col("terminal_id") >= 0) & F.col("terminal_id").isNotNull(),
            F.col("terminal_id")
        ).otherwise(SPECIAL_TERMINAL_ID)
    )

    # tx_fraud_scenario
    df = df.withColumn(
        "tx_fraud_scenario",
        F.when(
            (F.col("tx_fraud_scenario") >= 0) & F.col("tx_fraud_scenario").isNotNull(),
            F.col("tx_fraud_scenario")
        ).otherwise(SPECIAL_SCENARIO_ID)
    )

    # tx_time_seconds
    df = df.withColumn(
        "tx_time_seconds",
        F.when(
            (F.col("tx_time_seconds") >= 0) & F.col("tx_time_seconds").isNotNull(),
            F.col("tx_time_seconds")
        ).otherwise(SPECIAL_TIME_SECONDS)
    )

    # bad_tx_time_days
    df = df.withColumn(
        "tx_time_days",
        F.when(
            (F.col("tx_time_days") >= 0) & F.col("tx_time_days").isNotNull(),
            F.col("tx_time_days")
        ).otherwise(SPECIAL_TIME_DAYS)
    )


    #!!!
    #df.persist(StorageLevel.MEMORY_AND_DISK)

# =====================================================
# Логирование, если задано
# =====================================================

    if LOG:

        dq_stats = df.select(
            F.count("*").alias("rows_after_clean"),
            #F.countDistinct("transaction_id").alias("unique_transactions"),
            F.sum(F.when(F.col("rn") > 1, 1).otherwise(0)).alias("duplicates"),
            F.sum(F.when(~((F.col("transaction_id") >= 0) & (F.col("transaction_id").isNotNull())), 1).otherwise(0)).alias("bad_transaction_id"),
            F.sum(F.when(F.col("tx_datetime").isNull(), 1).otherwise(0)).alias("bad_tx_datetime"),
            F.sum(F.when(F.col("customer_id") == SPECIAL_CUSTOMER_ID, 1).otherwise(0)).alias("bad_customer_id"),
            F.sum(F.when(F.col("terminal_id") == SPECIAL_TERMINAL_ID, 1).otherwise(0)).alias("bad_terminal_id"),
            F.sum(F.when(~((F.col("tx_amount") >= 0) & (F.col("tx_amount").isNotNull()) ), 1).otherwise(0)).alias("bad_tx_amount"),
            F.sum(F.when(F.col("tx_time_seconds") == SPECIAL_TIME_SECONDS, 1).otherwise(0)).alias("bad_tx_time_seconds"),
            F.sum(F.when(F.col("tx_time_days") == SPECIAL_TIME_DAYS, 1).otherwise(0)).alias("bad_tx_time_days"),
            F.sum(F.when(F.col("tx_fraud_scenario") == SPECIAL_SCENARIO_ID, 1).otherwise(0)).alias("bad_scenario_id"),
            F.sum(F.when(~((F.col("tx_fraud").isin([0,1])) & (F.col("tx_fraud").isNotNull())), 1).otherwise(0)).alias("bad_tx_fraud")
        )

        dq1 = dq_corrupt.collect()[0]
        dq2 = dq_stats.collect()[0]

        logger.warning(
            f"""
            DATA QUALITY REPORT
            -------------------
            total_rows:            {dq1['total_rows']}
            corrupt_rows:          {dq1['corrupt_rows']}

            rows_after_clean:      {dq2['rows_after_clean']}
            duplicates:            {dq2['duplicates']}

            bad_transaction_id:    {dq2['bad_transaction_id']}
            bad_tx_datetime:       {dq2['bad_tx_datetime']}
            bad_customer_id:       {dq2['bad_customer_id']}
            bad_terminal_id:       {dq2['bad_terminal_id']}
            bad_scenario_id:       {dq2['bad_scenario_id']}
            bad_tx_amount:         {dq2['bad_tx_amount']}
            bad_tx_time_seconds:   {dq2['bad_tx_time_seconds']}
            bad_tx_time_days:      {dq2['bad_tx_time_days']}
            bad_tx_fraud:          {dq2['bad_tx_fraud']}
            """
            )

    # =====================================================
    # Фильтрация и финальное приведение типов
    # =====================================================

    df_clean = (
        df.filter(
            (F.col("rn") == 1) &
            (F.col("transaction_id") >= 0) &
            F.col("transaction_id").isNotNull() &
            F.col("tx_datetime").isNotNull() &
            (F.col("tx_amount") >= 0) &
            F.col("tx_amount").isNotNull() &
            F.col("tx_fraud").isin([0, 1]) &
            F.col("tx_fraud").isNotNull()
        )
        .withColumn("tx_fraud", F.col("tx_fraud").cast(BooleanType()))
        .drop("rn")
    )


    # df.filter(df["customer_id"]==-1).limit(5)

    # =====================================================
    # Запись результата
    # =====================================================
    (
        df_clean
        .coalesce(n_out)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


def main() -> None:
    try:
        # Парсинг аргументов
        args = parse_arguments()

        # Логирование параметров запуска
        print("=" * 50)
        print("Запуск скрипта обработки данных")
        print(f"in path: {args.in_path}")
        print(f"out path: {args.out_path}")
        print(f"HDFS хост:порт: {args.master_conn}")
        print(f"Логирование фильтраций: {'Включено' if args.log_stats else 'Выключено'}")
        print(f"Локальный запуск: {'Включено' if args.local else 'Выключено'}")
        print("=" * 50)

        # Запуск обработки данных
        clear_data(
            in_path=args.in_path,
            out_path=args.out_path,
            master_conn=args.master_conn,
            log_stats=args.log_stats,
            local=args.local
        )


    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

#python infra/scripts/clean_data.py --hdfs-path=/user/ubuntu/data/2022-11-04.txt \
#--s3-bucket-path=s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data \
#--log-stats \
#--local

# yc dataproc job submit spark \
#   --cluster-name otus-dataproc-cluster \
#   --name clean-fraud-data \
#   --main-python-file infra\scripts\clean_data.py \
#   --args "--hdfs-path=/user/ubuntu/data/2022-11-04.txt --s3-bucket-path=s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data "


#spark-submit s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/scpts/clean_data.py \
#--hdfs-path /user/ubuntu/data/2022-11-04.txt \
#--s3-bucket-path s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data


#python clean_data.py \
#--hdfs-path="/media/rk/500гб/Обучение/MLOps/16 Валидация данных/data" \
#--s3-bucket-path="/media/rk/500гб/Обучение/MLOps/16 Валидация данных/data_parquet" \
#--log-stats \
#--local
