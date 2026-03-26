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
        help='подключение к мастер ноде'
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

def interval_to_seconds(interval_str):
    """Превращает '1 minute', '2 hours', '10s' в секунды (int)."""
    units = {
        "s": 1, "second": 1, "seconds": 1,
        "m": 60, "minute": 60, "minutes": 60,
        "h": 3600, "hour": 3600, "hours": 3600,
        "d": 86400, "day": 86400, "days": 86400
    }
    match = re.match(r"(\d+)\s*([a-zA-Z]+)", interval_str.lower().strip())
    if match:
        value, unit = match.groups()
        return int(value) * units.get(unit, 0)
    return None

def create_windows(partition_col, time_col, window_definitions, prefix_name='tx'):
    """
    window_definitions: dict вида {"название": (start_sec, end_sec)}
    Пример: {"1d_hist": (-86400, -1), "1d_full": (-86399, 0)}
    """
    windows = {}
    for name, (start, end) in window_definitions.items():
        windows[f'{prefix_name}_{name}'] = Window.partitionBy(*partition_col) \
                              .orderBy(time_col) \
                              .rangeBetween(start, end)
    #print(',\n'.join(windows.keys()))
    return windows


def add_aggregated_features(df, target_col, windows, agg_types=["count", "avg"],prefix=''):
    """
    df: DataFrame
    target_col: колонка для агрегации (напр. 'tx_amount')
    windows: словарь объектов WindowSpec из create_windows
    agg_types: типы агрегатов
    """
    for win_name, win_spec in windows.items():
        # Базовые агрегаты
        if "count" in agg_types or "avg" in agg_types or "isnew" in agg_types:
            count_col = f"{prefix}{target_col}_cn_{win_name}"
            df = df.withColumn(count_col, F.count(target_col).over(win_spec))
        
        if "sum" in agg_types or "avg" in agg_types:
            sum_col = f"{prefix}temp_sum_{win_name}"
            df = df.withColumn(sum_col, F.sum(target_col).over(win_spec))
            
            if "avg" in agg_types:
                # В Spark avg через Window может быть медленнее, чем sum/count,
                # но при наличии count это просто деление
                df = df.withColumn(f"{prefix}{target_col}_avg_{win_name}", F.col(sum_col) / F.col(count_col))
            
            if "sum" not in agg_types:
                df = df.drop(sum_col)
                
        if "std" in agg_types:
             df = df.withColumn(f"{target_col}_std_{win_name}", F.stddev(F.col(target_col)).over(win_spec))

        if "isnew" in agg_types:
            isnew_col = f"{prefix}{target_col}_isnew_{win_name}"
            df = df.withColumn(isnew_col, F.when(F.col(count_col) == 0, 1).otherwise(0))

        if "count" not in agg_types and ("avg" in agg_types or "isnew" in agg_types):
            df = df.drop(count_col)
              
    return df

def add_ratio_features(df, target_col, windows, agg_types="avg"):
    """
    df: DataFrame
    target_col: колонка для агрегации (напр. 'tx_amount')
    windows: словарь объектов WindowSpec (нужно использовать окна с -1 или большей задержкой)
    agg_type: тип исторического агрегата для сравнения ('avg', 'sum', 'max')
    """
    prefix='T__prefix__'
    cols = df.columns
    #print(cols)
    df = add_aggregated_features(df, target_col, windows, agg_types=agg_types,
                                 prefix=prefix)
    #print(df.columns)
    hist_agg_cols = [cl for cl in df.columns if not cl in cols]
    #print(hist_agg_cols)
    for col_name in hist_agg_cols:
        ratio_col_name = f"ratio_{col_name.replace(prefix,'')}"
    
        # Используем coalesce, чтобы избежать деления на NULL или 0
        # Если истории нет, рейт обычно принимается равным 1.0 (норма)
        df = df.withColumn(
            ratio_col_name, 
            F.col(target_col) / F.when(F.col(col_name) != 0, F.col(col_name)).otherwise(F.col(target_col))
        )
    
    # Удаляем промежуточную колонку с агрегатом, чтобы не замусоривать датасет
    df = df.drop(*hist_agg_cols)
        
    return df

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
        # Если истории нет, рейт обычно принимается равным 1.0 (норма)
        df = df.withColumn(
            ratio_col_name, 
            F.col(target_col) / F.when(F.col(col_name) != 0, F.col(col_name)).otherwise(F.col(target_col))
        )
    
    # Удаляем промежуточную колонку с агрегатом, чтобы не замусоривать датасет
    if drop_hist:
        df = df.drop(*hist_agg_cols)
        
    return df
    
def add_aggregated_features_for_heavy(df, partition_cols, time_col, target_col, 
                                      windows_definitions, 
                                      bucket_interval=60,
                                      agg_types=['count'],
                                      include_current=True,
                                      end_of_current=0,
                                      prefix = ""
                                      ):
    """
    Расчет агрегатов (AVG, STD, COUNT) с учетом текущей транзакции 
    без перекоса данных.
    для включения в результат текущих значений все окна должны оканчиваться одинаково на -bucket_interval
    """
    
    need_count = "count" in agg_types
    need_sum = "sum" in agg_types
    need_avg = "avg" in agg_types
    need_std = "std" in agg_types

    # 1. СТАДИЯ "СЖАТИЯ" (Stage 1: Bucket Aggregation)
    # Создаем временные бакеты
    #df_with_buckets = df.withColumn(
    #"_bucket", 
    #F.window(F.col(time_col), bucket_interval)["start"].cast("long")
    #)
    df_with_buckets = df.withColumn(
        "_bucket", 
        (F.col(time_col) / bucket_interval).cast("long") * bucket_interval
    )
    
    agg_ops = []

    if need_count or need_avg:
        agg_ops.append(F.count(target_col).alias("_b_cnt"))
    
    if need_sum or need_avg:
        agg_ops.append(F.sum(target_col).alias("_b_sum"))
            
    if need_std:
        agg_ops.append(F.sum(F.col(target_col)**2).alias("_b_sum_sq"))
    
    
    # Агрегируем компоненты для сумм, средних и стандартного отклонения
    df_buckets = df_with_buckets.groupBy(*partition_cols, "_bucket").agg(
        *agg_ops
    )
    
    # 2. СТАДИЯ "ИСТОРИИ" (Stage 2: Sliding Windows on Buckets)
    # Рассчитываем агрегаты только по ПРОШЛЫМ бакетам (не включая текущий)

    # 2. СТАДИЯ "ИСТОРИИ"
    # ВАЖНО: Пересоздаем окна специально для df_buckets по колонке _bucket
    for win_name, (start, end) in windows_definitions.items():
        # Создаем новое окно для агрегации бакетов
        win_spec = Window.partitionBy(*partition_cols).orderBy("_bucket").rangeBetween(start, end)
        
        if need_count or need_avg:
            count_col = f"{prefix}_{target_col}_cn_{win_name}"
            df_buckets = df_buckets.withColumn(count_col+"_hist", F.sum("_b_cnt").over(win_spec))
        if need_sum or need_avg:
            sum_col = f"{prefix}_{target_col}_sum_{win_name}"
            df_buckets = df_buckets.withColumn(sum_col+"_hist", F.sum("_b_sum").over(win_spec))
        if need_std:
            sum_sq_col = f"{prefix}_{target_col}_sum_sq_{win_name}"
            df_buckets = df_buckets.withColumn(sum_sq_col+"_hist", F.sum("_b_sum_sq").over(win_spec))

    # 3. Соединение и инкремент (Stage 3)
    df_final = df_with_buckets.join(df_buckets.drop("_b_sum", "_b_cnt", "_b_sum_sq"), 
                                    on=[*partition_cols, "_bucket"], how="left")
    

    if include_current:
        win_local = Window.partitionBy(*partition_cols, "_bucket").orderBy(time_col) \
                          .rangeBetween(Window.unboundedPreceding, end_of_current)
        
        if need_count or need_avg or need_std:
            curr_cnt = F.count(target_col).over(win_local)
        if need_sum or need_avg:
            curr_sum = F.sum(target_col).over(win_local)
        if need_std:
            curr_sum_sq = F.sum(F.col(target_col)**2).over(win_local)
    else:
        # Если текущий не нужен, инкремент равен нулю
        curr_sum = curr_cnt = curr_sum_sq = F.lit(0)

    
    
     # 4. ФИНАЛЬНЫЙ РАСЧЕТ И ОБРАБОТКА NULL (Stage 4: Fusion & Cleaning)
    drop_list = ["_bucket"]
    for win_name in windows_definitions.keys():
        # Берем историю из Join (если ее нет — 0)
        if need_count or need_avg or need_std:
            count_col = f"{prefix}_{target_col}_cn_{win_name}"
            h_cnt = F.coalesce(F.col(count_col+"_hist"), F.lit(0))
            curr_sum = F.coalesce(curr_sum,F.lit(0))
            total_cnt = h_cnt + curr_cnt
            drop_list.append(count_col+"_hist")
        if need_sum or need_avg:
            sum_col = f"{prefix}_{target_col}_sum_{win_name}"
            h_sum = F.coalesce(F.col(sum_col+"_hist"), F.lit(0))
            curr_cnt = F.coalesce(curr_cnt,F.lit(0))
            total_sum = h_sum + curr_sum
            drop_list.append(sum_col+"_hist")
        if need_std:
            sum_sq_col = f"{prefix}_{target_col}_sum_sq_{win_name}"
            h_sum_sq = F.coalesce(F.col(sum_sq_col+"_hist"), F.lit(0))
            curr_sum_sq = F.coalesce(curr_sum_sq,F.lit(0))
            total_sum_sq = h_sum_sq + curr_sum_sq
            
            std_col_name = f"{prefix}_{target_col}_std_{win_name}"
            variance = (total_sum_sq / total_cnt) - ( (total_sum / total_cnt)**2 )
            df_final = df_final.withColumn(std_col_name,
                        F.when(total_cnt > 1, F.sqrt(F.greatest(F.lit(0), variance))).otherwise(F.lit(0)))
            
            drop_list.append(sum_sq_col+"_hist")

   
        # Базовые агрегаты
        if need_count:
            df_final = df_final.withColumn(count_col, total_cnt)
        if need_sum:
            df_final = df_final.withColumn(sum_col, total_sum)
        if need_avg:
            avg_col_name = f"{prefix}_{target_col}_avg_{win_name}"
            df_final = df_final.withColumn(avg_col_name,
                        F.when(total_cnt > 0, total_sum / total_cnt).otherwise(F.col(target_col)))

    # Финальная чистка технических колонок
    df_final = df_final.drop(*drop_list)
    #print(drop_list)
    return df_final

def get_count_risk_rolling_window_spark(df,full_colls, delay_cols, del_in_cols = True):
    """подсчёт риска терминалов"""
    col_delay_sum = [col for col in delay_cols if "_sum_" in col][0]
    col_delay_cn = [col for col in delay_cols if "_cn_" in col][0]

    drop_list = []
    for sum_col_name,cn_col_name in full_colls:
       
        # Вычитаем задержку, чтобы получить чистое окно в прошлом
        nb_fraud_window = F.col(sum_col_name) - F.col(col_delay_sum)
        nb_tx_window = F.col(cn_col_name) - F.col(col_delay_cn)
        
        # Считаем риск (используем nullif во избежание деления на ноль)
        risk_window = nb_fraud_window / F.when(nb_tx_window == 0, None).otherwise(nb_tx_window)
        
        # Добавляем колонки
        df = df.withColumn(f"{cn_col_name}_delay", F.coalesce(nb_tx_window, F.lit(0))) \
               .withColumn(f"{sum_col_name.replace('_sum_','_risk_')}_delay", F.coalesce(risk_window, F.lit(0.0)))

        drop_list.append(sum_col_name)
        drop_list.append(cn_col_name)
        
    drop_list.append(col_delay_sum)
    drop_list.append(col_delay_cn)

    if del_in_cols:
        df = df.drop(*drop_list)
    
    return df


def prepair_data(
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
        print("LOCAL RUN !!!")
        spark = (
            SparkSession.builder
                .appName("Spark ML Prepair Data")
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
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                #.config("spark.sql.adaptive.skewJoin.enabled", "true")
                #.config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
                #.config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
                #.config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:0.11.1") \
                #.config("spark.jars.repositories", "https://mmlspark.azureedge.net") \
                .getOrCreate()
        )

        #spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

    else:
        spark = (
            SparkSession.builder
                .appName("Spark ML Prepair")
                # Пути к Python (чтобы не было конфликтов версий)
                #.config("spark.pyspark.python", "/usr/bin/python3")
                #.config("spark.pyspark.driver.python", "/usr/bin/python3")
                
                # Драйвер
                .config("spark.driver.memory", "12g")
                .config("spark.driver.maxResultSize", "8g") 

                # Исполнители
                .config("spark.executor.instances", "4") 
                .config("spark.executor.cores", "4")
                .config("spark.executor.memory", "10g")
                .config("spark.executor.memoryOverhead", "4g")

                # параллелизм
                .config("spark.sql.shuffle.partitions", "128")
                .config("spark.default.parallelism", "128")

                # Адаптивность
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Склеивать мелкие части
                .config("spark.sql.adaptive.skewJoin.enabled", "true")           # Обработка перекосов данных
                #.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb") # целевой размер раздела при склейке

                # сетевой таймаут
                .config("spark.network.timeout", "600s")
                .getOrCreate()
        )


    spark.conf.set("spark.sql.ansi.enabled", "false")

    logger = logging.getLogger("PrepairData")
    logger.setLevel(logging.WARN)

    #ch = logging.StreamHandler()
    #formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    #ch.setFormatter(formatter)
    #logger.addHandler(ch)

    #logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("DataQuality")
    #sc = spark.sparkContext
    #sc.setLogLevel("WARN")
    #spark.sparkContext.setLogLevel("WARN")

    #вычисляем максимальное количество выходных файлов
    n_out = get_coalesce_number(spark,logger,INPUT_PATH, target_size_mb=128,zip_coeff=.2)

    # =====================================================
    # Чтение данных
    # =====================================================
    schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("tx_datetime", TimestampType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", IntegerType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("tx_fraud", BooleanType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True)
    ])

    df = (
        spark.read
        .schema(schema)
        .parquet(INPUT_PATH)
        #.persist(StorageLevel.MEMORY_AND_DISK)
    )
    #if SAMPLE_FRAQ > 0 and SAMPLE_FRAQ <=1:
    #    df=df.sample(fraction=SAMPLE_FRAQ, seed=42)

    # =====================================================
    # Генерация признаков
    # =====================================================
    HOUR = 3600
    DAY = 24 * 3600
    WEEK = 7 * 24 * 3600
    MONTH = 30 * 24 * 3600
    TMIN = 1*60
    bucket_interval = 3600 # для подсчёта терминалов

    #создаём дополнительную колонку unix_time, переводим tx_fraud в int
    features_df = df.withColumn("unix_time", F.col("TX_DATETIME").cast("long"))
    features_df = features_df.withColumn("tx_fraud", col("tx_fraud").cast(IntegerType()))
    features_df.columns

    # Определяем временные границы для "прогрева" признаков
    # Дата самой первой транзакции, чтобы отсчитать от нее 30 дней - время прогрева
    min_date = features_df.select(F.min("unix_time")).collect()[0][0]
    max_date = features_df.select(F.max("unix_time")).collect()[0][0]
    start_training_date = min_date + 3600*24*30+3600

    if LOG:
        logger.info(f'del: {datetime.fromtimestamp(min_date)} --> {datetime.fromtimestamp(start_training_date)}')
        logger.info(f'keep: {datetime.fromtimestamp(start_training_date)} --> {datetime.fromtimestamp(max_date)}')


    #определяем основные окна

    TIME_COL = "unix_time"

    # задаём параметры окон
    ct_win_defs = {"30d_hist": (-DAY*30-1, -1),
                   "7d_hist": (-DAY*7-1, -1)}

    win_defs_current = {
                "7d_full": (-WEEK, 0),
                "1d_full": (-DAY, 0),
                "1h_full": (-HOUR, 0),
            }


    win_defs_hist = {
                "30d_hist": (-MONTH-HOUR, -HOUR),
                "7d_hist": (-WEEK-HOUR, -HOUR),
                "1d_hist": (-DAY-HOUR, -HOUR),
            }

    #инициализируем окна по клиенту
    win_ct = create_windows(["customer_id","terminal_id"], TIME_COL, ct_win_defs, 
                            prefix_name = 'ct')


    win_cust_current = create_windows(["customer_id"], TIME_COL, win_defs_current, 
                            prefix_name = 'cust')

    win_cust_hist = create_windows(["customer_id"], TIME_COL, win_defs_hist, 
                            prefix_name = 'cust')



    if LOG:
        logger.info(f"Окна созданы")

    # Был ли клиент на этом терминале хоть раз за 30 дней
    features_df = add_aggregated_features(features_df,'tx_amount', 
                                        win_ct, agg_types=["isnew"])

    #статистики по клиенту с текущими значениями
    features_df = add_aggregated_features(features_df,'tx_amount', win_cust_current, agg_types=["count", "avg"])

    #статистики по клиенту с историческими значениями
    features_df = add_aggregated_features(features_df,'tx_amount', win_cust_hist, agg_types=["avg","std",'count'])


    #статистики по терминалу текущие 
    # (терминал обрабатывается другими функциями, использующими подсчёт по батчам + текущие статистики внутри батча)
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount', 
                                        win_defs_current, 
                                        bucket_interval=bucket_interval, 
                                        agg_types=["avg","count"],
                                        include_current=True,
                                        end_of_current=0,
                                        prefix='term')

    #статистики по терминалу исторические
    #features_df = add_aggregated_features(features_df,'tx_amount', win_term_current, agg_types=["count", "avg","std"])

    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount', 
                                        win_defs_hist, 
                                        bucket_interval=bucket_interval, 
                                        agg_types=["avg","count",'std'],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='term')


    #подсчёт новых клиентов на терминале за 30 дней
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount_isnew_ct_30d_hist', 
                                        win_defs_current,
                                        bucket_interval=bucket_interval, 
                                        agg_types=["sum"],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='term')

    #подсчёт новых клиентов на терминале за 7 дней
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount_isnew_ct_7d_hist', 
                                        win_defs_current,
                                        bucket_interval=bucket_interval, 
                                        agg_types=["sum"],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='term')

    #подсчёт риска терминала
    win_delay_fraud  = {
                "7d_delay": (-WEEK, 0)
            }

    win_fraud  = {
                "1d_full": (-WEEK-DAY, 0),
                "7d_full": (-WEEK-WEEK, 0)
            }

    #получаем базовое окно задержки для вычитания
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_fraud', 
                                        win_delay_fraud,
                                        bucket_interval=bucket_interval, 
                                        agg_types=["sum","count"],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='fraud')

    #считаем полные окна с задержкой 
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_fraud', 
                                        win_fraud,
                                        bucket_interval=3600, 
                                        agg_types=["sum","count"],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='fraud')

    features_df = get_count_risk_rolling_window_spark(features_df,
                                                [('fraud_tx_fraud_sum_1d_full','fraud_tx_fraud_cn_1d_full'),
                                                    ('fraud_tx_fraud_sum_7d_full','fraud_tx_fraud_cn_7d_full')], 
                                                ['fraud_tx_fraud_cn_7d_delay',
                                                'fraud_tx_fraud_sum_7d_delay'],
                                                    del_in_cols = True)

    # Убираем первую неделю, чтобы убрать записи, где признаки не успели накопиться
    features_df = features_df.filter(F.col("unix_time") >= start_training_date)

    # =====================================================
    # Запись результата
    # =====================================================
    (
        features_df
        .coalesce(n_out)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()

    return

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
        prepair_data(
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

# python ./proc_for_ml_data.py \
# --in-path=s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data_for_test_in \
# --out-path=./out_folder \
# --log-stats \
# --local

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