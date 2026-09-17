import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    stddev,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = 'UserAnomalyDetection') -> SparkSession:
    spark = SparkSession.builder.appName(app_name).config("spark.sql.adaptive.enabled", "true").config("spark.sql.adaptive.coalescePartitions.enabled", "true").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark сессия создана: {app_name}")

    return spark

def load_events(spark: SparkSession, filepath: str) -> SparkDataFrame:
    logger.info(f"Загрузка событий из {filepath}")
    SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("device", StringType(), True),
    StructField("is_anomaly", IntegerType(), True),
    ])

    df = spark.read.option("header", "true").option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").schema(SCHEMA).csv(filepath)
    logger.info("Загрузка завершена")

    return df

def detected_zscore_anomalies(df: SparkDataFrame, threshold: float = 3.0) -> SparkDataFrame:

    logger.info(f"Детекция аномалий через Z-score (threshold = {threshold})")

    stats = df.groupBy("user_id").agg(
        avg("amount").alias("avg_amount"),
        stddev("amount").alias("std_amount"),
        count("*").alias("event_count")
    )

    df_with_stats = df.join(stats, on="user_id", how="left")

    df_with_zscore = df_with_stats.withColumn(
        "zscore",
        when(
            col("std_amount") > 0,
            (col("amount") - col("avg_amount")) / col("std_amount")
        ).otherwise(0)
    )

    anomalies = df_with_zscore.filter(
        spark_abs(col("zscore")) > threshold
        ).cache()
    
    logger.info(f"Найдено {anomalies.count()} аномалий через Z-score")
    return anomalies

def detected_iqr_anomalies(df: SparkDataFrame) -> SparkDataFrame: 
    logger.info("Детекция аномалий через IQR")
    quantiles = df.approxQuantile("amount", [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]

    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    logger.info(f"Q1={q1:.2f}, Q3={q3:.2f}, IQR={iqr:.2f}")
    logger.info(f"Границы: [{lower_bound:.2f}, {upper_bound:.2f}]")

    anomalies = df.filter(
        (col("amount") < lower_bound) | (col("amount") > upper_bound)
    ).cache()
    logger.info(f"Найдено аномалий {anomalies.count()} через IQR")
    return anomalies

def detect_rapid_activity(df: SparkDataFrame, window_seconds: int = 60, threshold: int = 10) -> SparkDataFrame:
    logger.info("Поиск аномалий по всплеску активности")
    rapid = df.groupBy(
        "user_id", 
        window(col("timestamp"), f"{window_seconds} seconds")
        ).agg(
            count("*").alias("events_in_window")
        ).filter(col("events_in_window") > threshold
                 ).cache()
    logger.info(f"Найдено всплесков: {rapid.count()}")
    return rapid

def detected_device_anomalies(df: SparkDataFrame, usual_threshold: int = 3) -> SparkDataFrame:
    logger.info("Поиск аномалий дефектных устройств")
    suspicious_devices = df.filter(
        (col("device") == "Unknown") | (col("device").isNull())
                                   ).cache()

    logger.info(f"Подозрительных устройств: {suspicious_devices.count()}")

    usual_devices = df.groupBy("user_id", "device").agg(
        count("*").alias("usage_count")
    ).filter(
        col("usage_count") >= usual_threshold
    )
    
    df_with_usual = df.join(
        broadcast(usual_devices),
        on=["user_id", "device"],
        how="left"
    )

    
    new_devices = df_with_usual.filter(
        (col("usage_count").isNull()) &
        (col("device") != "Unknown") &
        (col("device").isNotNull())
    ).drop("usage_count")

    
    anomalies = suspicious_devices.unionByName(new_devices).dropDuplicates(["event_id"]).cache()
    
    logger.info(f"Всего аномалий устройств: {anomalies.count()}")
    
    return anomalies

def compare_with_ground_truth(df: SparkDataFrame, detected_df: SparkDataFrame, method_name: str = "Unknown") -> dict:
    pass

def save_animalies(df: SparkDataFrame, output_path: str, mode: str = "overwrite"):
    pass

if __name__ == "__main__":
    try:
        spark = create_spark_session()

        spark.stop()


    except Exception as e:
        logger.error(f"Ошибка при выполнении {e!s}")
        raise