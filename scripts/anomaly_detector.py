import logging
import os
import sys

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    stddev,
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

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# def create_spark_session(app_name: str = "UserAnomalyDetection") -> SparkSession:
#     spark = SparkSession.builder \
#         .appName(app_name) \
#         .config("spark.sql.adaptive.enabled", "true") \
#         .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
#         .getOrCreate()
    
#     spark.sparkContext.setLogLevel("WARN")
#     logger.info(f"Spark-сессия создана: {app_name}")
    
#     return spark

def create_spark_session(app_name: str = 'UserAnomalyDetection') -> SparkSession:
    master = os.getenv("SPARK_MASTER", "local[*]")
    spark = SparkSession.builder \
                .appName(app_name) \
                .master(master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.python.worker.reuse", "false") \
                .config("spark.python.worker.timeout", "600") \
                .getOrCreate()



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

def detected_zscore_anomalies(df: SparkDataFrame, threshold: float = 2.0) -> SparkDataFrame:

    logger.info(f"Детекция аномалий через Z-score (threshold = {threshold})")

    purchases = df.filter(col("event_type") == "purchase")
    stats = purchases.groupBy("user_id").agg(
        avg("amount").alias("avg_amount"),
        stddev("amount").alias("std_amount"),
        count("*").alias("event_count")
    )

    df_with_stats = purchases.join(stats, on="user_id", how="left")

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

    purchases = df.filter(col("event_type") == "purchase")
    quantiles = purchases.approxQuantile("amount", [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]

    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    logger.info(f"Q1={q1:.2f}, Q3={q3:.2f}, IQR={iqr:.2f}")
    logger.info(f"Границы: [{lower_bound:.2f}, {upper_bound:.2f}]")

    anomalies = purchases.filter(
        (col("amount") < lower_bound) | (col("amount") > upper_bound)
    ).cache()
    logger.info(f"Найдено аномалий {anomalies.count()} через IQR")
    return anomalies

def detect_rapid_activity(df: SparkDataFrame, window_seconds: int = 10, threshold: int = 20) -> SparkDataFrame:
    logger.info("Поиск аномалий по всплеску активности")
    rapid = df.groupBy(
        "user_id", 
        window(col("timestamp"), f"{window_seconds} seconds")
        ).agg(
            count("*").alias("events_in_window")
        ).filter(col("events_in_window") > threshold
                 ).select(
                     "user_id",
                     col("window.start").alias("w_start"),
                     col("window.end").alias("w_end")
                     )

    rapid_events = df.join(
        broadcast(rapid), 
        on="user_id",
        how="inner"
        ).filter(
            (col("timestamp") >= col("w_start")) & 
            (col("timestamp") < col("w_end"))
             ).drop("w_start", "w_end").cache()


    logger.info(f"Найдено всплесков: {rapid_events.count()}")
    return rapid_events

def detected_device_anomalies(df: SparkDataFrame) -> SparkDataFrame:
    logger.info("Поиск аномалий по устройствам (Unknown / null)")

    anomalies = df.filter(
        (col("device") == "Unknown") | (col("device").isNull())
    ).cache()

    logger.info(f"Найдено аномалий по устройствам: {anomalies.count()}")
    return anomalies

def compare_with_ground_truth(df: SparkDataFrame, detected_df: SparkDataFrame, method_name: str = "Unknown") -> dict:
    logger.info(f"Сравниваем {method_name} с ground truth")
    actual_df = df.filter(col("is_anomaly") == 1).select("event_id").distinct()
    predicted_df = detected_df.select("event_id").distinct()

    true_positives = actual_df.join(predicted_df, on="event_id", how="inner").count()
    false_positives = predicted_df.join(actual_df, on="event_id", how="left_anti").count()
    false_negatives = actual_df.join(predicted_df, on="event_id", how="left_anti").count()

    actual_count = actual_df.count()
    predicted_count = predicted_df.count()

    precision = true_positives / predicted_count if predicted_count > 0 else 0
    recall = true_positives / actual_count if actual_count > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    metrics = {
        "method":method_name,
        "actual_anomalies": actual_count,
        "predicted_anomalies": predicted_count,
        "true_positives":true_positives,
        "false_positives":false_positives,
        "false_negatives":false_negatives,
        "precision":round(precision, 4),
        "recall":round(recall, 4),
        "f1_score":round(f1, 4),
        }
    logger.info(f"Реальных аномалий: {metrics['actual_anomalies']}")
    logger.info(f"Найдено аномалий: {metrics['predicted_anomalies']}")
    logger.info(f"True Positive: {metrics['true_positives']}")
    logger.info(f"False Positive: {metrics['false_positives']}")
    logger.info(f"False Negative: {metrics['false_negatives']}")
    logger.info(f"Точность: {metrics['precision']}")
    logger.info(f"Полнота: {metrics['recall']}")
    logger.info(f"F1-score: {metrics['f1_score']}")
    return metrics

def save_anomalies(df: SparkDataFrame, output_path: str, mode: str = "overwrite"):
    logger.info(f"Сохранение аномалий в {output_path}")

    df.coalesce(1).write.mode(mode).option("header", "true").csv(output_path)

    logger.info(f"Аномалии сохранены в {output_path}")





if __name__ == "__main__":
    spark = None
    try:
        input_file = sys.argv[1] if len(sys.argv) > 1 else "../data/events/test_events.csv"
        output_path = sys.argv[2] if len(sys.argv) > 2 else "../data/anomalies/"

        spark = create_spark_session()
        

        df = load_events(spark, input_file)

        z_score_anomalies = detected_zscore_anomalies(df=df)
        iqr_anomalies = detected_iqr_anomalies(df=df)
        rapid_activity = detect_rapid_activity(df=df)
        device_anomalies = detected_device_anomalies(df=df)

        metrics_zscore = compare_with_ground_truth(df=df, detected_df=z_score_anomalies, method_name="Z-score")
        metrics_iqr = compare_with_ground_truth(df=df, detected_df=iqr_anomalies, method_name="IQR")
        metrics_rapid_activity = compare_with_ground_truth(df=df, detected_df=rapid_activity, method_name="Rapid Activity")
        metrics_devices = compare_with_ground_truth(df=df, detected_df=device_anomalies, method_name="Device")

        save_anomalies(z_score_anomalies, f"{output_path}/zscore")
        save_anomalies(iqr_anomalies, f"{output_path}/iqr")
        save_anomalies(rapid_activity, f"{output_path}/rapid")
        save_anomalies(device_anomalies, f"{output_path}/device")

        logger.info("Детекция завершена")
        logger.info("Итоговые данные: ")
        logger.info(f"  - Z-score: {metrics_zscore['predicted_anomalies']} аномалий "
                    f"(precision={metrics_zscore['precision']}, "
                    f"recall={metrics_zscore['recall']}, "
                    f"f1={metrics_zscore['f1_score']})")
        logger.info(f"  - IQR: {metrics_iqr['predicted_anomalies']} аномалий "
                    f"(precision={metrics_iqr['precision']}, "
                    f"recall={metrics_iqr['recall']}, "
                    f"f1={metrics_iqr['f1_score']})")
        logger.info(f"  - Rapid: {metrics_rapid_activity['predicted_anomalies']} событий "
                    f"(precision={metrics_rapid_activity['precision']}, "
                    f"recall={metrics_rapid_activity['recall']}, "
                    f"f1={metrics_rapid_activity['f1_score']})")
        logger.info(f"  - Devices: {metrics_devices['predicted_anomalies']} аномалий "
                    f"(precision={metrics_devices['precision']}, "
                    f"recall={metrics_devices['recall']}, "
                    f"f1={metrics_devices['f1_score']})")
    except Exception as e:
        logger.error(f"Ошибка при выполнении {e!s}")
        raise
    finally:
        if spark is not None:
            spark.stop()