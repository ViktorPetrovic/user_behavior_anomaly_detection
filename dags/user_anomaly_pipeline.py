import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
import pandas

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
sys.path.append("/opt/airflow")

from src.generators.event_generator import EventGenerator

DATA_DIR = Path("/opt/airflow/data/events")
ANOMALIES_DIR = Path("/opt/airflow/data/anomalies")




def generate_events(**context):
    try:
        logger.info("Создаем генератор")
        generator = EventGenerator()

        df = generator.generate_dataset()
        execution_date = context.get('execution_date', datetime.now())
        date_str = execution_date.stftime("%Y%m%d_%H%M%S")
        filepath = DATA_DIR / f"test_events_{date_str}.csv"

        filepath = generator.save_to_csv(df, filepath)

        return str(filepath)

    except Exception as e:
        logger.error(f"Во время генерации данных произошла ошибка{e!s}")
        raise



application='/opt/spark/scripts/anomaly_detector.py'