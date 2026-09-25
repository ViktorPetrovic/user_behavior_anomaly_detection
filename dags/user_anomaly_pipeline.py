import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

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
        date_str = execution_date.strftime("%Y%m%d_%H%M%S")
        filepath = DATA_DIR / f"test_events_{date_str}.csv"

        filepath = generator.save_to_csv(df, filepath)

        return str(filepath)

    except Exception as e:
        logger.error(f"Во время генерации данных произошла ошибка{e!s}")
        raise

def check_results(**context):
    logger.info("Проверка результатов")
    try:
        if ANOMALIES_DIR.exists():
            anomaly_files = list(ANOMALIES_DIR.glob("**/*.csv"))
            logger.info(f"Найдено {len(anomaly_files)} файлов с аномалиями")
            for f in anomaly_files[:5]:
                logger.info(f"  - {f.name}")
        else:
            logger.warning("Файлы с аномалиями не найдены")
        logger.info("Проверка завершена")
    except Exception as e:
        logger.error(f"Во время проверки возникла ошибка {e!s}")
        raise

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date':datetime(2026, 9, 20),
    'retries': 2,
    'retry_delay':timedelta(minutes=1),
    }

with DAG(
    dag_id="user_anomaly_pipeline",
    description="Генерация данных и поиск аномалий через Spark по этим данным",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    ) as dag:

    generate_task = PythonOperator(
        task_id = "generate_events",
        python_callable=generate_events,
        )

    detect_task = SparkSubmitOperator(
        task_id = 'detected_anomalies',
        application='/opt/airflow/scripts/anomaly_detector.py',
        conn_id="spark_default",
        application_args=[
            "{{task_instance.xcom_pull(task_ids='generate_events')}}",
            '/opt/airflow/data/anomalies/',
            ],
        verbose=True
        )
    check_task = PythonOperator(
        task_id = "check_task",
        python_callable=check_results
        )

generate_task >> detect_task >> check_task