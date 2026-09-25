# User Behavior Anomaly Detector 

> ETL-пайплайн для детекции аномалий в поведении пользователей мобильного приложения. Проект показывает оркестрацию задач с Apache Airflow, генерацию событий с аномалиями и их обнаружение через PySpark.

---

## Оглавление

- [Описание](#описание)
- [Архитектура](#архитектура)
- [Структура проекта](#структура-проекта)
- [Технологии](#технологии)
- [Установка и запуск](#установка-и-запуск)
- [Расписание и ручной запуск](#расписание-и-ручной-запуск)
- [Как это работает](#как-это-работает)
- [Методы детекции аномалий](#методы-детекции-аномалий)
- [Пример работы](#пример-работы)
- [Мониторинг и логи](#мониторинг-и-логи)
- [Устранение неполадок](#устранение-неполадок)
- [Полезные команды](#полезные-команды)

---

## Описание

Проект автоматизирует процесс обнаружения аномалий в поведении пользователей мобильного приложения. 
Каждый день запускается DAG, который:

1. **Генерирует** события пользователей (login, click, purchase, logout) с контролируемыми аномалиями (высокая стоимость покупки, всплески активностей, подозительный девайс)
2. **Запускает** PySpark-скрипт для детекции аномалий 4 методами
3. **Сравнивает** предсказания с ground truth
4. **Вычисляет** метрики качества: Precision, Recall, F1-score
5. **Сохраняет** результаты в CSV

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                     DAG: user_anomaly_pipeline                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────┐                                        │
│  │  generate_data      │  → Генерирует CSV с аномалиями         │
│  │  (PythonOperator)   │  → Сохраняет в data/events/            │
│  └─────────────────────┘                                        │
│           │                                                     │
│           │ XCom: filepath                                      │
│           ▼                                                     │
│  ┌─────────────────────┐                                        │
│  │  detected_anomalies │  → Запускает PySpark local[*]          │
│  │  (SparkSubmit)      │  → 4 метода детекции                   │
│  └─────────────────────┘  → Сохраняет в data/anomalies/         │
│           │                                                     │
│           │ XCom: filepath                                      │
│           ▼                                                     │
│  ┌─────────────────────┐                                        │
│  │ check_task          │ → Проверяет результаты                 │
│  │ (PythonOperator)    │ → Логирует найденные файлы             │
│  └─────────────────────┘                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Структура проекта

```
user_behavior_anomaly_detection/
├── docker-compose.yml          # Docker Compose для Airflow 3.3.1 + PostgreSQL + Spark
├── .gitignore                  # Игнорируемые файлы
├── .env                        # Переменные окружения (создать и скопировать из .env.example)
├── .env.example                # Пример переменных окружения
├── requirements.txt            # Python зависимости
├── README.md                   # Документация
├── Dockerfile                  # Docker-образ Airflow с Java
│
├── dags/                       # DAG-и Airflow
│   └── user_anomaly_pipeline.py
│
├── src/                        # Переиспользуемый код
│   ├── __init__.py
│   └── generators/
│       ├── __init__.py
│       └── event_generator.py  # Генерация событий с аномалиями
│
├── scripts/                    # PySpark-скрипты
│   └── anomaly_detector.py     # Детекция и сравнение аномалий
│
├── data/                       # Данные
│   ├── events/                 # CSV с событиями
│   │ └── .gitkeep
│   └── anomalies/              # Папки с аномалиями от Spark
│   └── .gitkeep
├── plugins/                    # Плагины Airflow
│   └── .gitkeep
│
├── logs/                       # Логи Airflow
└   └── .gitkeep
```

---

## Технологии

| Технология         | Версия    | Назначение                  |
|--------------------|-----------|-----------------------------|
| **Apache Airflow** | 3.3.1     | Оркестрация задач               |
| **Apache Spark**   | 3.5.5     | Распределённая обработка данных |
| **PySpark**        | 3.5.5     | Python API для Spark            |
| **PostgreSQL**     | 15-alpine | Хранение метаданных Airflow     |
| **Python**         | 3.11      | Основной язык                   |
| **Pandas**         | 2.x       | Обработка данных                |
| **NumPy**          | 1.x       | Генерация случайных данных      |
| **Docker**         | Latest    | Контейнеризация                 |

---

## Установка и запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/ViktorPetrovic/user_behavior_anomaly_detection.git
cd user_behavior_anomaly_detection
```

### 2. Создать файл `.env`

```bash
# Скопировать пример конфигурации
cp .env.example .env

# Отредактировать .env (если нужно)
nano .env  # или откройте в любом редакторе
```

### 3. Запустить Docker контейнеры

```bash
# Собрать образ и запустить контейнеры
docker-compose up -d --build

# Проверить, что все работает
docker-compose ps
```

**Ожидаемый результат:**

```
NAME                                                   STATUS
user_behavior_anomaly_detection-airflow-1              Up (healthy)
user_behavior_anomaly_detection-postgres-1             Up (healthy)
user_behavior_anomaly_detection-spark-master-1         Up (healthy)
user_behavior_anomaly_detection-spark-worker-1         Up
```

### 4. Доступ к Airflow UI
> **Важно:** Пароль от доступа в Airflow UI для admin генерируется случайно при первом запуске.

- **URL:** http://localhost:8080
- **Логин:** `admin`
- **Пароль:** `сгенерируется автоматически. Чтобы узнать его в терминале Docker введите:`

```bash
docker-compose logs airflow | findstr "admin"
```
### 5. Запустить DAG

1. Открыть Airflow UI → `http://localhost:8080`
2. Найти DAG `user_anomaly_pipeline`
3. Включить DAG (переключатель ON)
4. Нажать кнопку "Trigger DAG"
> При первом запуске даг выполняется автоматически

---

## Расписание и ручной запуск

### Расписание

DAG запускается ежедневно в **00:00 UTC**:

> **Важно:** Время указано в UTC. Для Москвы (UTC+3) это **03:00**.

### Ручной запуск

1. Открой `http://localhost:8080`
2. Найди DAG `user_anomaly_pipeline`
3. Нажми кнопку "Trigger DAG"

**Через CLI:**

```bash
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags trigger user_anomaly_pipeline
```

---

## Как это работает

### 1. Генерация событий (`generate_events`)

Генератор создает ~1000 событий с 4 типами аномалий:

| Аномалия            |            Описание                |         Пример         |
|---------------------|------------------------------------|------------------------|
| `large_purchase`    | Крупная покупка                    | Сумма покупки 50000    |
| `night_activity`    | Активность ночью                   | 3.00 ночи              |
| `suspicious_device` | Подозрительное устройство          | Unknown                |
| `rapid_activity`    | Всплеск активности (20-70 событий) | 50 событий за 5 секунд |


**Параметры генерации:**
`anomaly_rate=0.05` — 5% обычных аномалий

`rapid_rate=0.005` — 0.5% всплесков


**Результат:** `data/events/test_events_YYYYMMDD_HHMMSS.csv`

### 2. Детекция аномалий (`detected_anomalies`)

PySpark-скрипт запускается в режиме `local[*]` и применяет 4 метода:

| Метод              | Что ищет                              | Тип           |
|--------------------|---------------------------------------|---------------|
| **Z-score**        | Выбросы по сумме покупки (|z| > 2)    | Статистический|
| **IQR**            | Выбросы за границами 1.5 × IQR        | Статистический|
| **Rapid Activity** | Всплески (> 20 событий за 10 секунд)  | Временной     |
| **Devices**        | Подозрительные устройства (`Unknown`) | Логический    |

**Результат:** `data/anomalies/{zscore,iqr,rapid,device}/part-*.csv`

### 3. Проверка результатов (`check_task`)

Считает количество найденных файлов и логирует их.

---

## Методы детекции аномалий

### Сравнение методов

| Метод              | Precision | Recall | F1-score | Особенность                     |
|--------------------|-----------|--------|----------|---------------------------------|
| **Z-score**        | 0.80      | 0.12   | 0.21     | Ловит крупные покупки           |
| **IQR**            | 0.95      | 0.41   | 0.57     | **Лучший баланс**               |
| **Rapid Activity** | 0.04      | 0.71   | 0.07     | Высокий Recall, много ложных    |
| **Devices**        | 0.65      | 0.31   | 0.42     | Точный, но ловит только устройства |

### Метрики

- **Precision** — точность: доля правильных среди найденных
- **Recall** — полнота: доля найденных среди реальных
- **F1-score** — баланс между Precision и Recall

**Лучший метод определяется по F1-score.**

---

## Пример работы

### CSV с событиями

```csv
event_id,user_id,event_type,timestamp,amount,city,device,is_anomaly
6397a560-...,USER_0001,click,2026-09-21T02:05:56.064793+03:00,0.0,Paris,Android,1
4a9253bf-...,USER_0035,click,2026-09-21T02:13:32.064793+03:00,0.0,Paris,iOS,1
670b2df9-...,USER_0038,click,2026-09-21T02:25:02.064793+03:00,0.0,London,Android,1
6f5dfcc7-...,USER_0006,click,2026-09-21T03:09:56.064793+03:00,0.0,New York,Android,1
```

### Логи PySpark
```
Spark сессия создана: UserAnomalyDetection
Загрузка событий из /opt/airflow/data/events/test_events_20260921_131221.csv
Загрузка завершена
Детекция аномалий через Z-score (threshold = 2.0)
Найдено 4 аномалий через Z-score
Детекция аномалий через IQR
Q1=323.23, Q3=631.31, IQR=308.08
Границы: [-138.89, 1093.43]
Найдено аномалий 19 через IQR
...
```
### Итоговые метрики

```
  Лучший метод детекции
  Метод: IQR
  F1-score: 0.5714
  Recall: 0.9524
  Precision: 0.4082
  Найдено аномалий: 21
```

---

## Мониторинг и логи

### Логи контейнеров

```bash
# Логи Airflow
docker-compose logs airflow --tail=50

# Логи Spark Master
docker-compose logs spark-master --tail=50

# Логи Spark Worker
docker-compose logs spark-worker --tail=50

# Логи PostgreSQL
docker-compose logs postgres --tail=50
```
### Spark UI

- **URL:** http://localhost:8081
- **Что видно:** статус Spark Master, подключённые Worker'ы

> **Важно:** В режиме `local[*]` задачи DAG-а **не появляются** в Spark UI. Кластер доступен для ручного запуска.

### Логи DAG в UI

1. Открыть DAG в Airflow UI
2. Нажать на конкретный Task
3. Выбрать **"Log"**

### Проверка данных

```bash
# Проверить CSV
docker exec -it user_behavior_anomaly_detection-airflow-1 ls -la /opt/airflow/data/events/
# Проверить результаты Spark
docker exec -it user_behavior_anomaly_detection-airflow-1 ls -la /opt/airflow/data/anomalies/

# Посмотреть содержимое
docker exec -it user_behavior_anomaly_detection-airflow-1 find /opt/airflow/data/anomalies/ -name "*.csv" | head -5
```

### Проверка состояния

```bash
# Статус контейнеров
docker-compose ps

# Использование ресурсов
docker stats

# Проверить, что DAG загружен
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags list
```

---

## Устранение неполадок

### Ошибка: `FileNotFoundError: /opt/airflow/data/events/*.csv`

**Решение:** Проверьте, что файл существует:
```bash
docker exec -it user_behavior_anomaly_detection-airflow-1 ls -la /opt/airflow/data/events/
```

### Ошибка: `Java gateway process exited`

**Решение:** Убедитесь, что Java установлена в контейнере Airflow:
```bash
docker exec -it user_behavior_anomaly_detection-airflow-1 java -version
```
Если нет — добавьте в `Dockerfile`:
```dockerfile
RUN apt-get install -y openjdk-17-jre-headless
```

### Ошибка: `ps: command not found`

**Решение:** Добавьте `procps` в `Dockerfile`:
```dockerfile
RUN apt-get install -y procps
```

### Порт 8080 уже занят

**Решение:** Освободите или измените порт в `docker-compose.yml`:
```yaml
ports:
  - "8081:8080" 
```

### Ошибка: `DAG seems to be missing`

**Решение:**
1. Проверьте, что файл находится в папке `dags/`
2. Проверьте ошибки импорта:
```bash
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags list-import-errors
```
3. Перезапустите Airflow:
```bash
docker-compose restart airflow
```

---

## Полезные команды

### Управление контейнерами

```bash
# Запустить
docker-compose up -d

# Остановить
docker-compose down

# Перезапустить
docker-compose restart

# Пересобрать и запустить
docker-compose up -d --build

# Остановить и удалить все (включая данные)
docker-compose down -v
```

### Доступ к контейнеру Airflow

```bash
# Зайти в контейнер Airflow
docker exec -it user_behavior_anomaly_detection-airflow-1 bash

# Проверить установленные пакеты
docker exec -it user_behavior_anomaly_detection-airflow-1 pip list

# Проверить переменные окружения
docker exec user_behavior_anomaly_detection-airflow-1 env
```

### Работа с Airflow CLI

```bash
# Список всех DAG
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags list

# Ошибки импорта DAG
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags list-import-errors

# Статус запусков DAG
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags list-runs user_anomaly_pipeline

# Запустить DAG
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags trigger user_anomaly_pipeline

# Приостановить / возобновить DAG
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags pause user_anomaly_pipeline
docker exec -it user_behavior_anomaly_detection-airflow-1 airflow dags unpause user_anomaly_pipeline
```

### Проверка данных в PostgreSQL

```bash
# Подключиться к PostgreSQL
docker exec -it user_behavior_anomaly_detection-postgres-1 psql -U airflow -d airflow
```

**Внутри `psql`:**

```sql
-- Посмотреть таблицы XCom
SELECT * FROM xcom ORDER BY id DESC LIMIT 10;

-- Посмотреть запуски DAG
SELECT dag_id, state, execution_date FROM dag_run 
WHERE dag_id = 'user_anomaly_pipeline' 
ORDER BY execution_date DESC LIMIT 10;

-- Выйти
\q
```

---

## Зависимости

### requirements.txt

```txt
apache-airflow==3.3.1
apache-airflow-providers-apache-spark==6.3.2
pyspark==3.5.5
pandas
numpy
```

---

## Контакты

- **Автор:** [Александр]
- **Email:** [navselv3@yandex.ru]
- **GitHub:** [github.com/ViktorPetrovic](https://github.com/ViktorPetrovic)
---

## Благодарности

- [Apache Airflow](https://airflow.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Docker](https://www.docker.com/)
