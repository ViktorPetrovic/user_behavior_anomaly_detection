import logging
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EventGenerator:
    def __init__(self, num_users: int = 50, seed: int = 42):
        self.num_users = num_users
        random.seed(seed)
        np.random.seed(seed)

        self.user_ids = [f"USER_{i:04d}" for i in range(num_users)]

        self.cities = ["Moscow", "London", "New York", "Tokyo", "Paris"]

        self.events = ['click', 'purchase', 'login', 'logout']

        self.devices = ['iOS', 'Android', 'Web']

        self.avg_purchase_amount = 500.0
        logger.info(f"Генератор создан с {num_users} пользователей")

    def generate_normal_event(self, user_id: str = None) -> dict:
        if user_id is None:
            user_id = random.choice(self.user_ids)

        event_type = random.choice(self.events)
        amount = 0.0
        if event_type == 'purchase':
            amount = round(np.random.normal(self.avg_purchase_amount, 200), 2)
            amount = max(10.0, amount)

        return {
            'event_id': str(uuid.uuid4()),
            'user_id':user_id,
            'event_type': event_type,
            'timestamp': datetime.now().isoformat(),
            'amount': amount,
            'city': random.choice(self.cities),
            'device': random.choice(self.devices),
            'is_anomaly': 0

            }

    def generate_anomaly_event(self, user_id: str = None) -> dict:

        if user_id is None:
            user_id = random.choice(self.user_ids)

        anomaly_type = random.choice(['large_purchase', 'night_activity', 'suspicious_device'])

        event_type = "purchase" if anomaly_type == 'large_purchase' else 'click'

        amount = 0.0

        if anomaly_type == 'large_purchase':
            amount = round(random.uniform(10000, 100000), 2)

        timestamp = datetime.now()
        if anomaly_type == 'night_activity':
             timestamp = timestamp.replace(hour=random.randint(2, 5))

        device = random.choice(self.devices)

        if anomaly_type == 'suspicious_device':
             device = 'Unknown'


        return {
            'event_id': str(uuid.uuid4()),
            'user_id':user_id,
            'event_type': event_type,
            'timestamp': timestamp.isoformat(),
            'amount': amount,
            'city': random.choice(self.cities),
            'device': device,
            'is_anomaly': 1
             }
             
    def generate_dataset(self, total_events: int = 1000, anomaly_rate: float = 0.05) -> pd.DataFrame:
        logger.info(f"Генерация {total_events} событий с {anomaly_rate * 100}% аномалий")
        anomaly_count = 0
        events = []
        for _ in range(total_events):
            if random.random() < anomaly_rate:
                event = self.generate_anomaly_event()
                anomaly_count += 1
            else:
                event = self.generate_normal_event()
            events.append(event)
        
        df = pd.DataFrame(events)
        df = df.sort_values('timestamp').reset_index(drop=True)
        logger.info(f"Создано {len(df)} событий, {anomaly_count} аномалий ({anomaly_count/total_events*100:.1f}%)")
        return df

    def generate_rapid_activity(self, total_events: int = 50, user_id: str = None) -> pd.DataFrame:
        if user_id is None:
            user_id = random.choice(self.user_ids)
        logger.info(f"Генерация всплеска активности для пользователя {user_id}: {total_events} событий")
        base_time = datetime.now()
        events = []

        for i in range(total_events):
            timestamp = base_time + timedelta(seconds=i)
            events.append(
                {
                'event_id': str(uuid.uuid4()),
                'user_id':user_id,
                'event_type': random.choice(self.events),
                'timestamp': timestamp.isoformat(),
                'amount': round(random.uniform(100, 1000), 2) if random.random() < 0.3 else 0.0,
                'city': random.choice(self.cities),
                'device': random.choice(self.devices),
                'is_anomaly': 1
                }
                    )

        df = pd.DataFrame(events)
        logger.info(f"Создан всплеск: {len(df)} событий для {user_id}")
        return df

    def save_to_csv(self, df: pd.DataFrame, filepath: Path) -> Path:

        filepath.parent.mkdir(exist_ok=True, parents=True)

        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"События сохранены в {filepath}")

        return filepath

if __name__ == "__main__":
    # Создаем генератор
    generator = EventGenerator(num_users=50, seed=42)

    # Генерируем обычный набор
    df = generator.generate_dataset(total_events=1000, anomaly_rate=0.05)

    # Выводим статистику
    print("\n" + "=" * 60)
    print("📊 СТАТИСТИКА СОБЫТИЙ")
    print("=" * 60)
    print(f"Всего событий: {len(df)}")
    print(f"Аномалий: {df['is_anomaly'].sum()}")
    print("\nТипы событий:")
    print(df['event_type'].value_counts())
    print("\nУстройства:")
    print(df['device'].value_counts())
    print(f"\nСредняя сумма покупки: {df[df['event_type'] == 'purchase']['amount'].mean():.2f}")
    print(f"Максимальная сумма: {df['amount'].max():.2f}")

    # Сохраняем
    filepath = Path("data/events/test_events.csv")
    generator.save_to_csv(df, filepath)

    print(f"\nТестовые данные сохранены в {filepath}")