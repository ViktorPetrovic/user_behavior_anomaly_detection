import logging
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

class EventGenerator:
    def __init__(self, num_users: int = 50, seed: int = 42):
        self.num_users = num_users
        random.seed(seed)
        np.random.seed(seed)

        self.user_ids = [f"USER_{i:04d}" for i in range(num_users)]

        self.cities = ["Moscow", "London", "New York", "Tokyo", "Paris"]

        self.event_types = ['click', 'purchase', 'login', 'logout']

        self.devices = ['iOS', 'Android', 'Web']

        self.avg_purchase_amount = 500.0
        logger.info(f"Генератор создан с {num_users} пользователей")

    def generate_normal_event(self, user_id: str = None, timestamp: datetime = None) -> dict:
        if user_id is None:
            user_id = random.choice(self.user_ids)

        event_type = random.choice(self.event_types)
        amount = 0.0
        if event_type == 'purchase':
            amount = round(np.random.normal(self.avg_purchase_amount, 200), 2)
            amount = max(10.0, amount)

        return {
            'event_id': str(uuid.uuid4()),
            'user_id':user_id,
            'event_type': event_type,
            'timestamp': timestamp.isoformat(),
            'amount': amount,
            'city': random.choice(self.cities),
            'device': random.choice(self.devices),
            'is_anomaly': 0

            }

    def generate_anomaly_event(self, timestamp: datetime = None, user_id: str = None) -> dict:

        if user_id is None:
            user_id = random.choice(self.user_ids)
        if timestamp is None:
            timestamp = datetime.now()

        anomaly_type = random.choice(['large_purchase', 'night_activity', 'suspicious_device'])

        event_type = "purchase" if anomaly_type == 'large_purchase' else 'click'

        amount = 0.0

        if anomaly_type == 'large_purchase':
            amount = round(random.uniform(10000, 100000), 2)

        if anomaly_type == 'night_activity':
            timestamp = timestamp.replace(hour=random.randint(2, 5))

        if anomaly_type == 'suspicious_device':
            device = 'Unknown'
        else:
            device = random.choice(self.devices)


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
             
    def generate_dataset(self, total_events: int = 1000, anomaly_rate: float = 0.05, rapid_rate: float = 0.005) -> pd.DataFrame:
        logger.info(f"Генерация {total_events} событий с (аномалии={anomaly_rate:.1%}, всплески={rapid_rate:.1%})")
        events = []
        base_time = datetime.now()
        
        for i in range(total_events):
            ran = random.random()
            timestamp = base_time + timedelta(seconds=i * 2)
            if ran < rapid_rate:
                burst_size = random.randint(20, 70)
                rapid_events = self.generate_rapid_activity(timestamp=timestamp, total_events = burst_size)
                events += rapid_events
                continue
            elif ran < anomaly_rate + rapid_rate:
                event = self.generate_anomaly_event(timestamp=timestamp)     
            else:
                event = self.generate_normal_event(timestamp=timestamp)

                
            events.append(event)
        df = pd.DataFrame(events)
        df = df.sort_values('timestamp').reset_index(drop=True)
        anomaly_count = int(df["is_anomaly"].sum())
        logger.info(f"Создано {len(df)} событий со всплесками, {anomaly_count} аномалий ({anomaly_count/len(df)*100:.1f}%)")
        return df

    def generate_rapid_activity(self, timestamp: datetime, total_events: int = 50, user_id: str = None) -> list:
        if user_id is None:
            user_id = random.choice(self.user_ids)
        logger.debug(f"Генерация всплеска активности для пользователя {user_id}: {total_events} событий")
        events = []

        for i in range(total_events):
            times = timestamp + timedelta(seconds=i * 0.1)
            events.append(
                {
                'event_id': str(uuid.uuid4()),
                'user_id':user_id,
                'event_type': random.choice(self.event_types),
                'timestamp': times.isoformat(),
                'amount': round(random.uniform(100, 1000), 2) if random.random() < 0.3 else 0.0,
                'city': random.choice(self.cities),
                'device': random.choice(self.devices),
                'is_anomaly': 1
                }
                    )

        
        logger.debug(f"Создан всплеск: {len(events)} событий для {user_id}")
        return events

    def save_to_csv(self, df: pd.DataFrame, filepath: Path) -> Path:

        filepath.parent.mkdir(exist_ok=True, parents=True)

        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"События сохранены в {filepath}")

        return filepath

if __name__ == "__main__":
    
    generator = EventGenerator(num_users=50, seed=42)
    df = generator.generate_dataset(total_events=1000, anomaly_rate=0.05, rapid_rate=0.005,)

    logger.info("\nСтатистика событий")
    logger.info(f"\nВсего событий: {len(df)}")
    logger.info(f"Аномалий: {df['is_anomaly'].sum()}")
    logger.info("\nТипы событий:")
    logger.info(df['event_type'].value_counts())
    logger.info("\nУстройства:")
    logger.info(df['device'].value_counts())
    logger.info(f"\nСредняя сумма покупки: {df[df['event_type'] == 'purchase']['amount'].mean():.2f}")
    logger.info(f"Максимальная сумма: {df['amount'].max():.2f}")

    
    filepath = Path("data/events/test_events.csv")
    generator.save_to_csv(df, filepath)

    logger.info(f"\nТестовые данные сохранены в {filepath}")